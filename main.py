import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.news_agent import NewsAgent
from agents.jobs_agent import JobsAgent
from agents.orchestrator import OrchestratorAgent
from agents.alert_agent import AlertAgent
from agents.trigger_engine import AdaptiveTriggerEngine
from agents.thesis_state_machine import ThesisStateMachine
from agents.entity_heat_tracker import EntityHeatTracker
from agents.query_evolution_agent import QueryEvolutionAgent
from agents.contradiction_agent import ContradictionAgent
from agents.alert_decision_agent import AlertDecisionAgent
from agents.crawl_planner_agent import CrawlPlannerAgent
from bot.telegram_bot import OracleBot

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Shared instances ──────────────────────────────────────────────────

db = TiDBClient()
news_agent = NewsAgent(db)
jobs_agent = JobsAgent(db)
alert_agent = AlertAgent(db)

# v2 agents
state_machine = ThesisStateMachine(db)
heat_tracker = EntityHeatTracker(db)
query_evolution = QueryEvolutionAgent(db)
contradiction_agent = ContradictionAgent(db)
alert_decision = AlertDecisionAgent(db, alert_agent)
crawl_planner = CrawlPlannerAgent(db)

# Orchestrator with all v2 agents injected
orchestrator = OrchestratorAgent(
    db,
    alert_agent=alert_agent,
    query_evolution=query_evolution,
    contradiction_agent=contradiction_agent,
    alert_decision=alert_decision,
    state_machine=state_machine,
    heat_tracker=heat_tracker,
)

trigger_engine = AdaptiveTriggerEngine(db, news_agent, orchestrator, alert_agent)
bot = OracleBot(db, orchestrator)

scheduler = AsyncIOScheduler()


# ── Scheduled jobs ────────────────────────────────────────────────────

def job_scrape_news() -> None:
    logger.info("Scheduled: news scrape")
    try:
        news_agent.run()
    except Exception as e:
        logger.error("Scheduled news scrape failed: %s", e)


def job_scrape_jobs() -> None:
    logger.info("Scheduled: jobs scrape")
    try:
        jobs_agent.run()
    except Exception as e:
        logger.error("Scheduled jobs scrape failed: %s", e)


def job_analyse() -> None:
    logger.info("Scheduled: orchestrator analysis")
    try:
        orchestrator.run()
    except Exception as e:
        logger.error("Scheduled analysis failed: %s", e)


def job_alert_decision() -> None:
    logger.info("Scheduled: alert decision")
    try:
        alert_decision.run()
    except Exception as e:
        logger.error("Scheduled alert decision failed: %s", e)


def job_pulse() -> None:
    logger.info("Scheduled: market pulse check")
    try:
        trigger_engine.check_pulse()
    except Exception as e:
        logger.error("Scheduled pulse check failed: %s", e)


def job_crawl_plan() -> None:
    logger.info("Scheduled: crawl planning")
    try:
        crawl_planner.run()
    except Exception as e:
        logger.error("Scheduled crawl plan failed: %s", e)


def job_heat_update() -> None:
    logger.info("Scheduled: entity heat update")
    try:
        heat_tracker.run()
    except Exception as e:
        logger.error("Scheduled heat update failed: %s", e)


# ── FastAPI lifecycle ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Oracle v2 starting up")
    db.connect()
    db.create_tables()

    # Build and start Telegram bot
    tg_app = bot.build()
    await tg_app.initialize()
    await tg_app.start()
    await bot.set_commands()
    await tg_app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot started")

    # Schedule agents (v2 loop)
    scheduler.add_job(job_scrape_news, "interval", minutes=30, next_run_time=datetime.now())
    scheduler.add_job(job_scrape_jobs, "interval", hours=2, next_run_time=datetime.now())
    scheduler.add_job(job_analyse, "interval", minutes=20, next_run_time=datetime.now())
    scheduler.add_job(job_alert_decision, "interval", minutes=5, next_run_time=datetime.now())
    scheduler.add_job(job_pulse, "interval", minutes=2)
    scheduler.add_job(job_crawl_plan, "interval", minutes=15, next_run_time=datetime.now())
    scheduler.add_job(job_heat_update, "interval", minutes=10, next_run_time=datetime.now())
    scheduler.start()
    logger.info("Scheduler started — 7 jobs registered")

    yield

    logger.info("Oracle v2 shutting down")
    scheduler.shutdown(wait=False)
    await tg_app.updater.stop()
    await tg_app.stop()
    await tg_app.shutdown()
    news_agent.close()
    jobs_agent.close()
    orchestrator.close()
    alert_agent.close()
    trigger_engine.close()
    query_evolution.close()
    contradiction_agent.close()


app = FastAPI(title="Oracle v2", version="2.0.0", lifespan=lifespan)


# ── Dashboard ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open("dashboard/index.html") as f:
        return HTMLResponse(content=f.read())


# ── API endpoints ─────────────────────────────────────────────────────

def _serialize_datetimes(rows: list[dict]) -> list[dict]:
    for r in rows:
        for k, v in r.items():
            if isinstance(v, datetime):
                r[k] = v.isoformat()
    return rows


@app.get("/api/signals")
async def api_signals(company: str | None = None, signal_type: str | None = None, limit: int = 50):
    try:
        signals = db.get_signals(company=company, signal_type=signal_type, limit=limit)
        return JSONResponse(content={"signals": _serialize_datetimes(signals)})
    except Exception as e:
        logger.error("API signals error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/theses")
async def api_theses(company: str | None = None, min_confidence: float = 0.0, limit: int = 20):
    try:
        theses = db.get_theses(company=company, min_confidence=min_confidence, limit=limit)
        return JSONResponse(content={"theses": _serialize_datetimes(theses)})
    except Exception as e:
        logger.error("API theses error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/contradictions")
async def api_contradictions(limit: int = 20):
    try:
        contradictions = db.get_contradictions(limit=limit)
        return JSONResponse(content={"contradictions": _serialize_datetimes(contradictions)})
    except Exception as e:
        logger.error("API contradictions error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/entity-heat")
async def api_entity_heat(limit: int = 20):
    try:
        heat = db.get_entity_heat(limit=limit)
        return JSONResponse(content={"entities": _serialize_datetimes(heat)})
    except Exception as e:
        logger.error("API entity-heat error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/alerts")
async def api_alerts(hours: int = 24):
    try:
        alerts = db.get_recent_alerts(hours=hours)
        return JSONResponse(content={"alerts": _serialize_datetimes(alerts)})
    except Exception as e:
        logger.error("API alerts error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/status")
async def api_status():
    try:
        recent = db.get_recent_signals(hours=24, limit=1000)
        theses = db.get_theses(limit=1000)
        heat = db.get_entity_heat(limit=5)
        return JSONResponse(content={
            "signals_24h": len(recent),
            "total_theses": len(theses),
            "high_confidence": len([t for t in theses if t["confidence"] >= 75]),
            "active_states": {
                state: len([t for t in theses if t.get("thesis_state") == state])
                for state in ["candidate", "emerging", "developing", "strong", "actionable", "conflicted", "stale"]
            },
            "hottest_entities": [{"company": h.get("company"), "heat": h.get("heat_score")} for h in heat[:5]],
            "scheduler_running": scheduler.running,
        })
    except Exception as e:
        logger.error("API status error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/scrape/news")
async def api_trigger_news():
    try:
        signals = news_agent.run()
        return JSONResponse(content={"scraped": len(signals)})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/scrape/jobs")
async def api_trigger_jobs():
    try:
        signals = jobs_agent.run()
        return JSONResponse(content={"scraped": len(signals)})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/analyse")
async def api_trigger_analyse():
    try:
        theses = orchestrator.run()
        return JSONResponse(content={"generated": len(theses)})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
