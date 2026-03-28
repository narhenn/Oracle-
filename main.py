import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.news_agent import NewsAgent
from agents.jobs_agent import JobsAgent
from agents.alert_agent import AlertAgent
from agents.policy_agent import PolicyAgent
from agents.signal_normalizer import SignalNormalizerAgent
from agents.source_discovery_agent import SourceDiscoveryAgent
from agents.crawl_planner_agent import CrawlPlannerAgent
from agents.investigation_agent import InvestigationAgent
from agents.contradiction_agent import ContradictionAgent
from agents.query_evolution_agent import QueryEvolutionAgent
from agents.thesis_state_machine import ThesisStateMachine
from agents.entity_heat_tracker import EntityHeatTracker
from agents.alert_decision_agent import AlertDecisionAgent
from agents.orchestrator import OrchestratorAgent
from agents.pulse_agent import PulseAgent
from agents.learning_agent import LearningAgent
from agents.report_composer_agent import ReportComposerAgent
from agents.system_health_monitor import SystemHealthMonitor
from bot.telegram_bot import OracleBot

load_dotenv()

logging.basicConfig(
    level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ── Instantiation (dependency order) ──────────────────────────────────

db = TiDBClient()
policy_agent = PolicyAgent(db)
alert_agent = AlertAgent(db)
signal_normalizer = SignalNormalizerAgent(db)
source_discovery = SourceDiscoveryAgent(db, policy_agent)
news_agent = NewsAgent(db)
jobs_agent = JobsAgent(db)
crawl_planner = CrawlPlannerAgent(db)
investigation_agent = InvestigationAgent(db, crawl_planner, policy_agent)
contradiction_agent = ContradictionAgent(db)
query_evolution = QueryEvolutionAgent(db)
state_machine = ThesisStateMachine(db)
heat_tracker = EntityHeatTracker(db)
alert_decision = AlertDecisionAgent(db, alert_agent, policy_agent)

orchestrator = OrchestratorAgent(
    db,
    alert_agent=alert_agent,
    query_evolution=query_evolution,
    contradiction_agent=contradiction_agent,
    alert_decision=alert_decision,
    state_machine=state_machine,
    heat_tracker=heat_tracker,
    investigation_agent=investigation_agent,
    policy_agent=policy_agent,
)

pulse_agent = PulseAgent(db, news_agent, orchestrator, alert_decision)
learning_agent = LearningAgent(db, policy_agent)
report_composer = ReportComposerAgent(db, alert_agent)
health_monitor = SystemHealthMonitor(db, alert_agent)
bot = OracleBot(db, orchestrator)

scheduler = AsyncIOScheduler()


# ── Scheduled jobs ────────────────────────────────────────────────────

def job_pulse():
    try: pulse_agent.run()
    except Exception as e: logger.error("pulse failed: %s", e)

def job_news():
    try: news_agent.run()
    except Exception as e: logger.error("news failed: %s", e)

def job_jobs():
    try: jobs_agent.run()
    except Exception as e: logger.error("jobs failed: %s", e)

def job_analyse():
    try: orchestrator.run()
    except Exception as e: logger.error("analysis failed: %s", e)

def job_alert():
    try: alert_decision.run()
    except Exception as e: logger.error("alert decision failed: %s", e)

def job_source_discovery():
    try: source_discovery.run()
    except Exception as e: logger.error("source discovery failed: %s", e)

def job_health():
    try: health_monitor.check_all()
    except Exception as e: logger.error("health check failed: %s", e)

def job_crawl_plan():
    try: crawl_planner.run()
    except Exception as e: logger.error("crawl plan failed: %s", e)

def job_heat():
    try: heat_tracker.run()
    except Exception as e: logger.error("heat update failed: %s", e)

def job_learning():
    try: learning_agent.run_daily()
    except Exception as e: logger.error("learning failed: %s", e)

def job_report():
    try: report_composer.compose_daily_report()
    except Exception as e: logger.error("report failed: %s", e)


# ── FastAPI lifecycle ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Oracle v2 starting up — full autonomous system")
    db.connect()
    db.create_tables()

    tg_app = bot.build()
    await tg_app.initialize()
    await tg_app.start()
    await bot.set_commands()
    await tg_app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot started")

    # Register all scheduler jobs
    scheduler.add_job(job_pulse, "interval", minutes=2, id="pulse")
    scheduler.add_job(job_news, "interval", minutes=30, next_run_time=datetime.now(), id="news")
    scheduler.add_job(job_jobs, "interval", hours=2, next_run_time=datetime.now(), id="jobs")
    scheduler.add_job(job_analyse, "interval", minutes=45, next_run_time=datetime.now(), id="analyse")
    scheduler.add_job(job_alert, "interval", minutes=5, next_run_time=datetime.now(), id="alert")
    scheduler.add_job(job_source_discovery, "interval", hours=6, id="source_discovery")
    scheduler.add_job(job_health, "interval", minutes=10, next_run_time=datetime.now(), id="health")
    scheduler.add_job(job_crawl_plan, "interval", minutes=15, next_run_time=datetime.now(), id="crawl")
    scheduler.add_job(job_heat, "interval", minutes=10, next_run_time=datetime.now(), id="heat")
    scheduler.add_job(job_learning, CronTrigger(hour=3, minute=0), id="learning")
    scheduler.add_job(job_report, CronTrigger(hour=8, minute=0), id="report")
    scheduler.start()
    logger.info("Scheduler started — 11 jobs registered")

    yield

    logger.info("Oracle v2 shutting down")
    scheduler.shutdown(wait=False)
    await tg_app.updater.stop()
    await tg_app.stop()
    await tg_app.shutdown()
    for agent in [news_agent, jobs_agent, orchestrator, alert_agent, pulse_agent,
                  query_evolution, contradiction_agent, investigation_agent, health_monitor]:
        if hasattr(agent, "close"):
            agent.close()


app = FastAPI(title="Oracle v2", version="2.0.0", lifespan=lifespan)


# ── Utilities ─────────────────────────────────────────────────────────

def _ser(rows: list[dict]) -> list[dict]:
    for r in rows:
        for k, v in r.items():
            if isinstance(v, datetime):
                r[k] = v.isoformat()
    return rows


# ── Dashboard ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open("dashboard/index.html") as f:
        return HTMLResponse(content=f.read())


# ── API ───────────────────────────────────────────────────────────────

@app.get("/api/signals")
async def api_signals(company: str | None = None, signal_type: str | None = None, limit: int = 50):
    try:
        return JSONResponse(content={"signals": _ser(db.get_signals(company=company, signal_type=signal_type, limit=limit))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/theses")
async def api_theses(company: str | None = None, min_confidence: float = 0.0, limit: int = 20):
    try:
        return JSONResponse(content={"theses": _ser(db.get_theses(company=company, min_confidence=min_confidence, limit=limit))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/contradictions")
async def api_contradictions(limit: int = 20):
    try:
        return JSONResponse(content={"contradictions": _ser(db.get_contradictions(limit=limit))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/entity-heat")
async def api_entity_heat(limit: int = 20):
    try:
        return JSONResponse(content={"entities": _ser(db.get_entity_heat(limit=limit))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/alerts")
async def api_alerts(hours: int = 24):
    try:
        return JSONResponse(content={"alerts": _ser(db.get_recent_alerts(hours=hours))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/system-health")
async def api_system_health():
    try:
        return JSONResponse(content={"components": _ser(db.get_system_health())})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/sources")
async def api_sources():
    try:
        active = db.get_active_sources()
        probation = db.get_sources_by_status("probation")
        candidates = db.get_sources_by_status("candidate")
        retired = db.get_sources_by_status("retired")
        return JSONResponse(content={
            "active": _ser(active), "probation": _ser(probation),
            "candidates": _ser(candidates), "retired": _ser(retired),
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/investigations")
async def api_investigations(limit: int = 10):
    try:
        return JSONResponse(content={"investigations": _ser(db.get_recent_investigations(limit=limit))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/signal-links")
async def api_signal_links(hours: int = 24):
    try:
        return JSONResponse(content={"links": _ser(db.get_signal_links_recent(hours=hours))})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/policies")
async def api_policies():
    try:
        return JSONResponse(content={"policies": _ser(db.get_all_policies())})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/status")
async def api_status():
    try:
        recent = db.get_recent_signals(hours=24, limit=1000)
        theses = db.get_theses(limit=1000)
        heat = db.get_entity_heat(limit=5)
        health = db.get_system_health()
        return JSONResponse(content={
            "signals_24h": len(recent),
            "total_theses": len(theses),
            "high_confidence": len([t for t in theses if t["confidence"] >= 75]),
            "active_states": {
                state: len([t for t in theses if t.get("thesis_state") == state])
                for state in ["candidate", "emerging", "developing", "strong", "actionable", "conflicted", "stale"]
            },
            "hottest_entities": [{"company": h.get("company"), "heat": h.get("heat_score")} for h in heat[:5]],
            "system_health": {h["component_name"]: h["status"] for h in health},
            "scheduler_running": scheduler.running,
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.post("/api/scrape/news")
async def api_trigger_news():
    try: return JSONResponse(content={"scraped": len(news_agent.run())})
    except Exception as e: return JSONResponse(content={"error": str(e)}, status_code=500)

@app.post("/api/scrape/jobs")
async def api_trigger_jobs():
    try: return JSONResponse(content={"scraped": len(jobs_agent.run())})
    except Exception as e: return JSONResponse(content={"error": str(e)}, status_code=500)

@app.post("/api/analyse")
async def api_trigger_analyse():
    try: return JSONResponse(content={"generated": len(orchestrator.run())})
    except Exception as e: return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
