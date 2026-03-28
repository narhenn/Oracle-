import os
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.news_agent import NewsAgent
from agents.jobs_agent import JobsAgent
from agents.orchestrator import OrchestratorAgent
from agents.alert_agent import AlertAgent
from agents.trigger_engine import AdaptiveTriggerEngine
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
orchestrator = OrchestratorAgent(db, alert_agent=alert_agent)
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


def job_alert() -> None:
    logger.info("Scheduled: alert check")
    try:
        alert_agent.run()
    except Exception as e:
        logger.error("Scheduled alert check failed: %s", e)


def job_pulse() -> None:
    logger.info("Scheduled: market pulse check")
    try:
        trigger_engine.check_pulse()
    except Exception as e:
        logger.error("Scheduled pulse check failed: %s", e)


# ── FastAPI lifecycle ─────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Oracle starting up")
    db.connect()
    db.create_tables()

    # Build and start Telegram bot
    tg_app = bot.build()
    await tg_app.initialize()
    await tg_app.start()
    await bot.set_commands()
    await tg_app.updater.start_polling(drop_pending_updates=True)
    logger.info("Telegram bot started")

    # Schedule agents
    scheduler.add_job(job_scrape_news, "interval", minutes=30, next_run_time=datetime.now())
    scheduler.add_job(job_scrape_jobs, "interval", hours=2, next_run_time=datetime.now())
    scheduler.add_job(job_analyse, "interval", minutes=45, next_run_time=datetime.now())
    scheduler.add_job(job_alert, "interval", minutes=5, next_run_time=datetime.now())
    scheduler.add_job(job_pulse, "interval", minutes=2)
    scheduler.start()
    logger.info("Scheduler started — 5 jobs registered")

    yield

    # Shutdown
    logger.info("Oracle shutting down")
    scheduler.shutdown(wait=False)
    await tg_app.updater.stop()
    await tg_app.stop()
    await tg_app.shutdown()
    news_agent.close()
    jobs_agent.close()
    orchestrator.close()
    alert_agent.close()
    trigger_engine.close()


app = FastAPI(title="Oracle", version="1.0.0", lifespan=lifespan)


# ── Dashboard ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open("dashboard/index.html") as f:
        return HTMLResponse(content=f.read())


# ── API endpoints ─────────────────────────────────────────────────────

@app.get("/api/signals")
async def api_signals(company: str | None = None, signal_type: str | None = None, limit: int = 50):
    try:
        signals = db.get_signals(company=company, signal_type=signal_type, limit=limit)
        # Convert datetime objects for JSON serialization
        for s in signals:
            if isinstance(s.get("timestamp"), datetime):
                s["timestamp"] = s["timestamp"].isoformat()
        return JSONResponse(content={"signals": signals})
    except Exception as e:
        logger.error("API signals error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/theses")
async def api_theses(company: str | None = None, min_confidence: float = 0.0, limit: int = 20):
    try:
        theses = db.get_theses(company=company, min_confidence=min_confidence, limit=limit)
        for t in theses:
            if isinstance(t.get("timestamp"), datetime):
                t["timestamp"] = t["timestamp"].isoformat()
        return JSONResponse(content={"theses": theses})
    except Exception as e:
        logger.error("API theses error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/contradictions")
async def api_contradictions(limit: int = 20):
    try:
        contradictions = db.get_contradictions(limit=limit)
        for c in contradictions:
            if isinstance(c.get("detected_at"), datetime):
                c["detected_at"] = c["detected_at"].isoformat()
        return JSONResponse(content={"contradictions": contradictions})
    except Exception as e:
        logger.error("API contradictions error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/status")
async def api_status():
    try:
        recent = db.get_recent_signals(hours=24, limit=1000)
        theses = db.get_theses(limit=1000)
        return JSONResponse(content={
            "signals_24h": len(recent),
            "total_theses": len(theses),
            "high_confidence": len([t for t in theses if t["confidence"] >= 75]),
            "scheduler_running": scheduler.running,
        })
    except Exception as e:
        logger.error("API status error: %s", e)
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/scrape/news")
async def api_trigger_news():
    """Manually trigger a news scrape."""
    try:
        signals = news_agent.run()
        return JSONResponse(content={"scraped": len(signals)})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/scrape/jobs")
async def api_trigger_jobs():
    """Manually trigger a jobs scrape."""
    try:
        signals = jobs_agent.run()
        return JSONResponse(content={"scraped": len(signals)})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/analyse")
async def api_trigger_analyse():
    """Manually trigger orchestrator analysis."""
    try:
        theses = orchestrator.run()
        return JSONResponse(content={"generated": len(theses)})
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
