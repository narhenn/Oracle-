import os
import time
import logging

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.news_agent import NewsAgent
from agents.orchestrator import OrchestratorAgent
from agents.alert_agent import AlertAgent

load_dotenv()

logger = logging.getLogger(__name__)

BRIGHT_DATA_API_URL = "https://api.brightdata.com/datasets/v3/trigger"
SERP_DATASET_ID = "gd_l1viktl72bvl7bjuj0"


class AdaptiveTriggerEngine:
    """Self-triggering engine that runs Oracle cycles when the market demands it."""

    def __init__(
        self,
        db: TiDBClient,
        news_agent: NewsAgent,
        orchestrator: OrchestratorAgent,
        alert_agent: AlertAgent,
    ) -> None:
        self._db = db
        self._news_agent = news_agent
        self._orchestrator = orchestrator
        self._alert_agent = alert_agent
        self._api_key = os.getenv("BRIGHT_DATA_API_KEY", "")
        self._client = httpx.Client(timeout=60.0)
        self._baseline_velocity: float | None = None

    # ── Public ────────────────────────────────────────────────────────

    def check_pulse(self) -> None:
        """Market pulse check — runs every 2 minutes via scheduler."""
        logger.info("TriggerEngine: checking market pulse")

        try:
            # 1. Check breaking news volume
            article_count = self._check_breaking_news()

            # 2. Check signal velocity
            velocity_ratio = self._check_signal_velocity()

            # 3. Decide whether to trigger
            trigger_reason = None

            if article_count > 10:
                trigger_reason = f"High activity: {article_count} breaking articles in last 2h"
            elif velocity_ratio is not None and velocity_ratio > 3.0:
                trigger_reason = f"Signal velocity spike: {velocity_ratio:.1f}x normal"
            elif velocity_ratio is not None and velocity_ratio < 0.5:
                logger.info("TriggerEngine: quiet period detected (velocity %.1fx)", velocity_ratio)

            if trigger_reason:
                self._trigger_cycle(trigger_reason)

        except Exception as e:
            logger.error("TriggerEngine: pulse check failed: %s", e)

    # ── Breaking News Check ───────────────────────────────────────────

    def _check_breaking_news(self) -> int:
        """Query Bright Data SERP for breaking Singapore market news. Returns article count."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = [{
            "url": "https://www.google.com/search?q=Singapore+market+news+breaking&tbs=qdr:2h"
        }]

        try:
            response = self._client.post(
                BRIGHT_DATA_API_URL,
                headers=headers,
                params={"dataset_id": SERP_DATASET_ID, "include_errors": "true"},
                json=payload,
            )
            response.raise_for_status()
            snapshot_id = response.json().get("snapshot_id")

            if not snapshot_id:
                return 0

            results = self._poll_snapshot(snapshot_id)
            count = len(results)
            logger.debug("TriggerEngine: found %d breaking articles", count)
            return count

        except Exception as e:
            logger.error("TriggerEngine: breaking news check failed: %s", e)
            return 0

    def _poll_snapshot(self, snapshot_id: str, max_attempts: int = 6) -> list[dict]:
        """Poll Bright Data for snapshot results."""
        snapshot_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        for attempt in range(max_attempts):
            response = self._client.get(snapshot_url, headers=headers, params={"format": "json"})

            if response.status_code == 200:
                data = response.json()
                return data if isinstance(data, list) else []
            elif response.status_code == 202:
                wait_time = min(5 * (attempt + 1), 20)
                logger.debug("TriggerEngine: snapshot not ready, waiting %ds", wait_time)
                time.sleep(wait_time)
            else:
                logger.error("TriggerEngine: snapshot poll failed: %d", response.status_code)
                return []

        return []

    # ── Signal Velocity ───────────────────────────────────────────────

    def _check_signal_velocity(self) -> float | None:
        """Compare signal count in last hour vs previous hour. Returns ratio."""
        try:
            current_hour = self._db.get_signal_count_between_hours(0, 1)
            previous_hour = self._db.get_signal_count_between_hours(1, 2)
        except Exception as e:
            logger.error("TriggerEngine: velocity check failed: %s", e)
            return None

        # Establish baseline on first run
        if self._baseline_velocity is None:
            self._baseline_velocity = max(float(previous_hour), 1.0)
            logger.info("TriggerEngine: baseline velocity set to %.0f signals/hr", self._baseline_velocity)

        if self._baseline_velocity == 0:
            self._baseline_velocity = 1.0

        # Update rolling baseline (weighted average)
        self._baseline_velocity = (self._baseline_velocity * 0.7) + (float(previous_hour) * 0.3)
        self._baseline_velocity = max(self._baseline_velocity, 1.0)

        ratio = float(current_hour) / self._baseline_velocity
        logger.debug(
            "TriggerEngine: velocity — current=%d, baseline=%.1f, ratio=%.1fx",
            current_hour,
            self._baseline_velocity,
            ratio,
        )
        return ratio

    # ── Trigger Full Cycle ────────────────────────────────────────────

    def _trigger_cycle(self, reason: str) -> None:
        """Run an immediate full Oracle cycle and log it."""
        logger.info("TriggerEngine: %s — triggering immediate Oracle cycle", reason)

        signals_before = len(self._db.get_recent_signals(hours=1, limit=1000))

        try:
            self._news_agent.run()
        except Exception as e:
            logger.error("TriggerEngine: news scrape failed during triggered cycle: %s", e)

        try:
            self._orchestrator.run()
        except Exception as e:
            logger.error("TriggerEngine: analysis failed during triggered cycle: %s", e)

        try:
            self._alert_agent.run()
        except Exception as e:
            logger.error("TriggerEngine: alert check failed during triggered cycle: %s", e)

        signals_after = len(self._db.get_recent_signals(hours=1, limit=1000))

        try:
            self._db.insert_trigger_log(reason, signals_before, signals_after)
        except Exception as e:
            logger.error("TriggerEngine: failed to log trigger: %s", e)

        logger.info(
            "TriggerEngine: cycle complete — signals before=%d, after=%d",
            signals_before,
            signals_after,
        )

    def close(self) -> None:
        self._client.close()
