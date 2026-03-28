import os
import time
import json
import logging

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

load_dotenv()

logger = logging.getLogger(__name__)

BRIGHT_DATA_API_URL = "https://api.brightdata.com/datasets/v3/trigger"
SERP_DATASET_ID = "gd_l1viktl72bvl7bjuj0"


class PulseAgent:
    """Replaces AdaptiveTriggerEngine with full decision intelligence."""

    def __init__(self, db: TiDBClient, news_agent=None, orchestrator=None, alert_decision=None) -> None:
        self._db = db
        self._news_agent = news_agent
        self._orchestrator = orchestrator
        self._alert_decision = alert_decision
        self._api_key = os.getenv("BRIGHT_DATA_API_KEY", "")
        self._client = httpx.Client(timeout=60.0)
        self._baseline_velocity: float | None = None

    def run(self) -> dict | None:
        """Every 2 minutes — measure pulse and decide trigger."""
        logger.info("PulseAgent: checking market pulse")

        try:
            velocity = self._measure_signal_velocity()
            heat_spikes = self._measure_entity_heat()
            contradiction_pressure = self._measure_contradiction_pressure()
            market_pulse = self._measure_market_pulse()

            trigger = self._decide_trigger(velocity, heat_spikes, contradiction_pressure, market_pulse)

            if trigger["trigger_type"] != "no_action":
                self._db.insert_trigger_event(
                    trigger["trigger_type"], trigger["urgency_level"],
                    json.dumps(trigger.get("target_entities", [])),
                    trigger["reason"], trigger["recommended_cycle"],
                )
                self._execute_trigger(trigger)
                return trigger

            return None

        except Exception as e:
            logger.error("PulseAgent: pulse check failed: %s", e)
            return None

    def _measure_signal_velocity(self) -> float:
        current_30m = self._db.get_signal_count_between_hours(0, 1)
        prev_6h_avg = sum(self._db.get_signal_count_between_hours(i, i + 1) for i in range(1, 7)) / 6

        if self._baseline_velocity is None:
            self._baseline_velocity = max(prev_6h_avg, 1.0)
        self._baseline_velocity = max(self._baseline_velocity * 0.8 + prev_6h_avg * 0.2, 1.0)

        return current_30m / self._baseline_velocity

    def _measure_entity_heat(self) -> list[str]:
        heat_data = self._db.get_entity_heat(limit=20)
        spikes = [h["company"] for h in heat_data if h.get("heat_score", 0) > 70]
        return spikes

    def _measure_contradiction_pressure(self) -> int:
        contradictions = self._db.get_contradictions(limit=50)
        recent = [c for c in contradictions if c.get("action_taken") in ("none", "reinvestigate")]
        return len(recent)

    def _measure_market_pulse(self) -> int:
        try:
            headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
            payload = [{"url": "https://www.google.com/search?q=Singapore+market+news+breaking&tbs=qdr:2h"}]
            response = self._client.post(BRIGHT_DATA_API_URL, headers=headers,
                                         params={"dataset_id": SERP_DATASET_ID, "include_errors": "true"}, json=payload)
            response.raise_for_status()
            snapshot_id = response.json().get("snapshot_id")
            if not snapshot_id:
                return 0
            return len(self._poll_snapshot(snapshot_id))
        except Exception as e:
            logger.error("PulseAgent: market pulse check failed: %s", e)
            return 0

    def _decide_trigger(self, velocity: float, heat_spikes: list[str],
                        contradiction_pressure: int, market_pulse: int) -> dict:
        if market_pulse > 15 or velocity > 4.0:
            return {
                "trigger_type": "deep_investigation",
                "urgency_level": "critical",
                "target_entities": heat_spikes[:5],
                "reason": f"Market surge: {market_pulse} articles, velocity {velocity:.1f}x",
                "recommended_cycle": "deep",
            }
        if heat_spikes and velocity > 2.0:
            return {
                "trigger_type": "entity_spike",
                "urgency_level": "high",
                "target_entities": heat_spikes[:3],
                "reason": f"Entity spikes: {', '.join(heat_spikes[:3])}, velocity {velocity:.1f}x",
                "recommended_cycle": "targeted",
            }
        if contradiction_pressure > 5:
            return {
                "trigger_type": "contradiction_review",
                "urgency_level": "medium",
                "target_entities": [],
                "reason": f"{contradiction_pressure} unresolved contradictions",
                "recommended_cycle": "standard",
            }
        if market_pulse > 10:
            return {
                "trigger_type": "normal_refresh",
                "urgency_level": "medium",
                "target_entities": [],
                "reason": f"{market_pulse} breaking articles detected",
                "recommended_cycle": "standard",
            }
        if velocity < 0.3:
            stale = self._db.get_theses_by_state("stale", limit=5)
            if stale:
                return {
                    "trigger_type": "stale_thesis_recheck",
                    "urgency_level": "low",
                    "target_entities": [t["company"] for t in stale],
                    "reason": f"Quiet period, {len(stale)} stale theses need recheck",
                    "recommended_cycle": "standard",
                }

        return {"trigger_type": "no_action", "urgency_level": "low", "target_entities": [],
                "reason": "Normal activity levels", "recommended_cycle": "none"}

    def _execute_trigger(self, trigger: dict) -> None:
        logger.info("PulseAgent: executing %s trigger (%s)", trigger["trigger_type"], trigger["urgency_level"])

        if trigger["recommended_cycle"] in ("standard", "targeted", "deep"):
            if self._news_agent:
                try:
                    self._news_agent.run()
                except Exception as e:
                    logger.error("PulseAgent: news scrape failed: %s", e)

            if self._orchestrator:
                try:
                    self._orchestrator.run()
                except Exception as e:
                    logger.error("PulseAgent: analysis failed: %s", e)

            if self._alert_decision:
                try:
                    self._alert_decision.run()
                except Exception as e:
                    logger.error("PulseAgent: alert decision failed: %s", e)

    def _poll_snapshot(self, snapshot_id: str, max_attempts: int = 5) -> list[dict]:
        url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
        headers = {"Authorization": f"Bearer {self._api_key}"}
        for attempt in range(max_attempts):
            r = self._client.get(url, headers=headers, params={"format": "json"})
            if r.status_code == 200:
                data = r.json()
                return data if isinstance(data, list) else []
            elif r.status_code == 202:
                time.sleep(min(5 * (attempt + 1), 20))
            else:
                return []
        return []

    def close(self) -> None:
        self._client.close()
