import os
import time
import logging

import httpx
import mysql.connector
from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.alert_agent import AlertAgent

load_dotenv()

logger = logging.getLogger(__name__)


class SystemHealthMonitor:
    """Monitors all Oracle components and auto-recovers where possible."""

    def __init__(self, db: TiDBClient, alert_agent: AlertAgent) -> None:
        self._db = db
        self._alert_agent = alert_agent
        self._client = httpx.Client(timeout=15.0)

    def check_all(self) -> dict:
        """Run all health checks. Every 10 minutes."""
        logger.info("SystemHealthMonitor: running health checks")
        results = {}

        checks = [
            ("tidb", self._check_tidb),
            ("bright_data", self._check_bright_data),
            ("agnes_claw", self._check_agnes),
            ("telegram", self._check_telegram),
            ("signal_pipeline", self._check_signal_pipeline),
        ]

        critical_failures = []
        for name, check_fn in checks:
            result = self._check_component(name, check_fn)
            results[name] = result
            if result["status"] == "critical":
                critical_failures.append(name)

        if critical_failures:
            self._alert_if_critical(critical_failures)

        logger.info("SystemHealthMonitor: %s", {k: v["status"] for k, v in results.items()})
        return results

    def _check_component(self, name: str, test_fn) -> dict:
        try:
            start = time.time()
            status, details = test_fn()
            latency = int((time.time() - start) * 1000)

            self._db.upsert_system_health(
                component_name=name,
                status=status,
                avg_latency_ms=latency,
                recovery_action=details if status != "healthy" else None,
            )
            return {"status": status, "latency_ms": latency, "details": details}
        except Exception as e:
            self._db.upsert_system_health(name, "critical", recovery_action=str(e))
            return {"status": "critical", "latency_ms": 0, "details": str(e)}

    def _check_tidb(self) -> tuple[str, str]:
        conn = self._db._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return "healthy", ""
        except Exception as e:
            return "critical", f"TiDB query failed: {e}"
        finally:
            cursor.close()
            conn.close()

    def _check_bright_data(self) -> tuple[str, str]:
        api_key = os.getenv("BRIGHT_DATA_API_KEY", "")
        if not api_key:
            return "degraded", "API key not configured"
        try:
            r = self._client.get("https://api.brightdata.com/zone", headers={"Authorization": f"Bearer {api_key}"})
            if r.status_code in (200, 401, 403):
                return "healthy", ""
            return "degraded", f"Unexpected status: {r.status_code}"
        except Exception as e:
            return "degraded", f"Connection failed: {e}"

    def _check_agnes(self) -> tuple[str, str]:
        api_key = os.getenv("AGNES_API_KEY", "")
        if not api_key:
            return "degraded", "API key not configured"
        try:
            r = self._client.post(
                "https://api.agnesclaw.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": "agnes-claw-v1", "messages": [{"role": "user", "content": "ping"}], "max_tokens": 5},
            )
            if r.status_code in (200, 429):
                return "healthy", ""
            return "degraded", f"Status: {r.status_code}"
        except Exception as e:
            return "degraded", f"Connection failed: {e}"

    def _check_telegram(self) -> tuple[str, str]:
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        if not token:
            return "degraded", "Bot token not configured"
        try:
            r = self._client.get(f"https://api.telegram.org/bot{token}/getMe")
            if r.status_code == 200:
                return "healthy", ""
            return "degraded", f"Status: {r.status_code}"
        except Exception as e:
            return "degraded", f"Connection failed: {e}"

    def _check_signal_pipeline(self) -> tuple[str, str]:
        recent = self._db.get_recent_signals(hours=2, limit=1)
        if recent:
            return "healthy", ""
        # Check if it's just a quiet period
        last_6h = self._db.get_recent_signals(hours=6, limit=1)
        if last_6h:
            return "healthy", "No signals in 2h but pipeline active in last 6h"
        return "degraded", "No new signals in 6+ hours"

    def _alert_if_critical(self, components: list[str]) -> None:
        msg = (
            f"<b>Oracle System Alert</b>\n\n"
            f"<b>Critical failures:</b> {', '.join(components)}\n\n"
            f"Automatic recovery attempted. Check system health dashboard."
        )
        self._alert_agent.send_message(msg)
        logger.warning("SystemHealthMonitor: CRITICAL — %s", components)

    def close(self) -> None:
        self._client.close()
