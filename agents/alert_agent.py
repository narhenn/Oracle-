import os
import logging
from typing import Optional

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

load_dotenv()

logger = logging.getLogger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


class AlertAgent:
    """Monitors TiDB for high-confidence theses and sends proactive Telegram alerts."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db
        self._bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        self._client = httpx.Client(timeout=30.0)

    # ── Public ────────────────────────────────────────────────────────

    def run(self, threshold: float = 75.0) -> list[dict]:
        """Check for high-confidence theses and alert on new ones."""
        logger.info("AlertAgent: scanning for theses above %.0f%% confidence", threshold)

        theses = self._db.get_high_confidence_theses(threshold=threshold)
        new_alerts: list[dict] = []

        for thesis in theses:
            thesis_id = thesis["id"]
            success = self._send_alert(thesis)
            if success:
                self._db.mark_thesis_alerted(thesis_id)
                new_alerts.append(thesis)
                logger.info(
                    "AlertAgent: sent alert for thesis #%d — %s (%.0f%%)",
                    thesis_id,
                    thesis["company"],
                    thesis["confidence"],
                )

        if not new_alerts:
            logger.debug("AlertAgent: no new alerts to send")

        return new_alerts

    # ── Telegram Messaging ────────────────────────────────────────────

    def _send_alert(self, thesis: dict) -> bool:
        """Send a formatted Telegram alert for a high-confidence thesis."""
        message = self._format_alert(thesis)
        return self._send_telegram_message(message)

    def send_message(self, text: str, chat_id: Optional[str] = None) -> bool:
        """Send an arbitrary message via Telegram. Used by other components."""
        return self._send_telegram_message(text, chat_id=chat_id)

    def _send_telegram_message(self, text: str, chat_id: Optional[str] = None) -> bool:
        """Low-level Telegram send."""
        target_chat = chat_id or self._chat_id
        if not self._bot_token or not target_chat:
            logger.error("AlertAgent: missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
            return False

        url = TELEGRAM_API_URL.format(token=self._bot_token)
        payload = {
            "chat_id": target_chat,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }

        try:
            response = self._client.post(url, json=payload)
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            logger.error(
                "Telegram API error: %d — %s",
                e.response.status_code,
                e.response.text[:200],
            )
            return False
        except httpx.RequestError as e:
            logger.error("Telegram request failed: %s", e)
            return False

    # ── Formatting ────────────────────────────────────────────────────

    def _format_alert(self, thesis: dict) -> str:
        """Format a thesis into a readable Telegram alert."""
        confidence = thesis.get("confidence", 0)
        bar = self._confidence_bar(confidence)
        evidence = thesis.get("evidence_ids", "")

        return (
            f"<b>🚨 Oracle Alert — High Confidence Signal</b>\n\n"
            f"<b>Company:</b> {thesis.get('company', 'Unknown')}\n"
            f"<b>Confidence:</b> {confidence:.0f}% {bar}\n\n"
            f"<b>Thesis:</b>\n{thesis.get('thesis_text', 'N/A')}\n\n"
            f"<b>Evidence:</b> Signal IDs {evidence}\n"
            f"<b>Generated:</b> {thesis.get('timestamp', 'N/A')}"
        )

    def _confidence_bar(self, confidence: float) -> str:
        """Visual confidence bar for Telegram."""
        filled = int(confidence / 10)
        return "█" * filled + "░" * (10 - filled)

    def close(self) -> None:
        self._client.close()
