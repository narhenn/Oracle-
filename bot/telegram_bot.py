import os
import logging

from telegram import Update, BotCommand
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from dotenv import load_dotenv

from db.tidb import TiDBClient
from agents.orchestrator import OrchestratorAgent

load_dotenv()

logger = logging.getLogger(__name__)


class OracleBot:
    """Telegram bot interface for Oracle — natural language market queries."""

    def __init__(self, db: TiDBClient, orchestrator: OrchestratorAgent) -> None:
        self._db = db
        self._orchestrator = orchestrator
        self._token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self._app: Application | None = None

    # ── Setup ─────────────────────────────────────────────────────────

    def build(self) -> Application:
        """Build and return the Telegram Application (does not start polling)."""
        self._app = (
            Application.builder()
            .token(self._token)
            .build()
        )

        self._app.add_handler(CommandHandler("start", self._cmd_start))
        self._app.add_handler(CommandHandler("signals", self._cmd_signals))
        self._app.add_handler(CommandHandler("theses", self._cmd_theses))
        self._app.add_handler(CommandHandler("focus", self._cmd_focus))
        self._app.add_handler(CommandHandler("status", self._cmd_status))
        self._app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_query))

        logger.info("OracleBot: application built with all handlers")
        return self._app

    async def set_commands(self) -> None:
        """Register bot commands with Telegram for the menu."""
        if not self._app or not self._app.bot:
            return
        commands = [
            BotCommand("start", "Introduction and help"),
            BotCommand("signals", "View recent signals (optional: company name)"),
            BotCommand("theses", "View latest investment theses"),
            BotCommand("focus", "Set your investment focus (e.g. /focus fintech)"),
            BotCommand("status", "System status and stats"),
        ]
        await self._app.bot.set_my_commands(commands)
        logger.info("OracleBot: bot commands registered")

    # ── Command Handlers ──────────────────────────────────────────────

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Welcome message and usage guide."""
        text = (
            "<b>Oracle — Singapore Market Intelligence</b>\n\n"
            "I monitor Singapore tech news and hiring signals to generate investment theses.\n\n"
            "<b>Commands:</b>\n"
            "/signals — Recent market signals\n"
            "/theses — Latest investment theses\n"
            "/focus &lt;topic&gt; — Set your investment focus\n"
            "/status — System stats\n\n"
            "<b>Or just ask me anything:</b>\n"
            "<i>\"What's moving in Singapore fintech?\"</i>\n"
            "<i>\"Any AI startups hiring aggressively?\"</i>"
        )
        await update.message.reply_text(text, parse_mode="HTML")

    async def _cmd_signals(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show recent signals, optionally filtered by company."""
        company = " ".join(context.args) if context.args else None

        try:
            signals = self._db.get_signals(company=company, limit=10)
        except Exception as e:
            logger.error("Failed to fetch signals: %s", e)
            await update.message.reply_text("Failed to fetch signals. Try again later.")
            return

        if not signals:
            msg = f"No signals found for '{company}'." if company else "No signals yet. Agents are still collecting data."
            await update.message.reply_text(msg)
            return

        lines = ["<b>Recent Signals:</b>\n"]
        for s in signals:
            confidence = s.get("confidence_score", 0)
            lines.append(
                f"#{s['id']} <b>{s['company']}</b> ({s['source']})\n"
                f"  {s['signal_type']} — {s['signal_text'][:150]}\n"
                f"  Confidence: {confidence:.0f}%\n"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def _cmd_theses(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show latest investment theses."""
        try:
            theses = self._db.get_theses(limit=5)
        except Exception as e:
            logger.error("Failed to fetch theses: %s", e)
            await update.message.reply_text("Failed to fetch theses. Try again later.")
            return

        if not theses:
            await update.message.reply_text("No theses generated yet. The orchestrator runs periodically.")
            return

        lines = ["<b>Latest Investment Theses:</b>\n"]
        for t in theses:
            bar = "█" * int(t["confidence"] / 10) + "░" * (10 - int(t["confidence"] / 10))
            lines.append(
                f"<b>{t['company']}</b> — {t['confidence']:.0f}% {bar}\n"
                f"{t['thesis_text'][:250]}\n"
                f"Evidence: signals {t.get('evidence_ids', 'N/A')}\n"
            )

        await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    async def _cmd_focus(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Set user's investment focus preference."""
        if not context.args:
            await update.message.reply_text(
                "Usage: /focus <topic>\nExample: /focus fintech AI blockchain"
            )
            return

        focus = " ".join(context.args)
        user_id = str(update.effective_user.id)

        try:
            self._db.upsert_user_preference(user_id, focus)
            await update.message.reply_text(
                f"Investment focus set to: <b>{focus}</b>\n\n"
                "I'll tailor my analysis to these areas.",
                parse_mode="HTML",
            )
        except Exception as e:
            logger.error("Failed to save preference: %s", e)
            await update.message.reply_text("Failed to save preference. Try again.")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Show system status and signal counts."""
        try:
            recent_signals = self._db.get_recent_signals(hours=24, limit=1000)
            all_theses = self._db.get_theses(limit=1000)
            high_conf = [t for t in all_theses if t["confidence"] >= 75]

            signal_types: dict[str, int] = {}
            for s in recent_signals:
                st = s.get("signal_type", "unknown")
                signal_types[st] = signal_types.get(st, 0) + 1

            type_breakdown = "\n".join(f"  {k}: {v}" for k, v in sorted(signal_types.items())) or "  None"

            text = (
                "<b>Oracle Status</b>\n\n"
                f"<b>Signals (24h):</b> {len(recent_signals)}\n"
                f"{type_breakdown}\n\n"
                f"<b>Total Theses:</b> {len(all_theses)}\n"
                f"<b>High Confidence (≥75%):</b> {len(high_conf)}\n"
            )
            await update.message.reply_text(text, parse_mode="HTML")
        except Exception as e:
            logger.error("Failed to get status: %s", e)
            await update.message.reply_text("Failed to fetch status.")

    # ── Natural Language Query ────────────────────────────────────────

    async def _handle_query(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle free-text messages as natural language queries via the orchestrator."""
        question = update.message.text.strip()
        if not question:
            return

        user_id = str(update.effective_user.id)
        await update.message.reply_text("Analysing signals... one moment.")

        try:
            answer = self._orchestrator.query(question, user_telegram_id=user_id)
            await update.message.reply_text(answer, parse_mode="HTML")
        except Exception as e:
            logger.error("Query failed: %s", e)
            await update.message.reply_text(
                "Something went wrong processing your query. Try rephrasing or check /status."
            )
