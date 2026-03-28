import os
import json
import logging
from typing import Optional

import httpx
from dotenv import load_dotenv

from __future__ import annotations

from typing import TYPE_CHECKING

from db.tidb import TiDBClient

if TYPE_CHECKING:
    from agents.alert_agent import AlertAgent

load_dotenv()

logger = logging.getLogger(__name__)

AGNES_API_URL = "https://api.agnesclaw.com/v1/chat/completions"

SYSTEM_PROMPT = """You are Oracle, a Singapore market intelligence analyst. You receive raw signals — news articles and strategic hiring data — about Singapore tech companies.

Your job:
1. Cross-reference signals about the same company or sector
2. Identify patterns (e.g. a company hiring AI engineers + announcing a product launch = AI pivot)
3. Generate an investment thesis with a confidence score (0-100)
4. Higher confidence when multiple independent signals corroborate each other
5. Always cite which signal IDs you used as evidence

Output STRICTLY as JSON array. Each element:
{
    "company": "Company Name",
    "thesis": "Your investment thesis in 2-3 sentences",
    "confidence": 82,
    "evidence_ids": [1, 4, 7]
}

Rules:
- Only generate theses with confidence >= 30
- Confidence 30-50: single weak signal
- Confidence 50-70: multiple signals or one strong signal
- Confidence 70-85: corroborated across sources
- Confidence 85-100: overwhelming multi-source convergence
- Be specific about Singapore market context
- If no meaningful pattern exists, return an empty array []
"""

QUERY_PROMPT = """You are Oracle, a Singapore market intelligence analyst. A user has asked a question about the Singapore market. You have access to the following signals from your database.

Answer the user's question using ONLY the provided signals. Be specific, cite signal IDs, and give confidence levels for any claims.

If the signals don't contain enough information to answer, say so honestly.
"""


class OrchestratorAgent:
    """Cross-references signals via Agnes-Claw LLM to generate investment theses."""

    def __init__(self, db: TiDBClient, alert_agent: AlertAgent | None = None) -> None:
        self._db = db
        self._alert_agent = alert_agent
        self._api_key = os.getenv("AGNES_API_KEY", "")
        self._client = httpx.Client(timeout=120.0)

    # ── Public ────────────────────────────────────────────────────────

    def run(self, hours: int = 24) -> list[dict]:
        """Analyse recent signals and generate theses. Returns stored theses."""
        logger.info("OrchestratorAgent: starting analysis cycle (last %dh)", hours)

        signals = self._db.get_recent_signals(hours=hours, limit=100)
        if not signals:
            logger.info("OrchestratorAgent: no recent signals to analyse")
            return []

        signals_text = self._format_signals(signals)
        raw_theses = self._call_agnes(SYSTEM_PROMPT, f"Analyse these signals:\n\n{signals_text}")

        if not raw_theses:
            logger.info("OrchestratorAgent: Agnes returned no theses")
            return []

        stored = self._store_theses(raw_theses)
        logger.info("OrchestratorAgent: generated and stored %d theses", len(stored))

        for thesis in stored:
            self.generate_next_queries(thesis)
            if self._alert_agent and thesis.get("confidence", 0) >= 90:
                self._alert_agent.post_to_channel(thesis)

        self.detect_contradictions()

        return stored

    def query(self, user_question: str, user_telegram_id: Optional[str] = None) -> str:
        """Answer a natural language question using stored signals."""
        logger.info("OrchestratorAgent: processing query — %s", user_question[:80])

        signals = self._db.get_recent_signals(hours=72, limit=100)
        theses = self._db.get_theses(limit=20)

        context_parts = []
        if signals:
            context_parts.append("RECENT SIGNALS:\n" + self._format_signals(signals))
        if theses:
            context_parts.append("EXISTING THESES:\n" + self._format_theses(theses))

        preference = None
        if user_telegram_id:
            preference = self._db.get_user_preference(user_telegram_id)

        user_msg = f"Question: {user_question}\n\n"
        if preference and preference.get("investment_focus"):
            user_msg += f"User's investment focus: {preference['investment_focus']}\n\n"
        user_msg += "\n".join(context_parts) if context_parts else "No signals available yet."

        response = self._call_agnes_raw(QUERY_PROMPT, user_msg)
        return response or "I don't have enough signals to answer that question yet."

    # ── Agnes-Claw LLM ───────────────────────────────────────────────

    def _call_agnes(self, system_prompt: str, user_message: str) -> list[dict]:
        """Call Agnes-Claw and parse JSON array response."""
        raw = self._call_agnes_raw(system_prompt, user_message)
        if not raw:
            return []

        try:
            # Extract JSON from response (handle markdown code blocks)
            json_str = raw
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0]
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0]

            parsed = json.loads(json_str.strip())
            if isinstance(parsed, list):
                return parsed
            logger.warning("Agnes returned non-array JSON: %s", type(parsed))
            return []
        except (json.JSONDecodeError, IndexError) as e:
            logger.error("Failed to parse Agnes response: %s — raw: %s", e, raw[:200])
            return []

    def _call_agnes_raw(self, system_prompt: str, user_message: str) -> Optional[str]:
        """Call Agnes-Claw API and return raw text response."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "agnes-claw-v1",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 4096,
        }

        try:
            response = self._client.post(AGNES_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError as e:
            logger.error("Agnes API HTTP error: %d — %s", e.response.status_code, e.response.text[:200])
            return None
        except (KeyError, IndexError) as e:
            logger.error("Unexpected Agnes response structure: %s", e)
            return None
        except httpx.RequestError as e:
            logger.error("Agnes API request failed: %s", e)
            return None

    # ── Formatting ────────────────────────────────────────────────────

    def _format_signals(self, signals: list[dict]) -> str:
        """Format signals into readable text for the LLM."""
        lines: list[str] = []
        for s in signals:
            ts = s.get("timestamp", "")
            lines.append(
                f"[Signal #{s['id']}] ({s['source']}) {s['company']} — "
                f"{s['signal_type']} — {s['signal_text'][:300]} "
                f"[{ts}]"
            )
        return "\n".join(lines)

    def _format_theses(self, theses: list[dict]) -> str:
        """Format existing theses for context."""
        lines: list[str] = []
        for t in theses:
            lines.append(
                f"[Thesis #{t['id']}] {t['company']} (confidence: {t['confidence']}%) — "
                f"{t['thesis_text'][:300]}"
            )
        return "\n".join(lines)

    # ── Storage ───────────────────────────────────────────────────────

    def _store_theses(self, raw_theses: list[dict]) -> list[dict]:
        """Validate and store theses in TiDB."""
        stored: list[dict] = []
        for thesis in raw_theses:
            company = thesis.get("company", "")
            thesis_text = thesis.get("thesis", "")
            confidence = thesis.get("confidence", 0)
            evidence_ids = thesis.get("evidence_ids", [])

            if not company or not thesis_text:
                logger.warning("Skipping thesis with missing company/text")
                continue
            if not isinstance(confidence, (int, float)) or confidence < 30:
                continue

            try:
                thesis_id = self._db.insert_thesis(
                    company=company,
                    thesis_text=thesis_text,
                    confidence=float(confidence),
                    evidence_ids=evidence_ids,
                )
                stored.append({
                    "id": thesis_id,
                    "company": company,
                    "thesis_text": thesis_text,
                    "confidence": confidence,
                    "evidence_ids": evidence_ids,
                })
            except Exception as e:
                logger.error("Failed to store thesis for %s: %s", company, e)

        return stored

    # ── Self-Directing Queries ────────────────────────────────────────

    def generate_next_queries(self, thesis: dict) -> list[str]:
        """Ask Agnes-Claw for follow-up search queries to confirm or challenge a thesis."""
        thesis_id = thesis["id"]
        thesis_text = thesis["thesis_text"]

        prompt = (
            f"You just generated this thesis: {thesis_text}\n\n"
            "Based on this, what are 3 follow-up search queries that would "
            "find MORE specific signals to either confirm or challenge this thesis?\n"
            "Return ONLY a JSON array of 3 search query strings."
        )

        raw = self._call_agnes_raw(
            "You are a research query generator. Return ONLY a JSON array of 3 search query strings. No other text.",
            prompt,
        )
        if not raw:
            return []

        try:
            json_str = raw
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0]
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0]

            queries = json.loads(json_str.strip())
            if not isinstance(queries, list):
                return []
        except (json.JSONDecodeError, IndexError) as e:
            logger.error("Failed to parse follow-up queries: %s", e)
            return []

        stored_queries: list[str] = []
        for q in queries[:3]:
            if not isinstance(q, str) or not q.strip():
                continue
            try:
                self._db.insert_generated_query(q.strip(), thesis_id)
                stored_queries.append(q.strip())
            except Exception as e:
                logger.error("Failed to store generated query: %s", e)

        logger.info(
            "OrchestratorAgent: generated %d follow-up queries for thesis #%d",
            len(stored_queries),
            thesis_id,
        )
        return stored_queries

    # ── Contradiction Detection ──────────────────────────────────────

    def detect_contradictions(self) -> list[dict]:
        """Scan recent theses for contradictions via Agnes-Claw."""
        theses = self._db.get_theses_since_days(days=7)
        if len(theses) <= 2:
            logger.debug("OrchestratorAgent: not enough theses for contradiction check")
            return []

        theses_text = "\n".join(
            f"[Thesis #{t['id']}] {t['company']} (confidence: {t['confidence']}%): {t['thesis_text']}"
            for t in theses
        )

        prompt = (
            "You are a critical analyst. Review these investment theses about "
            f"Singapore companies:\n\n{theses_text}\n\n"
            "Identify any contradictions or conflicts between them. "
            "For example: one thesis says sector is growing, another says contracting.\n\n"
            "Return ONLY a JSON array of objects:\n"
            '[{"thesis_a_id": int, "thesis_b_id": int, "contradiction": "one sentence", '
            '"severity": "high/medium/low"}]\n'
            "Return empty array [] if no contradictions found."
        )

        raw_contradictions = self._call_agnes(
            "You are a critical analyst. Return ONLY valid JSON. No other text.",
            prompt,
        )

        if not raw_contradictions:
            logger.info("OrchestratorAgent: no contradictions detected")
            return []

        valid_ids = {t["id"] for t in theses}
        stored: list[dict] = []

        for c in raw_contradictions:
            a_id = c.get("thesis_a_id")
            b_id = c.get("thesis_b_id")
            text = c.get("contradiction", "")
            severity = c.get("severity", "low")

            if a_id not in valid_ids or b_id not in valid_ids:
                continue
            if not text:
                continue
            if severity not in ("high", "medium", "low"):
                severity = "low"

            try:
                cid = self._db.insert_contradiction(a_id, b_id, text, severity)
                stored.append({"id": cid, "thesis_a_id": a_id, "thesis_b_id": b_id,
                               "contradiction": text, "severity": severity})
            except Exception as e:
                logger.error("Failed to store contradiction: %s", e)

        logger.info("OrchestratorAgent: detected and stored %d contradictions", len(stored))
        return stored

    def close(self) -> None:
        self._client.close()
