from __future__ import annotations

import os
import json
import logging
from typing import Optional, TYPE_CHECKING

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

if TYPE_CHECKING:
    from agents.alert_agent import AlertAgent
    from agents.query_evolution_agent import QueryEvolutionAgent
    from agents.contradiction_agent import ContradictionAgent
    from agents.alert_decision_agent import AlertDecisionAgent
    from agents.thesis_state_machine import ThesisStateMachine
    from agents.entity_heat_tracker import EntityHeatTracker
    from agents.investigation_agent import InvestigationAgent
    from agents.policy_agent import PolicyAgent

load_dotenv()

logger = logging.getLogger(__name__)

AGNES_API_URL = os.getenv("AGNES_API_URL", "https://api.agnesclaw.com/v1/chat/completions")

SYSTEM_PROMPT = """You are Oracle, a Singapore market intelligence analyst. You receive raw signals — news articles and strategic hiring data — about Singapore tech companies.

Your job:
1. Cross-reference signals about the same company or sector
2. Identify patterns (e.g. a company hiring AI engineers + announcing a product launch = AI pivot)
3. Generate an investment thesis with a confidence score (0-100)
4. Higher confidence when multiple independent signals corroborate each other
5. Always cite which signal IDs you used as evidence
6. Estimate evidence_strength (0-100) and source_diversity (0-100)
7. Estimate urgency (0-100) — how time-sensitive is this opportunity?

Output STRICTLY as JSON array. Each element:
{
    "company": "Company Name",
    "thesis": "Your investment thesis in 2-3 sentences",
    "confidence": 82,
    "evidence_ids": [1, 4, 7],
    "evidence_strength": 75,
    "source_diversity": 60,
    "urgency": 50
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

    def __init__(
        self,
        db: TiDBClient,
        alert_agent: AlertAgent | None = None,
        query_evolution: QueryEvolutionAgent | None = None,
        contradiction_agent: ContradictionAgent | None = None,
        alert_decision: AlertDecisionAgent | None = None,
        state_machine: ThesisStateMachine | None = None,
        heat_tracker: EntityHeatTracker | None = None,
        investigation_agent: InvestigationAgent | None = None,
        policy_agent: PolicyAgent | None = None,
    ) -> None:
        self._db = db
        self._alert_agent = alert_agent
        self._query_evolution = query_evolution
        self._contradiction_agent = contradiction_agent
        self._alert_decision = alert_decision
        self._state_machine = state_machine
        self._heat_tracker = heat_tracker
        self._investigation_agent = investigation_agent
        self._policy = policy_agent
        self._api_key = os.getenv("AGNES_API_KEY", "")
        self._client = httpx.Client(timeout=120.0)

    # ── Public ────────────────────────────────────────────────────────

    def run(self, hours: int = 24) -> list[dict]:
        """Full analysis cycle with v2 agents."""
        logger.info("OrchestratorAgent: starting v2 analysis cycle (last %dh)", hours)

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

        # Signal relationship detection
        self._detect_signal_relationships(signals)

        # v2: QueryEvolutionAgent replaces simple generate_next_queries
        for thesis in stored:
            if self._query_evolution:
                self._query_evolution.run(thesis)
            else:
                self._generate_next_queries_v1(thesis)

        # v2: ContradictionAgent with active downgrade
        if self._contradiction_agent:
            self._contradiction_agent.run()

        # Policy: force contradiction check on unchallenged theses
        if self._policy:
            for thesis in stored:
                if self._policy.prevent_narrative_lockin(thesis["id"]):
                    logger.info("OrchestratorAgent: narrative lockin check triggered for thesis #%d", thesis["id"])

        # v2: ThesisStateMachine — evaluate all thesis states
        if self._state_machine:
            self._state_machine.run_all()

        # v2: EntityHeatTracker
        if self._heat_tracker:
            self._heat_tracker.run()

        # v2: InvestigationAgent — check triggers
        if self._investigation_agent:
            self._investigation_agent.check_triggers(stored)

        # v2: AlertDecisionAgent with full formula
        if self._alert_decision:
            self._alert_decision.run()
        elif self._alert_agent:
            # Fallback: v1 channel post for 90%+ theses
            for thesis in stored:
                if thesis.get("confidence", 0) >= 90:
                    self._alert_agent.post_to_channel(thesis)

        return stored

    def query(self, user_question: str, user_telegram_id: Optional[str] = None) -> str:
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
        raw = self._call_agnes_raw(system_prompt, user_message)
        if not raw:
            return []
        try:
            json_str = raw
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0]
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0]
            parsed = json.loads(json_str.strip())
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, IndexError) as e:
            logger.error("Failed to parse Agnes response: %s — raw: %s", e, raw[:200])
            return []

    def _call_agnes_raw(self, system_prompt: str, user_message: str) -> Optional[str]:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": os.getenv("AGNES_MODEL", "gemini-2.0-flash"),
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
            return response.json()["choices"][0]["message"]["content"]
        except httpx.HTTPStatusError as e:
            logger.error("Agnes API HTTP error: %d — %s", e.response.status_code, e.response.text[:200])
            return None
        except (KeyError, IndexError) as e:
            logger.error("Unexpected Agnes response structure: %s", e)
            return None
        except httpx.RequestError as e:
            logger.error("Agnes API request failed: %s", e)
            return None

    # ── Signal Relationship Detection ─────────────────────────────────

    def _detect_signal_relationships(self, signals: list[dict]) -> int:
        """Detect relationships between signals about the same entity."""
        from collections import defaultdict
        by_company: dict[str, list] = defaultdict(list)
        for s in signals:
            by_company[s["company"]].append(s)

        links_created = 0
        for company, sigs in by_company.items():
            if len(sigs) < 2:
                continue
            for i in range(len(sigs)):
                for j in range(i + 1, min(len(sigs), i + 5)):  # limit pairs to avoid explosion
                    s1, s2 = sigs[i], sigs[j]
                    rel = self._classify_relationship(s1, s2)
                    if rel:
                        try:
                            self._db.insert_signal_link(s1["id"], s2["id"], rel["type"], rel["strength"])
                            links_created += 1
                        except Exception:
                            pass

        if links_created:
            logger.info("OrchestratorAgent: detected %d signal relationships", links_created)
        return links_created

    def _classify_relationship(self, s1: dict, s2: dict) -> dict | None:
        t1 = s1.get("signal_type", "")
        t2 = s2.get("signal_type", "")
        src1 = s1.get("source", "")
        src2 = s2.get("source", "")

        # Independent confirmation: same company, different sources, similar types
        if src1 != src2 and t1 == t2:
            return {"type": "independent_confirmation", "strength": 80}

        # Hiring pattern
        if "hiring" in t1 and "hiring" in t2:
            return {"type": "corroborating_hiring_pattern", "strength": 70}

        # Funding enables expansion
        if ("investment" in t1 and "expansion" in t2) or ("expansion" in t1 and "investment" in t2):
            return {"type": "funding_enables_expansion", "strength": 75}

        # Product + hiring = growth signal
        if ("product_launch" in t1 and "hiring" in t2) or ("hiring" in t1 and "product_launch" in t2):
            return {"type": "growth_signal", "strength": 65}

        # Different sources on same topic
        if src1 != src2:
            return {"type": "cross_source_coverage", "strength": 50}

        return None

    # ── Formatting ────────────────────────────────────────────────────

    def _format_signals(self, signals: list[dict]) -> str:
        lines: list[str] = []
        for s in signals:
            lines.append(
                f"[Signal #{s['id']}] ({s['source']}) {s['company']} — "
                f"{s['signal_type']} — {s['signal_text'][:300]} [{s.get('timestamp', '')}]"
            )
        return "\n".join(lines)

    def _format_theses(self, theses: list[dict]) -> str:
        lines: list[str] = []
        for t in theses:
            state = t.get("thesis_state", "candidate")
            lines.append(
                f"[Thesis #{t['id']}] {t['company']} (state: {state}, confidence: {t['confidence']}%) — "
                f"{t['thesis_text'][:300]}"
            )
        return "\n".join(lines)

    # ── Storage ───────────────────────────────────────────────────────

    def _store_theses(self, raw_theses: list[dict]) -> list[dict]:
        stored: list[dict] = []
        for thesis in raw_theses:
            company = thesis.get("company", "")
            thesis_text = thesis.get("thesis", "")
            confidence = thesis.get("confidence", 0)
            evidence_ids = thesis.get("evidence_ids", [])
            evidence_strength = thesis.get("evidence_strength", 0)
            source_diversity = thesis.get("source_diversity", 0)
            urgency = thesis.get("urgency", 0)

            if not company or not thesis_text:
                continue
            if not isinstance(confidence, (int, float)) or confidence < 30:
                continue

            # Determine initial state via state machine
            evidence_count = len(evidence_ids) if isinstance(evidence_ids, list) else 0
            if self._state_machine:
                initial_state = self._state_machine.compute_thesis_state_for_new(
                    confidence, evidence_count, source_diversity
                )
            else:
                initial_state = "candidate"

            try:
                thesis_id = self._db.insert_thesis(
                    company=company,
                    thesis_text=thesis_text,
                    confidence=float(confidence),
                    evidence_ids=evidence_ids,
                    thesis_state=initial_state,
                    urgency_score=float(urgency),
                    evidence_strength_score=float(evidence_strength),
                    source_diversity_score=float(source_diversity),
                )
                stored.append({
                    "id": thesis_id,
                    "company": company,
                    "thesis_text": thesis_text,
                    "confidence": confidence,
                    "evidence_ids": evidence_ids,
                    "thesis_state": initial_state,
                    "urgency_score": urgency,
                    "evidence_strength_score": evidence_strength,
                    "source_diversity_score": source_diversity,
                })
            except Exception as e:
                logger.error("Failed to store thesis for %s: %s", company, e)

        return stored

    # ── v1 fallback query generator ───────────────────────────────────

    def _generate_next_queries_v1(self, thesis: dict) -> list[str]:
        thesis_id = thesis["id"]
        prompt = (
            f"You just generated this thesis: {thesis['thesis_text']}\n\n"
            "What are 3 follow-up search queries to confirm or challenge this thesis?\n"
            "Return ONLY a JSON array of 3 search query strings."
        )
        raw = self._call_agnes_raw(
            "Return ONLY a JSON array of 3 search query strings.", prompt
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
        except (json.JSONDecodeError, IndexError):
            return []
        stored = []
        for q in queries[:3]:
            if isinstance(q, str) and q.strip():
                try:
                    self._db.insert_generated_query(q.strip(), thesis_id)
                    stored.append(q.strip())
                except Exception:
                    pass
        return stored

    def close(self) -> None:
        self._client.close()
