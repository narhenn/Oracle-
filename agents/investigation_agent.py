import os
import json
import logging

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

load_dotenv()

logger = logging.getLogger(__name__)

AGNES_API_URL = "https://api.agnesclaw.com/v1/chat/completions"


class InvestigationAgent:
    """Autonomous deep-dive engine triggered by confidence spikes, contradictions, or thin evidence."""

    def __init__(self, db: TiDBClient, crawl_planner=None, policy_agent=None) -> None:
        self._db = db
        self._crawl_planner = crawl_planner
        self._policy = policy_agent
        self._api_key = os.getenv("AGNES_API_KEY", "")
        self._client = httpx.Client(timeout=120.0)

    def investigate(self, thesis_id: int, trigger_reason: str) -> dict:
        """Main entry point for a deep investigation."""
        # Policy check
        if self._policy and not self._policy.check("investigation", {"thesis_id": thesis_id}):
            logger.info("InvestigationAgent: blocked by policy for thesis #%d", thesis_id)
            return {"status": "blocked", "reason": "policy_limit"}

        theses = self._db.get_theses(limit=1000)
        thesis = next((t for t in theses if t["id"] == thesis_id), None)
        if not thesis:
            return {"status": "error", "reason": "thesis_not_found"}

        inv_id = self._db.insert_investigation(thesis_id, trigger_reason)
        logger.info("InvestigationAgent: starting investigation #%d for thesis #%d (%s)",
                     inv_id, thesis_id, trigger_reason)

        # Build investigation plan
        plan = self._build_investigation_plan(thesis, trigger_reason)

        # Execute plan via crawl jobs
        self._execute_investigation(plan, thesis_id)

        # Synthesize (will be processed on next orchestrator cycle when signals arrive)
        self._db.update_investigation(inv_id, "in_progress")

        return {"status": "started", "investigation_id": inv_id, "queries": len(plan)}

    def check_triggers(self, stored_theses: list[dict]) -> None:
        """Check if any thesis warrants investigation. Called after each orchestrator cycle."""
        for thesis in stored_theses:
            reason = self._should_investigate(thesis)
            if reason:
                self.investigate(thesis["id"], reason)

    def resolve(self, inv_id: int, result_type: str, findings: str, signals_found: int) -> None:
        """Store investigation result."""
        self._db.update_investigation(inv_id, "completed", result_type, findings, signals_found)
        logger.info("InvestigationAgent: investigation #%d resolved as %s", inv_id, result_type)

    def _should_investigate(self, thesis: dict) -> str | None:
        """Determine if a thesis needs investigation."""
        confidence = thesis.get("confidence", 0)
        source_diversity = thesis.get("source_diversity_score", 0)
        contradiction = thesis.get("contradiction_score", 0)
        state = thesis.get("thesis_state", "candidate")

        # Fast confidence rise with low source diversity
        if confidence > 70 and source_diversity < 30:
            return f"High confidence ({confidence:.0f}%) but low source diversity ({source_diversity:.0f})"

        # Unresolved contradiction
        if state == "conflicted" and contradiction > 50:
            return f"Conflicted state with high contradiction score ({contradiction:.0f})"

        # High-confidence signals from single source
        evidence_ids = str(thesis.get("evidence_ids", ""))
        evidence_count = len([x for x in evidence_ids.split(",") if x.strip()]) if evidence_ids else 0
        if confidence > 60 and evidence_count < 2:
            return f"Confidence {confidence:.0f}% but only {evidence_count} evidence line(s)"

        return None

    def _build_investigation_plan(self, thesis: dict, trigger_reason: str) -> list[str]:
        """Call Agnes-Claw to generate targeted research queries."""
        prompt = (
            f"Company: {thesis['company']}\n"
            f"Thesis: {thesis['thesis_text']}\n"
            f"Investigation reason: {trigger_reason}\n\n"
            "Generate 5 highly specific search queries to investigate this thesis:\n"
            "- 2 queries to find confirming evidence from independent sources\n"
            "- 2 queries to find contradicting evidence\n"
            "- 1 query about adjacent companies/competitors\n\n"
            "Return ONLY a JSON array of query strings."
        )

        try:
            headers = {"Authorization": f"Bearer {self._api_key}", "Content-Type": "application/json"}
            response = self._client.post(AGNES_API_URL, headers=headers, json={
                "model": "agnes-claw-v1",
                "messages": [
                    {"role": "system", "content": "Return ONLY a JSON array of search query strings."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.3,
                "max_tokens": 1024,
            })
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"]

            json_str = raw
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0]
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0]

            queries = json.loads(json_str.strip())
            return queries if isinstance(queries, list) else []
        except Exception as e:
            logger.error("InvestigationAgent: plan generation failed: %s", e)
            return [f"{thesis['company']} Singapore latest news", f"{thesis['company']} controversy OR problems"]

    def _execute_investigation(self, queries: list[str], thesis_id: int) -> None:
        """Create high-priority crawl jobs and v2 queries for investigation."""
        for q in queries:
            if not isinstance(q, str):
                continue
            try:
                qid = self._db.insert_query_v2(q, "validation", thesis_id=thesis_id, expected_gain=85.0)
                self._db.insert_crawl_job(query_id=qid, crawl_type="investigation", priority=90, urgency="high")
            except Exception as e:
                logger.error("InvestigationAgent: failed to create crawl job: %s", e)

    def close(self) -> None:
        self._client.close()
