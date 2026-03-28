import os
import json
import logging
from typing import Optional

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

load_dotenv()

logger = logging.getLogger(__name__)

AGNES_API_URL = "https://api.agnesclaw.com/v1/chat/completions"

QUERY_GEN_PROMPT = """You are a research strategy engine for Singapore market intelligence. Given context about a thesis or entity, generate targeted search queries.

For each query, specify:
- query_text: the actual search string
- query_type: one of exploration, validation, contradiction, adjacent_entity, causal, timing
- specificity_score: 0-100 (higher = more specific/targeted)
- expected_gain_score: 0-100 (higher = more likely to find useful new info)

Return ONLY a JSON array of objects with these fields. No other text."""


class QueryEvolutionAgent:
    """Full research strategy engine — generates, ranks, and manages query trees."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db
        self._api_key = os.getenv("AGNES_API_KEY", "")
        self._client = httpx.Client(timeout=90.0)

    def run(self, thesis: dict) -> list[dict]:
        """Generate a full query tree for a thesis. Replaces simple 3-query generator."""
        thesis_id = thesis["id"]
        company = thesis.get("company", "")
        thesis_text = thesis.get("thesis_text", "")
        confidence = thesis.get("confidence", 0)
        thesis_state = thesis.get("thesis_state", "candidate")

        # Check policy limit
        max_queries = int(self._db.get_policy("max_query_expansions_per_thesis") or 6)

        # Determine what types of queries we need based on thesis state
        query_strategy = self._determine_strategy(thesis_state, confidence)

        prompt = (
            f"Company: {company}\n"
            f"Thesis: {thesis_text}\n"
            f"Confidence: {confidence}%\n"
            f"Thesis State: {thesis_state}\n\n"
            f"Generate {max_queries} research queries with these priorities:\n"
            f"{query_strategy}\n\n"
            "Focus on Singapore market context. Make queries specific enough to find actionable signals."
        )

        raw = self._call_agnes(prompt)
        if not raw:
            return []

        stored = []
        for q in raw[:max_queries]:
            query_text = q.get("query_text", "")
            query_type = q.get("query_type", "exploration")
            specificity = q.get("specificity_score", 50)
            expected_gain = q.get("expected_gain_score", 50)

            if not query_text:
                continue

            valid_types = {"exploration", "validation", "contradiction", "adjacent_entity", "causal", "timing"}
            if query_type not in valid_types:
                query_type = "exploration"

            try:
                qid = self._db.insert_query_v2(
                    query_text=query_text,
                    query_type=query_type,
                    thesis_id=thesis_id,
                    specificity=float(specificity),
                    expected_gain=float(expected_gain),
                )
                stored.append({"id": qid, "query_text": query_text, "query_type": query_type})

                # Also insert into v1 generated_queries for backward compat
                self._db.insert_generated_query(query_text, thesis_id)
            except Exception as e:
                logger.error("Failed to store evolved query: %s", e)

        logger.info(
            "QueryEvolutionAgent: generated %d queries for thesis #%d (%s)",
            len(stored), thesis_id, company,
        )

        # Expire old queries
        expired = self._db.expire_old_queries()
        if expired:
            logger.debug("QueryEvolutionAgent: expired %d old queries", expired)

        return stored

    def _determine_strategy(self, state: str, confidence: float) -> str:
        """Determine query mix based on thesis state."""
        if state == "candidate":
            return (
                "- 3 exploration queries (find new signals about this company)\n"
                "- 2 validation queries (confirm the initial signal)\n"
                "- 1 adjacent_entity query (related companies/competitors)"
            )
        elif state in ("emerging", "developing"):
            return (
                "- 2 validation queries (confirm from independent sources)\n"
                "- 1 contradiction query (find evidence against this thesis)\n"
                "- 1 causal query (what's driving this? why now?)\n"
                "- 1 timing query (when will this materialize?)\n"
                "- 1 adjacent_entity query (who else is affected?)"
            )
        elif state == "strong":
            return (
                "- 2 contradiction queries (actively challenge this thesis)\n"
                "- 2 timing queries (when/how will this play out?)\n"
                "- 1 causal query (deeper understanding of drivers)\n"
                "- 1 adjacent_entity query (ripple effects)"
            )
        elif state == "conflicted":
            return (
                "- 3 validation queries (resolve the contradiction)\n"
                "- 2 contradiction queries (find which narrative is correct)\n"
                "- 1 causal query (understand root cause of conflict)"
            )
        elif state == "stale":
            return (
                "- 3 exploration queries (any recent news about this company?)\n"
                "- 2 timing queries (has anything changed since last signal?)\n"
                "- 1 validation query (is the original thesis still relevant?)"
            )
        else:
            return (
                "- 2 exploration queries\n"
                "- 2 validation queries\n"
                "- 1 contradiction query\n"
                "- 1 timing query"
            )

    def _call_agnes(self, user_message: str) -> list[dict]:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "agnes-claw-v1",
            "messages": [
                {"role": "system", "content": QUERY_GEN_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.4,
            "max_tokens": 2048,
        }

        try:
            response = self._client.post(AGNES_API_URL, headers=headers, json=payload)
            response.raise_for_status()
            raw = response.json()["choices"][0]["message"]["content"]

            json_str = raw
            if "```json" in json_str:
                json_str = json_str.split("```json")[1].split("```")[0]
            elif "```" in json_str:
                json_str = json_str.split("```")[1].split("```")[0]

            parsed = json.loads(json_str.strip())
            return parsed if isinstance(parsed, list) else []
        except Exception as e:
            logger.error("QueryEvolutionAgent Agnes call failed: %s", e)
            return []

    def close(self) -> None:
        self._client.close()
