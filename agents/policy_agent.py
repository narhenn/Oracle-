import json
import logging
from datetime import datetime

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)


class PolicyAgent:
    """The governor — enforces compute budgets, alert limits, and safe autonomy controls."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db
        self._api_call_count: int = 0
        self._api_call_hour: int = -1

    def check(self, action_type: str, context: dict = None) -> bool:
        """Check if an action is allowed by current policies. Returns True if allowed."""
        context = context or {}
        allowed = True
        reason = ""

        if action_type == "investigation":
            allowed, reason = self._check_investigation_allowed()
        elif action_type == "alert":
            allowed, reason = self._check_alert_allowed(context.get("thesis_id", 0))
        elif action_type == "source_add":
            allowed, reason = self._check_source_add_allowed()
        elif action_type == "api_call":
            allowed, reason = self._check_api_budget()

        if not allowed:
            self._db.insert_policy_log(action_type, True, reason, json.dumps(context) if context else None)
            logger.info("PolicyAgent: BLOCKED %s — %s", action_type, reason)

        return allowed

    def enforce_budget(self) -> None:
        """Track API calls per hour."""
        current_hour = datetime.now().hour
        if current_hour != self._api_call_hour:
            self._api_call_count = 0
            self._api_call_hour = current_hour
        self._api_call_count += 1

    def get_policy(self, name: str) -> float:
        return self._db.get_policy(name)

    def prevent_narrative_lockin(self, thesis_id: int) -> bool:
        """Force contradiction check if thesis hasn't been challenged in 48h."""
        lockin_hours = int(self._db.get_policy("narrative_lockin_hours") or 48)
        # Check if any contradiction exists for this thesis recently
        contradictions = self._db.get_contradictions(limit=100)
        for c in contradictions:
            if c.get("thesis_a_id") == thesis_id or c.get("thesis_b_id") == thesis_id:
                detected = c.get("detected_at")
                if detected and isinstance(detected, datetime):
                    hours_ago = (datetime.now() - detected).total_seconds() / 3600
                    if hours_ago < lockin_hours:
                        return False  # recently challenged, no lockin
        return True  # needs challenge

    def prevent_self_reference(self, signal_sources: list[str]) -> float:
        """Check if signals trace back to Oracle-generated content. Returns penalty (0-50)."""
        oracle_sources = {"generated_query", "oracle", "investigation"}
        oracle_count = sum(1 for s in signal_sources if s in oracle_sources)
        if not signal_sources:
            return 0
        ratio = oracle_count / len(signal_sources)
        if ratio > 0.5:
            return 40.0
        if ratio > 0.3:
            return 20.0
        return 0.0

    def _check_investigation_allowed(self) -> tuple[bool, str]:
        max_inv = int(self._db.get_policy("max_deep_investigations_per_hour") or 3)
        current = self._db.count_investigations_last_hour()
        if current >= max_inv:
            return False, f"Investigation limit reached ({current}/{max_inv} per hour)"
        return True, ""

    def _check_alert_allowed(self, thesis_id: int) -> tuple[bool, str]:
        if thesis_id:
            min_evidence = int(self._db.get_policy("min_evidence_lines_for_alert") or 2)
            theses = self._db.get_theses(limit=1000)
            thesis = next((t for t in theses if t["id"] == thesis_id), None)
            if thesis:
                evidence_ids = str(thesis.get("evidence_ids", ""))
                count = len([x for x in evidence_ids.split(",") if x.strip()]) if evidence_ids else 0
                if count < min_evidence:
                    return False, f"Only {count} evidence lines (need {min_evidence})"

            dup = self._db.count_alerts_for_thesis(thesis_id, hours=4)
            if dup >= 2:
                return False, f"Already {dup} alerts for thesis #{thesis_id} in last 4h"

        return True, ""

    def _check_source_add_allowed(self) -> tuple[bool, str]:
        max_adds = int(self._db.get_policy("max_source_auto_adds_per_day") or 3)
        candidates = self._db.get_sources_by_status("candidate")
        today_adds = sum(1 for s in candidates
                         if s.get("created_at") and isinstance(s["created_at"], datetime)
                         and s["created_at"].date() == datetime.now().date()
                         and s.get("added_by") == "oracle")
        if today_adds >= max_adds:
            return False, f"Source auto-add limit reached ({today_adds}/{max_adds} today)"
        return True, ""

    def _check_api_budget(self) -> tuple[bool, str]:
        limit = int(self._db.get_policy("api_calls_per_hour_limit") or 100)
        if self._api_call_count >= limit:
            return False, f"API call budget exhausted ({self._api_call_count}/{limit} this hour)"
        return True, ""
