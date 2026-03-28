import logging
from datetime import datetime, timedelta

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)

# State transitions: current_state → allowed next states
TRANSITIONS = {
    "candidate":  {"emerging", "discarded"},
    "emerging":   {"developing", "conflicted", "stale", "discarded"},
    "developing": {"strong", "conflicted", "stale", "discarded"},
    "strong":     {"actionable", "conflicted", "stale"},
    "actionable": {"conflicted", "stale", "resolved"},
    "conflicted": {"developing", "emerging", "discarded", "resolved"},
    "stale":      {"emerging", "developing", "discarded"},
    "resolved":   {"strong", "stale"},
    "discarded":  set(),
}


class ThesisStateMachine:
    """Deterministic thesis state transitions based on scoring thresholds."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db

    def evaluate_and_transition(self, thesis: dict) -> str:
        """Evaluate a thesis and transition its state if warranted. Returns new state."""
        current = thesis.get("thesis_state", "candidate")
        confidence = thesis.get("confidence", 0)
        contradiction = thesis.get("contradiction_score", 0)
        evidence_strength = thesis.get("evidence_strength_score", 0)
        source_diversity = thesis.get("source_diversity_score", 0)
        urgency = thesis.get("urgency_score", 0)

        # Count evidence lines
        evidence_ids = thesis.get("evidence_ids", "")
        evidence_count = len([x for x in str(evidence_ids).split(",") if x.strip()]) if evidence_ids else 0

        new_state = current
        reason = ""

        # Contradiction override — any state can go conflicted
        if contradiction >= 60 and current not in ("conflicted", "discarded"):
            new_state = "conflicted"
            reason = f"Contradiction score {contradiction:.0f} exceeds threshold"

        # Stale check — no update in 7 days
        elif self._is_stale(thesis) and current in ("candidate", "emerging", "developing", "strong", "actionable"):
            new_state = "stale"
            reason = "No new evidence in 7+ days"

        # Forward transitions
        elif current == "candidate" and evidence_count >= 2 and confidence >= 30:
            new_state = "emerging"
            reason = f"{evidence_count} signals, confidence {confidence:.0f}%"

        elif current == "emerging" and source_diversity >= 40 and confidence >= 50:
            new_state = "developing"
            reason = f"Independent confirmation, diversity {source_diversity:.0f}"

        elif current == "developing" and evidence_strength >= 60 and confidence >= 70:
            new_state = "strong"
            reason = f"Strong evidence {evidence_strength:.0f}, confidence {confidence:.0f}%"

        elif current == "strong" and confidence >= 80 and urgency >= 50:
            new_state = "actionable"
            reason = f"Actionable: confidence {confidence:.0f}%, urgency {urgency:.0f}"

        elif current == "conflicted" and contradiction < 30 and confidence >= 50:
            new_state = "developing"
            reason = f"Contradiction resolved, score dropped to {contradiction:.0f}"

        elif current == "stale" and evidence_count >= 2 and confidence >= 40:
            new_state = "emerging"
            reason = "Refreshed with new evidence"

        # Apply transition if valid
        if new_state != current and new_state in TRANSITIONS.get(current, set()):
            self._db.update_thesis_state(thesis["id"], new_state, reason, "ThesisStateMachine")
            logger.info("Thesis #%s: %s → %s (%s)", thesis["id"], current, new_state, reason)
            return new_state

        return current

    def run_all(self) -> dict:
        """Evaluate all active theses and transition states. Returns counts."""
        theses = self._db.get_active_theses(limit=200)
        transitions = {}
        for t in theses:
            old = t.get("thesis_state", "candidate")
            new = self.evaluate_and_transition(t)
            if new != old:
                key = f"{old}→{new}"
                transitions[key] = transitions.get(key, 0) + 1

        # Auto-decay stale theses
        stale_days = int(self._db.get_policy("stale_thesis_days") or 7)
        stale_theses = self._db.get_theses_by_state("stale")
        for t in stale_theses:
            ts = t.get("timestamp")
            if ts and isinstance(ts, datetime):
                age_days = (datetime.now() - ts).days
                if age_days > stale_days * 2:
                    self._db.update_thesis_state(t["id"], "discarded", f"Stale for {age_days} days", "ThesisStateMachine")
                    transitions["stale→discarded"] = transitions.get("stale→discarded", 0) + 1

        if transitions:
            logger.info("ThesisStateMachine: %s", transitions)
        return transitions

    def _is_stale(self, thesis: dict) -> bool:
        ts = thesis.get("timestamp")
        if not ts or not isinstance(ts, datetime):
            return False
        stale_days = int(self._db.get_policy("stale_thesis_days") or 7)
        return (datetime.now() - ts).days >= stale_days

    def compute_thesis_state_for_new(self, confidence: float, evidence_count: int, source_diversity: float) -> str:
        """Determine initial state for a newly created thesis."""
        if evidence_count >= 3 and source_diversity >= 50 and confidence >= 70:
            return "strong"
        if evidence_count >= 2 and source_diversity >= 30 and confidence >= 50:
            return "developing"
        if evidence_count >= 2 and confidence >= 30:
            return "emerging"
        return "candidate"
