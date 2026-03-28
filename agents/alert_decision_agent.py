import logging
from typing import Optional

from db.tidb import TiDBClient
from agents.alert_agent import AlertAgent

logger = logging.getLogger(__name__)


class AlertDecisionAgent:
    """Replaces rigid thresholds with a full alert score formula.

    alert_score = confidence + urgency + novelty + user_relevance
                  + evidence_diversity - contradiction_risk
                  - alert_fatigue_penalty - weak_source_penalty

    Score → Action:
        < 60     → no alert
        60-74    → dashboard only
        75-84    → digest candidate
        85-92    → Telegram alert
        92+      → channel auto-post (if source diversity strong)
    """

    def __init__(self, db: TiDBClient, alert_agent: AlertAgent, policy_agent=None) -> None:
        self._db = db
        self._alert_agent = alert_agent
        self._policy = policy_agent

    def run(self) -> list[dict]:
        """Score all unalerted theses and take appropriate action."""
        logger.info("AlertDecisionAgent: evaluating theses for alerts")

        theses = self._db.get_active_theses(limit=100)
        actions = []

        for thesis in theses:
            # Skip already alerted or discarded
            if thesis.get("alert_status") in ("alerted", "channel_posted"):
                continue

            score = self._compute_alert_score(thesis)
            action = self._decide_action(score, thesis)

            # Update the thesis with computed alert score
            self._db.update_thesis_scores(thesis["id"], alert_score=round(score, 1))

            if action == "no_alert":
                continue

            # Policy check
            if self._policy and action in ("telegram_alert", "channel_post") and not self._policy.check("alert", {"thesis_id": thesis["id"]}):
                continue

            # Check minimum evidence floor (policy: no alert without 2 evidence lines)
            min_evidence = int(self._db.get_policy("min_evidence_lines_for_alert") or 2)
            evidence_ids = str(thesis.get("evidence_ids", ""))
            evidence_count = len([x for x in evidence_ids.split(",") if x.strip()]) if evidence_ids else 0
            if evidence_count < min_evidence and action in ("telegram_alert", "channel_post", "urgent"):
                logger.debug("AlertDecisionAgent: thesis #%d blocked — only %d evidence lines", thesis["id"], evidence_count)
                action = "dashboard"

            self._execute_action(thesis, action, score)
            actions.append({
                "thesis_id": thesis["id"],
                "company": thesis["company"],
                "alert_score": round(score, 1),
                "action": action,
            })

        logger.info("AlertDecisionAgent: processed %d alerts", len(actions))
        return actions

    def _compute_alert_score(self, thesis: dict) -> float:
        """Compute the full alert decision score."""
        confidence = thesis.get("confidence", 0)
        urgency = thesis.get("urgency_score", 0)
        evidence_strength = thesis.get("evidence_strength_score", 0)
        source_diversity = thesis.get("source_diversity_score", 0)
        contradiction = thesis.get("contradiction_score", 0)
        user_relevance = thesis.get("user_relevance_score", 0)

        # Novelty: newer theses are more novel
        novelty = 10.0  # base novelty score

        # Alert fatigue: penalize if we've already sent alerts for this thesis recently
        fatigue_count = self._db.count_alerts_for_thesis(thesis["id"], hours=24)
        fatigue_penalty = min(fatigue_count * 15, 40)

        # Weak source penalty
        weak_source_penalty = max(0, 30 - source_diversity * 0.5)

        # Composite score (normalized to ~0-100 range)
        score = (
            confidence * 0.35
            + urgency * 0.15
            + novelty
            + user_relevance * 0.10
            + evidence_strength * 0.15
            + source_diversity * 0.10
            - contradiction * 0.20
            - fatigue_penalty
            - weak_source_penalty * 0.3
        )

        return max(min(score, 100), 0)

    def _decide_action(self, score: float, thesis: dict) -> str:
        """Map alert score to action."""
        if score < 60:
            return "no_alert"
        elif score < 75:
            return "dashboard"
        elif score < 85:
            return "digest"
        elif score < 92:
            return "telegram_alert"
        else:
            # Channel post requires strong source diversity
            diversity = thesis.get("source_diversity_score", 0)
            if diversity >= 50:
                return "channel_post"
            return "telegram_alert"

    def _execute_action(self, thesis: dict, action: str, score: float) -> None:
        """Execute the alert action."""
        thesis_id = thesis["id"]
        confidence = thesis.get("confidence", 0)

        if action == "dashboard":
            self._db.insert_alert(thesis_id, "dashboard", score, confidence, "dashboard")
            self._db.update_thesis_scores(thesis_id, alert_status="dashboard")

        elif action == "digest":
            self._db.insert_alert(thesis_id, "digest", score, confidence, "digest")
            self._db.update_thesis_scores(thesis_id, alert_status="digest")

        elif action == "telegram_alert":
            success = self._alert_agent.send_alert(thesis)
            if success:
                self._db.insert_alert(thesis_id, "telegram", score, confidence, "telegram")
                self._db.update_thesis_scores(thesis_id, alert_status="alerted")
                self._db.mark_thesis_alerted(thesis_id)
                logger.info("AlertDecisionAgent: sent Telegram alert for thesis #%d (score %.0f)", thesis_id, score)

        elif action == "channel_post":
            success = self._alert_agent.post_to_channel(thesis)
            if success:
                self._db.insert_alert(thesis_id, "channel", score, confidence, "channel")
                self._db.update_thesis_scores(thesis_id, alert_status="channel_posted")
                logger.info("AlertDecisionAgent: channel post for thesis #%d (score %.0f)", thesis_id, score)
            # Also send Telegram alert
            self._alert_agent.send_alert(thesis)
            self._db.mark_thesis_alerted(thesis_id)
