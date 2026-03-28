import logging
from datetime import datetime, date
from collections import defaultdict

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)


class LearningAgent:
    """Self-optimization layer — tunes source weights, query strategies, and alert thresholds."""

    def __init__(self, db: TiDBClient, policy_agent=None) -> None:
        self._db = db
        self._policy = policy_agent

    def run_daily(self) -> dict:
        """Main daily optimization cycle."""
        logger.info("LearningAgent: starting daily optimization")
        results = {}

        results["query_insights"] = self._analyse_query_performance()
        results["source_updates"] = self._analyse_source_performance()
        results["alert_insights"] = self._analyse_alert_performance()
        results["thesis_accuracy"] = self._analyse_thesis_accuracy()
        adjustments = self._propose_policy_updates(results)
        applied = self._apply_safe_adjustments(adjustments)
        results["adjustments_applied"] = applied

        logger.info("LearningAgent: daily cycle complete — %d adjustments applied", applied)
        return results

    def _analyse_query_performance(self) -> dict:
        """Analyse which query types have highest signal yield."""
        queries = self._db.get_pending_queries_v2(limit=500)  # actually need all queries
        type_performance: dict[str, dict] = defaultdict(lambda: {"runs": 0, "signals": 0, "novelty_sum": 0})

        # Read query runs
        for q in queries:
            qtype = q.get("query_type", "exploration")
            yield_score = q.get("historical_yield", 0)
            type_performance[qtype]["runs"] += 1
            type_performance[qtype]["novelty_sum"] += yield_score

        best_type = max(type_performance.items(), key=lambda x: x[1]["novelty_sum"], default=("exploration", {}))
        logger.info("LearningAgent: best query type this period: %s", best_type[0])
        return {"best_type": best_type[0], "stats": dict(type_performance)}

    def _analyse_source_performance(self) -> int:
        """Update source quality scores based on 30-day metrics."""
        sources = self._db.get_active_sources()
        updates = 0

        for src in sources:
            perf = self._db.get_source_performance_30d(src["id"])
            if not perf:
                continue

            avg_novelty = sum(p.get("avg_novelty_score", 0) for p in perf) / len(perf)
            avg_reliability = sum(p.get("avg_reliability_score", 0) for p in perf) / len(perf)
            avg_success = sum(p.get("fetch_success_rate", 0) for p in perf) / len(perf)
            total_useful = sum(p.get("useful_signal_count", 0) for p in perf)

            new_quality = (avg_novelty * 0.3 + avg_reliability * 0.3 + avg_success * 100 * 0.2 + min(total_useful, 50) * 0.4)
            new_quality = max(10, min(100, new_quality))

            old_quality = src.get("quality_score", 50)
            # Smooth update: 70% old, 30% new
            blended = old_quality * 0.7 + new_quality * 0.3

            self._db.update_source_quality(src["id"], round(blended, 1), round(avg_reliability, 1))
            updates += 1

        logger.info("LearningAgent: updated %d source quality scores", updates)
        return updates

    def _analyse_alert_performance(self) -> dict:
        """Check alert acknowledgement rates and adjust thresholds."""
        alerts = self._db.get_recent_alerts(hours=168)  # last 7 days
        if not alerts:
            return {"total": 0}

        by_type: dict[str, dict] = defaultdict(lambda: {"total": 0, "acked": 0})
        for a in alerts:
            atype = a.get("alert_type", "unknown")
            by_type[atype]["total"] += 1
            if a.get("acknowledged"):
                by_type[atype]["acked"] += 1

        low_ack_types = []
        for atype, stats in by_type.items():
            if stats["total"] >= 5:
                ack_rate = stats["acked"] / stats["total"]
                if ack_rate < 0.3:
                    low_ack_types.append(atype)
                    logger.info("LearningAgent: alert type '%s' has %.0f%% ack rate — too low", atype, ack_rate * 100)

        return {"total": len(alerts), "low_ack_types": low_ack_types}

    def _analyse_thesis_accuracy(self) -> dict:
        """Check resolved/discarded theses to calibrate confidence."""
        resolved = self._db.get_theses_by_state("resolved", limit=50)
        discarded = self._db.get_theses_by_state("discarded", limit=50)

        overconfident = 0
        underconfident = 0

        for t in discarded:
            if t.get("confidence", 0) > 70:
                overconfident += 1

        for t in resolved:
            if t.get("confidence", 0) < 50:
                underconfident += 1

        return {"overconfident_discards": overconfident, "underconfident_resolves": underconfident}

    def _propose_policy_updates(self, findings: dict) -> list[dict]:
        """Propose policy adjustments based on findings."""
        proposals = []

        # If too many low-ack alerts, raise minimum alert score
        alert_data = findings.get("alert_insights", {})
        if alert_data.get("low_ack_types"):
            proposals.append({
                "policy": "min_alert_score_telegram",
                "delta": 3,
                "reason": f"Low ack rate on types: {alert_data['low_ack_types']}",
            })

        # If overconfident theses getting discarded, we need to be more conservative
        accuracy = findings.get("thesis_accuracy", {})
        if accuracy.get("overconfident_discards", 0) > 3:
            proposals.append({
                "policy": "min_evidence_lines_for_alert",
                "delta": 1,
                "reason": f"{accuracy['overconfident_discards']} high-confidence theses were discarded",
            })

        return proposals

    def _apply_safe_adjustments(self, proposals: list[dict]) -> int:
        """Apply adjustments within ±10% of current value. Flag larger ones."""
        applied = 0
        for p in proposals:
            current = self._db.get_policy(p["policy"])
            if current == 0:
                continue

            max_delta = current * 0.10
            actual_delta = min(abs(p["delta"]), max_delta)
            if p["delta"] < 0:
                actual_delta = -actual_delta

            new_value = current + actual_delta
            self._db.update_policy(p["policy"], round(new_value, 2), "LearningAgent", p["reason"])
            applied += 1
            logger.info("LearningAgent: adjusted %s: %.2f → %.2f (%s)", p["policy"], current, new_value, p["reason"])

        return applied
