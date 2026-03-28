import logging
from collections import defaultdict

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)


class EntityHeatTracker:
    """Tracks entity heat scores — what deserves more attention."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db

    def run(self) -> list[dict]:
        """Recalculate heat scores for all companies with recent activity."""
        logger.info("EntityHeatTracker: recalculating heat scores")

        # Gather signals from last 7 days
        signals = self._db.get_recent_signals(hours=168, limit=1000)
        theses = self._db.get_theses_since_days(days=7)
        contradictions = self._db.get_contradictions(limit=100)

        # Group by company
        company_signals: dict[str, list] = defaultdict(list)
        for s in signals:
            company_signals[s["company"]].append(s)

        company_theses: dict[str, list] = defaultdict(list)
        for t in theses:
            company_theses[t["company"]].append(t)

        # Contradiction pressure per company
        contradiction_pressure: dict[str, float] = defaultdict(float)
        for c in contradictions:
            for key in ("company_a", "company_b"):
                company = c.get(key, "")
                if company:
                    sev_weight = {"high": 30, "medium": 15, "low": 5}.get(c.get("severity", "low"), 5)
                    contradiction_pressure[company] += sev_weight

        all_companies = set(company_signals.keys()) | set(company_theses.keys())
        results = []

        for company in all_companies:
            sigs = company_signals.get(company, [])
            ths = company_theses.get(company, [])
            cp = min(contradiction_pressure.get(company, 0), 100)

            # Signal velocity: signals per day (last 7 days)
            velocity = len(sigs) / 7.0

            # Market attention: weighted by signal types
            attention = 0.0
            type_weights = {
                "funding": 20, "acquisition": 25, "ipo": 30,
                "launch": 10, "expansion": 15, "partnership": 10,
                "hiring_ai": 15, "hiring_engineering_leadership": 20,
                "hiring_expansion": 15, "general": 3,
            }
            for s in sigs:
                st = s.get("signal_type", "general")
                attention += type_weights.get(st, 5)
            attention = min(attention, 100)

            # Investigation priority based on thesis state
            investigation = 0.0
            for t in ths:
                state = t.get("thesis_state", "candidate")
                state_weights = {
                    "candidate": 5, "emerging": 15, "developing": 25,
                    "strong": 10, "actionable": 5, "conflicted": 40, "stale": 20,
                }
                investigation += state_weights.get(state, 5)
            investigation = min(investigation, 100)

            # Composite heat score
            heat = (
                velocity * 15          # signal velocity component
                + attention * 0.3      # market attention component
                + cp * 0.25            # contradiction pressure
                + investigation * 0.2  # investigation need
            )
            heat = min(max(heat, 0), 100)

            self._db.upsert_entity_heat(
                company=company,
                heat_score=round(heat, 1),
                signal_velocity=round(velocity, 2),
                contradiction_pressure=round(cp, 1),
                market_attention=round(attention, 1),
                investigation_priority=round(investigation, 1),
            )

            results.append({
                "company": company,
                "heat_score": round(heat, 1),
                "signal_velocity": round(velocity, 2),
                "contradiction_pressure": round(cp, 1),
            })

        results.sort(key=lambda x: x["heat_score"], reverse=True)
        logger.info("EntityHeatTracker: updated %d companies, hottest: %s",
                     len(results), results[0]["company"] if results else "none")
        return results
