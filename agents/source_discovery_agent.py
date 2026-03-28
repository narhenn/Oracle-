import logging
import re
from collections import Counter
from datetime import datetime, timedelta
from urllib.parse import urlparse

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)


class SourceDiscoveryAgent:
    """Auto-discovers and manages source lifecycle: candidate → probation → active → degraded → retired."""

    def __init__(self, db: TiDBClient, policy_agent=None) -> None:
        self._db = db
        self._policy = policy_agent

    def run(self) -> dict:
        """Main discovery + lifecycle cycle."""
        logger.info("SourceDiscoveryAgent: running discovery cycle")
        results = {"discovered": 0, "promoted": 0, "retired": 0}

        # 1. Discover new sources from citations
        docs = self._db.get_raw_documents_recent(hours=48)
        candidates = self._extract_cited_domains(docs)
        for domain, score in candidates[:5]:
            if self._policy and not self._policy.check("source_add", {"domain": domain}):
                continue
            try:
                self._db.insert_source(domain, f"https://{domain}", "news", "candidate", "oracle")
                results["discovered"] += 1
                logger.info("SourceDiscoveryAgent: discovered candidate source %s (score %.0f)", domain, score)
            except Exception:
                pass

        # 2. Check probation sources for promotion
        probation = self._db.get_sources_by_status("probation")
        for src in probation:
            if self._should_promote(src):
                self._db.update_source_status(src["id"], "active")
                results["promoted"] += 1
                logger.info("SourceDiscoveryAgent: promoted source %s to active", src["source_name"])

        # 3. Check active sources for degradation/retirement
        active = self._db.get_sources_by_status("active")
        for src in active:
            if src.get("added_by") == "human":
                continue  # never auto-retire human sources
            if self._should_retire(src):
                self._db.update_source_status(src["id"], "retired")
                results["retired"] += 1
                logger.info("SourceDiscoveryAgent: retired source %s", src["source_name"])
            elif self._should_degrade(src):
                self._db.update_source_status(src["id"], "degraded")

        # 4. Promote candidates to probation after 24h
        candidates_db = self._db.get_sources_by_status("candidate")
        for src in candidates_db:
            created = src.get("created_at")
            if created and isinstance(created, datetime) and (datetime.now() - created).days >= 1:
                self._db.update_source_status(src["id"], "probation")

        logger.info("SourceDiscoveryAgent: %s", results)
        return results

    def _extract_cited_domains(self, docs: list[dict]) -> list[tuple[str, float]]:
        """Extract domains cited in raw documents, score by frequency and relevance."""
        domain_counter: Counter = Counter()
        known_sources = {s["source_name"] for s in self._db.get_active_sources()}

        url_pattern = re.compile(r'https?://([a-zA-Z0-9.-]+)')

        for doc in docs:
            text = doc.get("raw_text", "") or ""
            domains = url_pattern.findall(text)
            for d in domains:
                d = d.lower().strip(".")
                if d in known_sources or "google" in d or "facebook" in d or "twitter" in d:
                    continue
                if any(skip in d for skip in ["cdn", "static", "ads", "analytics", "tracking"]):
                    continue
                domain_counter[d] += 1

        scored = []
        for domain, count in domain_counter.most_common(20):
            relevance = self._score_candidate(domain, count)
            if relevance >= 30:
                scored.append((domain, relevance))

        return sorted(scored, key=lambda x: x[1], reverse=True)

    def _score_candidate(self, domain: str, citation_count: int) -> float:
        sg_bonus = 20 if any(kw in domain for kw in [".sg", "singapore", "asia"]) else 0
        tech_bonus = 15 if any(kw in domain for kw in ["tech", "startup", "venture", "fintech"]) else 0
        freq_score = min(citation_count * 10, 40)
        return freq_score + sg_bonus + tech_bonus

    def _should_promote(self, source: dict) -> bool:
        perf = self._db.get_source_performance_30d(source["id"])
        if len(perf) < 3:
            return False
        avg_useful = sum(p.get("useful_signal_count", 0) for p in perf) / len(perf)
        return avg_useful >= 1.0

    def _should_retire(self, source: dict) -> bool:
        perf = self._db.get_source_performance_30d(source["id"])
        if len(perf) < 14:
            return False
        avg_useful = sum(p.get("useful_signal_count", 0) for p in perf) / len(perf)
        avg_success = sum(p.get("fetch_success_rate", 0) for p in perf) / len(perf)
        return avg_useful < 0.1 or avg_success < 0.3

    def _should_degrade(self, source: dict) -> bool:
        quality = source.get("quality_score", 50)
        return quality < 30
