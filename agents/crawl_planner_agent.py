import logging
from typing import Optional

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)


class CrawlPlannerAgent:
    """Decides what to crawl next based on entity heat, pending queries, and source freshness.

    Replaces fixed-timer crawling with priority-based crawl planning.
    """

    def __init__(self, db: TiDBClient) -> None:
        self._db = db

    def run(self) -> list[dict]:
        """Generate a prioritized crawl queue for the next cycle."""
        logger.info("CrawlPlannerAgent: planning next crawl cycle")

        max_budget = int(self._db.get_policy("max_crawl_budget_per_hour") or 50)
        jobs_created = []

        # 1. Hot entity targeted crawls
        hot_companies = self._db.get_hottest_companies(limit=5)
        sources = self._db.get_active_sources()
        source_map = {s["source_name"]: s for s in sources}

        for rank, company in enumerate(hot_companies):
            heat = self._get_company_heat(company)
            priority = 90 - rank * 5  # top company gets 90, next 85, etc.

            for source in sources[:3]:  # top 3 quality sources
                if len(jobs_created) >= max_budget:
                    break
                job_id = self._db.insert_crawl_job(
                    source_id=source["id"],
                    crawl_type="targeted",
                    priority=priority,
                    urgency="high" if heat > 70 else "normal",
                    extraction_mode="serp",
                )
                jobs_created.append({
                    "id": job_id,
                    "type": "entity_targeted",
                    "company": company,
                    "source": source["source_name"],
                    "priority": priority,
                })

        # 2. Pending v2 query crawls
        pending_queries = self._db.get_pending_queries_v2(limit=10)
        for q in pending_queries:
            if len(jobs_created) >= max_budget:
                break
            priority = q.get("expected_gain_score", 50)
            job_id = self._db.insert_crawl_job(
                query_id=q["id"],
                crawl_type="query",
                priority=priority,
                urgency="normal",
                extraction_mode="serp",
            )
            jobs_created.append({
                "id": job_id,
                "type": "query",
                "query": q["query_text"][:60],
                "priority": priority,
            })

        # 3. Stale thesis refresh crawls
        stale_theses = self._db.get_theses_by_state("stale", limit=5)
        for t in stale_theses:
            if len(jobs_created) >= max_budget:
                break
            job_id = self._db.insert_crawl_job(
                crawl_type="refresh",
                priority=40,
                urgency="low",
                extraction_mode="serp",
            )
            jobs_created.append({
                "id": job_id,
                "type": "stale_refresh",
                "company": t["company"],
                "priority": 40,
            })

        # 4. Conflicted thesis investigation crawls
        conflicted = self._db.get_theses_by_state("conflicted", limit=3)
        for t in conflicted:
            if len(jobs_created) >= max_budget:
                break
            job_id = self._db.insert_crawl_job(
                crawl_type="investigation",
                priority=80,
                urgency="high",
                extraction_mode="serp",
            )
            jobs_created.append({
                "id": job_id,
                "type": "investigation",
                "company": t["company"],
                "priority": 80,
            })

        # 5. Standard source refresh (low priority baseline)
        for source in sources:
            if len(jobs_created) >= max_budget:
                break
            job_id = self._db.insert_crawl_job(
                source_id=source["id"],
                crawl_type="standard",
                priority=20 + source.get("quality_score", 50) * 0.2,
                urgency="low",
                extraction_mode="serp",
            )
            jobs_created.append({
                "id": job_id,
                "type": "standard",
                "source": source["source_name"],
                "priority": 20 + source.get("quality_score", 50) * 0.2,
            })

        logger.info("CrawlPlannerAgent: created %d crawl jobs (budget: %d)", len(jobs_created), max_budget)
        return jobs_created

    def _get_company_heat(self, company: str) -> float:
        heats = self._db.get_entity_heat(limit=100)
        for h in heats:
            if h.get("company") == company:
                return h.get("heat_score", 0)
        return 0.0
