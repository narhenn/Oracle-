import os
import logging
import time
from typing import Optional

import httpx
from dotenv import load_dotenv

from db.tidb import TiDBClient

load_dotenv()

logger = logging.getLogger(__name__)

BRIGHT_DATA_API_URL = "https://api.brightdata.com/datasets/v3/trigger"

SOURCES = [
    {
        "name": "mycareersfuture",
        "dataset_id": os.getenv("BRIGHT_DATA_DATASET_MCF", ""),
        "search_domain": "mycareersfuture.gov.sg",
    },
    {
        "name": "linkedin",
        "dataset_id": os.getenv("BRIGHT_DATA_DATASET_LINKEDIN", ""),
        "search_domain": "linkedin.com/jobs",
    },
]

# Roles that signal strategic intent when hired in volume
STRATEGIC_ROLES = {
    "ai": ["machine learning", "ai engineer", "data scientist", "nlp", "deep learning", "computer vision"],
    "blockchain": ["blockchain", "web3", "smart contract", "solidity", "defi"],
    "expansion": ["country manager", "regional lead", "head of expansion", "general manager"],
    "product": ["head of product", "vp product", "chief product", "product lead"],
    "engineering_leadership": ["vp engineering", "cto", "head of engineering", "engineering director"],
    "compliance": ["compliance", "regulatory", "risk officer", "aml", "kyc"],
    "fundraising": ["investor relations", "head of finance", "cfo", "fundrais"],
}


class JobsAgent:
    """Scrapes MyCareersFuture and LinkedIn for strategic hiring signals in Singapore."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db
        self._api_key = os.getenv("BRIGHT_DATA_API_KEY", "")
        self._client = httpx.Client(timeout=60.0)

    # ── Public ────────────────────────────────────────────────────────

    def run(self) -> list[dict]:
        """Run a full scrape cycle across job sources. Returns inserted signals."""
        logger.info("JobsAgent: starting scrape cycle")
        all_signals: list[dict] = []

        for source in SOURCES:
            try:
                raw_jobs = self._scrape_source(source)
                signals = self._parse_jobs(source["name"], raw_jobs)
                stored = self._store_signals(signals)
                all_signals.extend(stored)
                logger.info(
                    "JobsAgent: %s — scraped %d jobs, stored %d signals",
                    source["name"],
                    len(raw_jobs),
                    len(stored),
                )
            except Exception as e:
                logger.error("JobsAgent: failed to scrape %s: %s", source["name"], e)

        logger.info("JobsAgent: cycle complete — %d total signals", len(all_signals))
        return all_signals

    # ── Bright Data Scraping ──────────────────────────────────────────

    def _scrape_source(self, source: dict) -> list[dict]:
        """Scrape a job source via Bright Data dataset or SERP fallback."""
        dataset_id = source.get("dataset_id", "")

        if dataset_id:
            return self._scrape_via_dataset(dataset_id, source["search_domain"])

        return self._scrape_via_serp(source["search_domain"])

    def _scrape_via_dataset(self, dataset_id: str, domain: str) -> list[dict]:
        """Use a Bright Data dataset collector for structured job data."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = [{"url": f"https://{domain}", "keyword": "Singapore tech startup"}]

        response = self._client.post(
            BRIGHT_DATA_API_URL,
            headers=headers,
            params={"dataset_id": dataset_id, "include_errors": "true"},
            json=payload,
        )
        response.raise_for_status()
        snapshot_id = response.json().get("snapshot_id")

        if not snapshot_id:
            logger.warning("No snapshot_id returned for dataset %s", dataset_id)
            return []

        return self._poll_snapshot(snapshot_id)

    def _scrape_via_serp(self, domain: str) -> list[dict]:
        """Fallback: SERP API to find recent job postings."""
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        queries = [
            f"site:{domain} Singapore hiring engineer OR \"head of\" OR director",
            f"site:{domain} Singapore startup hiring AI OR blockchain OR fintech",
        ]

        all_results: list[dict] = []
        for query in queries:
            payload = [{"url": f"https://www.google.com/search?q={query}&tbs=qdr:w"}]
            try:
                response = self._client.post(
                    BRIGHT_DATA_API_URL,
                    headers=headers,
                    params={"dataset_id": "gd_l1viktl72bvl7bjuj0", "include_errors": "true"},
                    json=payload,
                )
                response.raise_for_status()
                snapshot_id = response.json().get("snapshot_id")
                if snapshot_id:
                    results = self._poll_snapshot(snapshot_id)
                    all_results.extend(results)
            except Exception as e:
                logger.error("SERP scrape failed for query: %s — %s", query, e)

        return all_results

    def _poll_snapshot(self, snapshot_id: str, max_attempts: int = 10) -> list[dict]:
        """Poll Bright Data for snapshot results."""
        snapshot_url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
        headers = {"Authorization": f"Bearer {self._api_key}"}

        for attempt in range(max_attempts):
            response = self._client.get(snapshot_url, headers=headers, params={"format": "json"})

            if response.status_code == 200:
                data = response.json()
                return data if isinstance(data, list) else []
            elif response.status_code == 202:
                wait_time = min(5 * (attempt + 1), 30)
                logger.debug("Snapshot %s not ready, waiting %ds", snapshot_id, wait_time)
                time.sleep(wait_time)
            else:
                logger.error("Snapshot poll failed: %d %s", response.status_code, response.text)
                return []

        logger.warning("Snapshot %s timed out after %d attempts", snapshot_id, max_attempts)
        return []

    # ── Parsing ───────────────────────────────────────────────────────

    def _parse_jobs(self, source_name: str, raw_jobs: list[dict]) -> list[dict]:
        """Normalise raw job data into strategic hiring signals."""
        signals: list[dict] = []

        for job in raw_jobs:
            title = job.get("title") or job.get("name") or ""
            company = job.get("company") or job.get("company_name") or self._extract_company(title)
            description = job.get("description") or job.get("snippet") or ""
            location = job.get("location") or ""

            if not title:
                continue

            strategic_category = self._classify_strategic_role(title, description)
            if not strategic_category:
                continue

            signal_text = (
                f"Hiring: {title} at {company}. "
                f"Category: {strategic_category}. "
                f"Location: {location}. "
                f"{description[:300]}"
            ).strip()

            signals.append({
                "source": source_name,
                "company": company,
                "signal_text": signal_text,
                "signal_type": f"hiring_{strategic_category}",
            })

        return signals

    def _classify_strategic_role(self, title: str, description: str) -> Optional[str]:
        """Return the strategic category if the role is strategically relevant, else None."""
        text = f"{title} {description}".lower()

        for category, keywords in STRATEGIC_ROLES.items():
            if any(kw in text for kw in keywords):
                return category

        return None

    def _extract_company(self, title: str) -> str:
        """Best-effort company extraction from job title string."""
        if " at " in title:
            return title.split(" at ")[-1].strip()
        if " - " in title:
            return title.split(" - ")[-1].strip()
        return "Unknown"

    # ── Storage ───────────────────────────────────────────────────────

    def _store_signals(self, signals: list[dict]) -> list[dict]:
        """Insert parsed signals into TiDB. Returns stored signals with IDs."""
        stored: list[dict] = []
        for signal in signals:
            try:
                signal_id = self._db.insert_signal(
                    source=signal["source"],
                    company=signal["company"],
                    signal_text=signal["signal_text"],
                    signal_type=signal["signal_type"],
                )
                signal["id"] = signal_id
                stored.append(signal)
            except Exception as e:
                logger.error("Failed to store job signal: %s — %s", signal.get("company"), e)
        return stored

    def close(self) -> None:
        self._client.close()
