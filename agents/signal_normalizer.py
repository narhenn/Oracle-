import hashlib
import logging
from collections import Counter

from db.tidb import TiDBClient

logger = logging.getLogger(__name__)

SIGNAL_TYPES = [
    "hiring", "partnership", "investment", "executive_change",
    "regulatory", "product_launch", "expansion", "restructuring",
    "acquisition", "infrastructure", "rumor_unverified",
]

TYPE_KEYWORDS = {
    "hiring": ["hiring", "recruits", "hires", "job opening", "head of", "vp ", "cto", "cfo"],
    "partnership": ["partners", "partnership", "collaboration", "alliance", "joint venture", "jv"],
    "investment": ["raises", "funding", "series", "seed", "investment", "venture", "round"],
    "executive_change": ["appoints", "ceo", "cfo", "cto", "steps down", "resigns", "new leadership"],
    "regulatory": ["regulation", "compliance", "license", "approved", "regulator", "mas ", "monetary authority"],
    "product_launch": ["launches", "unveils", "releases", "introduces", "new product", "beta"],
    "expansion": ["expands", "expansion", "new market", "opens office", "enters"],
    "restructuring": ["restructur", "layoff", "cuts jobs", "downsiz", "reorganiz"],
    "acquisition": ["acquires", "acquisition", "merger", "takeover", "bought"],
    "infrastructure": ["data center", "infrastructure", "cloud", "aws", "gcp", "azure"],
    "rumor_unverified": ["rumor", "reportedly", "allegedly", "sources say", "unconfirmed"],
}

SOURCE_INDEPENDENCE = {
    "primary": 90, "news": 70, "blog": 50, "aggregator": 30, "social": 20, "jobs": 75,
}


class SignalNormalizerAgent:
    """Turns raw data into structured, scored signals with entity resolution."""

    def __init__(self, db: TiDBClient) -> None:
        self._db = db
        self._entity_mention_counter: Counter = Counter()

    def normalize(self, source: str, company: str, signal_text: str,
                  signal_type: str = "", source_type: str = "news") -> dict | None:
        """Full normalization pipeline. Returns stored signal dict or None."""
        # Entity resolution
        canonical = self._resolve_entity(company)

        # Signal type classification
        if not signal_type or signal_type == "general":
            signal_type = self._classify_signal_type(signal_text)

        # Scoring
        source_quality = self._score_reliability(source)
        independence = self._score_independence(source_type)
        novelty = self._score_novelty(signal_text, canonical)
        strength = self._compute_signal_strength(independence, novelty, source_quality)

        # Skip very low quality
        if strength < 10 and novelty < 20:
            logger.debug("SignalNormalizer: skipping low-quality signal for %s (strength=%.0f)", canonical, strength)
            return None

        try:
            signal_id = self._db.insert_signal(
                source=source,
                company=canonical,
                signal_text=signal_text,
                signal_type=signal_type,
                confidence_score=strength,
            )

            # Update entity heat
            from agents.entity_heat_tracker import EntityHeatTracker
            heat_tracker = EntityHeatTracker(self._db)
            # Lightweight single-entity heat bump
            self._db.upsert_entity_heat(
                company=canonical,
                heat_score=0, signal_velocity=0, contradiction_pressure=0,
                market_attention=0, investigation_priority=0,
            )

            return {
                "id": signal_id,
                "company": canonical,
                "signal_type": signal_type,
                "independence": independence,
                "novelty": novelty,
                "reliability": source_quality,
                "strength": strength,
            }
        except Exception as e:
            logger.error("SignalNormalizer: failed to store signal: %s", e)
            return None

    def _resolve_entity(self, raw_name: str) -> str:
        """Map company mention to canonical entity. Auto-creates if seen 3+ times."""
        clean = raw_name.strip().title()
        if not clean or clean == "Unknown":
            return clean

        existing = self._db.get_entity_by_name(clean)
        if existing:
            return clean

        self._entity_mention_counter[clean] += 1
        if self._entity_mention_counter[clean] >= 3:
            sector = self._infer_sector(clean)
            self._db.upsert_entity(clean, sector=sector)
            logger.info("SignalNormalizer: auto-created entity '%s' (sector: %s)", clean, sector)
            self._entity_mention_counter[clean] = 0

        return clean

    def _infer_sector(self, company: str) -> str:
        """Basic sector inference from recent signals."""
        signals = self._db.get_signals(company=company, limit=10)
        text = " ".join(s.get("signal_text", "") for s in signals).lower()

        sector_keywords = {
            "fintech": ["fintech", "payment", "banking", "neobank", "defi", "crypto"],
            "healthtech": ["health", "medical", "biotech", "pharma", "telehealth"],
            "edtech": ["education", "edtech", "learning", "university"],
            "logistics": ["logistics", "supply chain", "shipping", "delivery"],
            "ecommerce": ["ecommerce", "marketplace", "retail", "shopping"],
            "ai_ml": ["artificial intelligence", "machine learning", "ai ", "deep learning", "nlp"],
            "saas": ["saas", "software", "platform", "b2b", "enterprise"],
            "cleantech": ["clean energy", "solar", "sustainability", "carbon"],
        }
        for sector, kws in sector_keywords.items():
            if any(kw in text for kw in kws):
                return sector
        return "technology"

    def _classify_signal_type(self, text: str) -> str:
        text_lower = text.lower()
        for sig_type, keywords in TYPE_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                return sig_type
        return "general"

    def _score_independence(self, source_type: str) -> float:
        return float(SOURCE_INDEPENDENCE.get(source_type, 50))

    def _score_novelty(self, signal_text: str, company: str) -> float:
        """Check semantic similarity against recent signals. Higher = more novel."""
        existing = self._db.get_signals(company=company, limit=20)
        if not existing:
            return 95.0

        sig_words = set(signal_text.lower().split())
        if not sig_words:
            return 50.0

        max_overlap = 0.0
        for s in existing:
            existing_words = set(s.get("signal_text", "").lower().split())
            if not existing_words:
                continue
            overlap = len(sig_words & existing_words) / max(len(sig_words | existing_words), 1)
            max_overlap = max(max_overlap, overlap)

        if max_overlap > 0.90:
            return 5.0  # near duplicate
        if max_overlap > 0.70:
            return 30.0
        if max_overlap > 0.50:
            return 60.0
        return 90.0

    def _score_reliability(self, source_name: str) -> float:
        return self._db.get_source_quality(source_name)

    def _compute_signal_strength(self, independence: float, novelty: float, reliability: float) -> float:
        return min(100, independence * 0.3 + novelty * 0.4 + reliability * 0.3)
