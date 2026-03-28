import os
import logging
from typing import Optional
from datetime import datetime

import certifi
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

THESIS_STATES = [
    "candidate", "emerging", "developing", "strong",
    "actionable", "conflicted", "stale", "resolved", "discarded",
]


class TiDBClient:
    """Manages TiDB Cloud Zero connection pool and all database operations."""

    def __init__(self) -> None:
        self._pool: Optional[pooling.MySQLConnectionPool] = None
        ssl_ca_path = "/etc/ssl/certs/ca-certificates.crt"
        if not os.path.exists(ssl_ca_path):
            ssl_ca_path = certifi.where()

        self._config = {
            "host": os.getenv("TIDB_HOST"),
            "user": os.getenv("TIDB_USER"),
            "password": os.getenv("TIDB_PASSWORD"),
            "database": os.getenv("TIDB_DATABASE", "oracle_db"),
            "ssl_ca": ssl_ca_path,
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
            "autocommit": True,
        }

    def connect(self) -> None:
        try:
            self._pool = pooling.MySQLConnectionPool(
                pool_name="oracle_pool",
                pool_size=5,
                pool_reset_session=True,
                **self._config,
            )
            logger.info("TiDB connection pool created")
        except Error as e:
            logger.error("Failed to create TiDB connection pool: %s", e)
            raise

    def _get_conn(self) -> mysql.connector.MySQLConnection:
        if self._pool is None:
            raise RuntimeError("TiDB pool not initialised — call connect() first")
        return self._pool.get_connection()

    def _exec(self, query: str, params: tuple = ()) -> None:
        """Execute a single statement (no result)."""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
        finally:
            cursor.close()
            conn.close()

    # ── Schema ────────────────────────────────────────────────────────

    def create_tables(self) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            # v1 tables (backward compat)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    source      VARCHAR(255) NOT NULL,
                    company     VARCHAR(255) NOT NULL,
                    signal_text TEXT NOT NULL,
                    signal_type VARCHAR(100) NOT NULL,
                    timestamp   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    confidence_score FLOAT DEFAULT 0.0,
                    INDEX idx_company (company),
                    INDEX idx_signal_type (signal_type),
                    INDEX idx_confidence (confidence_score)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS theses (
                    id           INT AUTO_INCREMENT PRIMARY KEY,
                    company      VARCHAR(255) NOT NULL,
                    thesis_text  TEXT NOT NULL,
                    confidence   FLOAT NOT NULL DEFAULT 0.0,
                    evidence_ids VARCHAR(500),
                    alerted      BOOLEAN DEFAULT FALSE,
                    thesis_state VARCHAR(20) DEFAULT 'candidate',
                    urgency_score FLOAT DEFAULT 0.0,
                    user_relevance_score FLOAT DEFAULT 0.0,
                    contradiction_score FLOAT DEFAULT 0.0,
                    evidence_strength_score FLOAT DEFAULT 0.0,
                    source_diversity_score FLOAT DEFAULT 0.0,
                    alert_score  FLOAT DEFAULT 0.0,
                    alert_status VARCHAR(20) DEFAULT 'none',
                    timestamp    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_company (company),
                    INDEX idx_confidence (confidence),
                    INDEX idx_state (thesis_state),
                    INDEX idx_alert_status (alert_status)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS generated_queries (
                    id               INT AUTO_INCREMENT PRIMARY KEY,
                    query_text       TEXT NOT NULL,
                    source_thesis_id INT NOT NULL,
                    used             BOOLEAN DEFAULT FALSE,
                    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_source_thesis (source_thesis_id),
                    INDEX idx_used (used)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS contradictions (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    thesis_a_id     INT NOT NULL,
                    thesis_b_id     INT NOT NULL,
                    contradiction   TEXT NOT NULL,
                    severity        VARCHAR(10) NOT NULL,
                    action_taken    VARCHAR(50) DEFAULT 'none',
                    detected_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_thesis_a (thesis_a_id),
                    INDEX idx_thesis_b (thesis_b_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trigger_log (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    trigger_reason  TEXT NOT NULL,
                    signals_before  INT NOT NULL DEFAULT 0,
                    signals_after   INT NOT NULL DEFAULT 0,
                    triggered_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    id               INT AUTO_INCREMENT PRIMARY KEY,
                    user_telegram_id VARCHAR(100) NOT NULL,
                    investment_focus TEXT,
                    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_telegram_id (user_telegram_id)
                )
            """)

            # ── v2 Phase 1 tables ─────────────────────────────────────

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entities (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    canonical_name  VARCHAR(255) NOT NULL,
                    entity_type     VARCHAR(50) DEFAULT 'company',
                    sector          VARCHAR(100),
                    geography       VARCHAR(100) DEFAULT 'Singapore',
                    aliases_json    TEXT,
                    parent_entity_id INT,
                    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_canonical (canonical_name),
                    INDEX idx_sector (sector)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sources (
                    id               INT AUTO_INCREMENT PRIMARY KEY,
                    source_name      VARCHAR(255) NOT NULL,
                    source_url       TEXT,
                    source_type      VARCHAR(50) DEFAULT 'news',
                    status           VARCHAR(20) DEFAULT 'active',
                    quality_score    FLOAT DEFAULT 50.0,
                    reliability_score FLOAT DEFAULT 50.0,
                    avg_signal_yield FLOAT DEFAULT 0.0,
                    added_by         VARCHAR(20) DEFAULT 'human',
                    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_source_name (source_name),
                    INDEX idx_status (status)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS queries_v2 (
                    id                   INT AUTO_INCREMENT PRIMARY KEY,
                    entity_id            INT,
                    thesis_id            INT,
                    query_text           TEXT NOT NULL,
                    query_type           VARCHAR(30) DEFAULT 'exploration',
                    status               VARCHAR(20) DEFAULT 'pending',
                    specificity_score    FLOAT DEFAULT 50.0,
                    expected_gain_score  FLOAT DEFAULT 50.0,
                    historical_yield     FLOAT DEFAULT 0.0,
                    created_by           VARCHAR(20) DEFAULT 'oracle',
                    created_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    expires_at           DATETIME,
                    INDEX idx_status (status),
                    INDEX idx_type (query_type),
                    INDEX idx_entity (entity_id),
                    INDEX idx_thesis (thesis_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_runs (
                    id                    INT AUTO_INCREMENT PRIMARY KEY,
                    query_id              INT NOT NULL,
                    executed_at           DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    result_count          INT DEFAULT 0,
                    useful_signal_count   INT DEFAULT 0,
                    novelty_yield         FLOAT DEFAULT 0.0,
                    triggered_thesis_update BOOLEAN DEFAULT FALSE,
                    INDEX idx_query (query_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS thesis_versions (
                    id               INT AUTO_INCREMENT PRIMARY KEY,
                    thesis_id        INT NOT NULL,
                    version_number   INT NOT NULL DEFAULT 1,
                    thesis_statement TEXT,
                    thesis_state     VARCHAR(20),
                    confidence_score FLOAT,
                    change_reason    TEXT,
                    generated_by_agent VARCHAR(50),
                    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_thesis (thesis_id)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS entity_heat (
                    id                    INT AUTO_INCREMENT PRIMARY KEY,
                    entity_id             INT,
                    company               VARCHAR(255),
                    heat_score            FLOAT DEFAULT 0.0,
                    signal_velocity       FLOAT DEFAULT 0.0,
                    contradiction_pressure FLOAT DEFAULT 0.0,
                    market_attention_score FLOAT DEFAULT 0.0,
                    investigation_priority FLOAT DEFAULT 0.0,
                    updated_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_company (company),
                    INDEX idx_heat (heat_score)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agent_policies (
                    id                INT AUTO_INCREMENT PRIMARY KEY,
                    policy_name       VARCHAR(100) NOT NULL,
                    current_value     FLOAT NOT NULL,
                    min_value         FLOAT DEFAULT 0.0,
                    max_value         FLOAT DEFAULT 100.0,
                    last_adjusted_by  VARCHAR(50) DEFAULT 'system',
                    adjustment_reason TEXT,
                    updated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_policy (policy_name)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id               INT AUTO_INCREMENT PRIMARY KEY,
                    thesis_id        INT NOT NULL,
                    alert_type       VARCHAR(30) NOT NULL,
                    alert_score      FLOAT NOT NULL DEFAULT 0.0,
                    confidence_score FLOAT DEFAULT 0.0,
                    channel          VARCHAR(30) DEFAULT 'dashboard',
                    acknowledged     BOOLEAN DEFAULT FALSE,
                    duplicate_of     INT,
                    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_thesis (thesis_id),
                    INDEX idx_type (alert_type),
                    INDEX idx_channel (channel)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawl_jobs (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    source_id       INT,
                    entity_id       INT,
                    query_id        INT,
                    crawl_type      VARCHAR(30) DEFAULT 'standard',
                    priority_score  FLOAT DEFAULT 50.0,
                    urgency_level   VARCHAR(20) DEFAULT 'normal',
                    scheduled_for   DATETIME,
                    status          VARCHAR(20) DEFAULT 'pending',
                    extraction_mode VARCHAR(30) DEFAULT 'serp',
                    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_status (status),
                    INDEX idx_priority (priority_score),
                    INDEX idx_scheduled (scheduled_for)
                )
            """)

            # Seed default policies
            cursor.execute("""
                INSERT IGNORE INTO agent_policies (policy_name, current_value, min_value, max_value, adjustment_reason)
                VALUES
                    ('max_deep_investigations_per_hour', 3, 1, 10, 'Phase 1 default'),
                    ('max_crawl_budget_per_hour', 50, 10, 200, 'Phase 1 default'),
                    ('max_query_expansions_per_thesis', 6, 3, 15, 'Phase 1 default'),
                    ('min_evidence_lines_for_alert', 2, 1, 5, 'Phase 1 default'),
                    ('stale_thesis_days', 7, 3, 30, 'Phase 1 default'),
                    ('query_expiry_days', 7, 3, 14, 'Phase 1 default')
            """)

            # Seed default sources
            cursor.execute("""
                INSERT IGNORE INTO sources (source_name, source_url, source_type, status, quality_score, added_by)
                VALUES
                    ('e27', 'https://e27.co', 'news', 'active', 70.0, 'human'),
                    ('techinasia', 'https://www.techinasia.com', 'news', 'active', 75.0, 'human'),
                    ('channelnewsasia', 'https://www.channelnewsasia.com', 'news', 'active', 85.0, 'human'),
                    ('mycareersfuture', 'https://www.mycareersfuture.gov.sg', 'jobs', 'active', 80.0, 'human'),
                    ('linkedin', 'https://www.linkedin.com/jobs', 'jobs', 'active', 70.0, 'human')
            """)

            logger.info("All v1 + v2 Phase 1 tables created / verified")
        except Error as e:
            logger.error("Table creation failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

        # Upgrade existing theses table if columns missing
        self._upgrade_theses_table()

    def _upgrade_theses_table(self) -> None:
        """Add v2 columns to existing theses table (idempotent)."""
        new_columns = [
            ("thesis_state", "VARCHAR(20) DEFAULT 'candidate'"),
            ("urgency_score", "FLOAT DEFAULT 0.0"),
            ("user_relevance_score", "FLOAT DEFAULT 0.0"),
            ("contradiction_score", "FLOAT DEFAULT 0.0"),
            ("evidence_strength_score", "FLOAT DEFAULT 0.0"),
            ("source_diversity_score", "FLOAT DEFAULT 0.0"),
            ("alert_score", "FLOAT DEFAULT 0.0"),
            ("alert_status", "VARCHAR(20) DEFAULT 'none'"),
        ]
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            for col_name, col_def in new_columns:
                try:
                    cursor.execute(f"ALTER TABLE theses ADD COLUMN {col_name} {col_def}")
                    logger.debug("Added column %s to theses", col_name)
                except Error:
                    pass  # column already exists
        finally:
            cursor.close()
            conn.close()

    # ── Signals ───────────────────────────────────────────────────────

    def insert_signal(
        self, source: str, company: str, signal_text: str,
        signal_type: str, confidence_score: float = 0.0,
    ) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO signals (source, company, signal_text, signal_type, confidence_score) VALUES (%s, %s, %s, %s, %s)",
                (source, company, signal_text, signal_type, confidence_score),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_signal failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_signals(self, company: Optional[str] = None, signal_type: Optional[str] = None, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            query = "SELECT * FROM signals WHERE 1=1"
            params: list = []
            if company:
                query += " AND company = %s"
                params.append(company)
            if signal_type:
                query += " AND signal_type = %s"
                params.append(signal_type)
            query += " ORDER BY timestamp DESC LIMIT %s"
            params.append(limit)
            cursor.execute(query, params)
            return cursor.fetchall()
        except Error as e:
            logger.error("get_signals failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_recent_signals(self, hours: int = 24, limit: int = 100) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM signals WHERE timestamp >= NOW() - INTERVAL %s HOUR ORDER BY timestamp DESC LIMIT %s",
                (hours, limit),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_recent_signals failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_signal_count_between_hours(self, hours_ago_start: int, hours_ago_end: int) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM signals WHERE timestamp >= NOW() - INTERVAL %s HOUR AND timestamp < NOW() - INTERVAL %s HOUR",
                (hours_ago_end, hours_ago_start),
            )
            return cursor.fetchone()[0]
        except Error as e:
            logger.error("get_signal_count_between_hours failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_signals_for_company(self, company: str, hours: int = 168) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM signals WHERE company = %s AND timestamp >= NOW() - INTERVAL %s HOUR ORDER BY timestamp DESC",
                (company, hours),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_signals_for_company failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Theses ────────────────────────────────────────────────────────

    def insert_thesis(
        self, company: str, thesis_text: str, confidence: float,
        evidence_ids: list[int], thesis_state: str = "candidate",
        urgency_score: float = 0.0, evidence_strength_score: float = 0.0,
        source_diversity_score: float = 0.0,
    ) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            ids_str = ",".join(str(i) for i in evidence_ids)
            cursor.execute(
                """INSERT INTO theses (company, thesis_text, confidence, evidence_ids, thesis_state,
                   urgency_score, evidence_strength_score, source_diversity_score)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (company, thesis_text, confidence, ids_str, thesis_state,
                 urgency_score, evidence_strength_score, source_diversity_score),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_thesis failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_theses(self, company: Optional[str] = None, min_confidence: float = 0.0, limit: int = 20) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            query = "SELECT * FROM theses WHERE confidence >= %s"
            params: list = [min_confidence]
            if company:
                query += " AND company = %s"
                params.append(company)
            query += " ORDER BY timestamp DESC LIMIT %s"
            params.append(limit)
            cursor.execute(query, params)
            return cursor.fetchall()
        except Error as e:
            logger.error("get_theses failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_theses_by_state(self, state: str, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM theses WHERE thesis_state = %s ORDER BY confidence DESC LIMIT %s",
                (state, limit),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_theses_by_state failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_theses_since_days(self, days: int = 7) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM theses WHERE timestamp >= NOW() - INTERVAL %s DAY ORDER BY timestamp DESC",
                (days,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_theses_since_days failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_active_theses(self, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM theses WHERE thesis_state NOT IN ('discarded','stale') ORDER BY confidence DESC LIMIT %s",
                (limit,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_active_theses failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def update_thesis_state(self, thesis_id: int, new_state: str, reason: str = "", agent: str = "system") -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            # Get current version for history
            cursor.execute("SELECT thesis_text, thesis_state, confidence FROM theses WHERE id = %s", (thesis_id,))
            row = cursor.fetchone()
            if row:
                cursor.execute(
                    """INSERT INTO thesis_versions (thesis_id, version_number, thesis_statement, thesis_state, confidence_score, change_reason, generated_by_agent)
                       VALUES (%s, (SELECT COALESCE(MAX(v.version_number),0)+1 FROM thesis_versions v WHERE v.thesis_id=%s), %s, %s, %s, %s, %s)""",
                    (thesis_id, thesis_id, row[0], row[1], row[2], reason, agent),
                )
            cursor.execute("UPDATE theses SET thesis_state = %s WHERE id = %s", (new_state, thesis_id))
            logger.debug("Thesis %s state → %s (%s)", thesis_id, new_state, reason)
        except Error as e:
            logger.error("update_thesis_state failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def update_thesis_scores(self, thesis_id: int, **scores) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            valid = {"confidence", "urgency_score", "user_relevance_score", "contradiction_score",
                     "evidence_strength_score", "source_diversity_score", "alert_score", "alert_status", "thesis_state"}
            sets = []
            params = []
            for k, v in scores.items():
                if k in valid:
                    sets.append(f"{k} = %s")
                    params.append(v)
            if sets:
                params.append(thesis_id)
                cursor.execute(f"UPDATE theses SET {', '.join(sets)} WHERE id = %s", tuple(params))
        except Error as e:
            logger.error("update_thesis_scores failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_high_confidence_theses(self, threshold: float = 75.0) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM theses WHERE confidence >= %s AND alerted = FALSE ORDER BY timestamp DESC",
                (threshold,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_high_confidence_theses failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def mark_thesis_alerted(self, thesis_id: int) -> None:
        self._exec("UPDATE theses SET alerted = TRUE WHERE id = %s", (thesis_id,))

    # ── Contradictions ─────────────────────────────────────────────────

    def insert_contradiction(self, thesis_a_id: int, thesis_b_id: int, contradiction: str, severity: str, action_taken: str = "none") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO contradictions (thesis_a_id, thesis_b_id, contradiction, severity, action_taken) VALUES (%s, %s, %s, %s, %s)",
                (thesis_a_id, thesis_b_id, contradiction, severity, action_taken),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_contradiction failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_contradictions(self, limit: int = 20) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """SELECT c.*, ta.company AS company_a, ta.thesis_text AS thesis_a_text, ta.confidence AS confidence_a,
                          tb.company AS company_b, tb.thesis_text AS thesis_b_text, tb.confidence AS confidence_b
                   FROM contradictions c
                   JOIN theses ta ON c.thesis_a_id = ta.id
                   JOIN theses tb ON c.thesis_b_id = tb.id
                   ORDER BY c.detected_at DESC LIMIT %s""",
                (limit,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_contradictions failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Entity Heat ────────────────────────────────────────────────────

    def upsert_entity_heat(self, company: str, heat_score: float, signal_velocity: float,
                           contradiction_pressure: float, market_attention: float, investigation_priority: float) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO entity_heat (company, heat_score, signal_velocity, contradiction_pressure, market_attention_score, investigation_priority, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, NOW())
                   ON DUPLICATE KEY UPDATE heat_score=%s, signal_velocity=%s, contradiction_pressure=%s,
                   market_attention_score=%s, investigation_priority=%s, updated_at=NOW()""",
                (company, heat_score, signal_velocity, contradiction_pressure, market_attention, investigation_priority,
                 heat_score, signal_velocity, contradiction_pressure, market_attention, investigation_priority),
            )
        except Error as e:
            logger.error("upsert_entity_heat failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_entity_heat(self, limit: int = 20) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM entity_heat ORDER BY heat_score DESC LIMIT %s", (limit,))
            return cursor.fetchall()
        except Error as e:
            logger.error("get_entity_heat failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_hottest_companies(self, limit: int = 10) -> list[str]:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT company FROM entity_heat ORDER BY heat_score DESC LIMIT %s", (limit,))
            return [row[0] for row in cursor.fetchall()]
        except Error as e:
            logger.error("get_hottest_companies failed: %s", e)
            return []
        finally:
            cursor.close()
            conn.close()

    # ── Queries v2 ─────────────────────────────────────────────────────

    def insert_query_v2(self, query_text: str, query_type: str = "exploration",
                        entity_id: int = None, thesis_id: int = None,
                        specificity: float = 50.0, expected_gain: float = 50.0) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO queries_v2 (entity_id, thesis_id, query_text, query_type, specificity_score, expected_gain_score, expires_at)
                   VALUES (%s, %s, %s, %s, %s, %s, NOW() + INTERVAL 7 DAY)""",
                (entity_id, thesis_id, query_text, query_type, specificity, expected_gain),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_query_v2 failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_pending_queries_v2(self, limit: int = 10) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """SELECT * FROM queries_v2
                   WHERE status = 'pending' AND (expires_at IS NULL OR expires_at > NOW())
                   ORDER BY expected_gain_score DESC, specificity_score DESC LIMIT %s""",
                (limit,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_pending_queries_v2 failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def mark_query_v2_status(self, query_id: int, status: str) -> None:
        self._exec("UPDATE queries_v2 SET status = %s WHERE id = %s", (status, query_id))

    def insert_query_run(self, query_id: int, result_count: int, useful_count: int,
                         novelty_yield: float, triggered_update: bool) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO query_runs (query_id, result_count, useful_signal_count, novelty_yield, triggered_thesis_update) VALUES (%s,%s,%s,%s,%s)",
                (query_id, result_count, useful_count, novelty_yield, triggered_update),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_query_run failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def expire_old_queries(self) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE queries_v2 SET status = 'expired' WHERE status = 'pending' AND expires_at <= NOW()")
            return cursor.rowcount
        except Error as e:
            logger.error("expire_old_queries failed: %s", e)
            return 0
        finally:
            cursor.close()
            conn.close()

    # ── Alerts ─────────────────────────────────────────────────────────

    def insert_alert(self, thesis_id: int, alert_type: str, alert_score: float,
                     confidence_score: float, channel: str) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO alerts (thesis_id, alert_type, alert_score, confidence_score, channel) VALUES (%s,%s,%s,%s,%s)",
                (thesis_id, alert_type, alert_score, confidence_score, channel),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_alert failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_recent_alerts(self, hours: int = 24) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM alerts WHERE created_at >= NOW() - INTERVAL %s HOUR ORDER BY created_at DESC",
                (hours,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_recent_alerts failed: %s", e)
            return []
        finally:
            cursor.close()
            conn.close()

    def count_alerts_for_thesis(self, thesis_id: int, hours: int = 24) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM alerts WHERE thesis_id = %s AND created_at >= NOW() - INTERVAL %s HOUR",
                (thesis_id, hours),
            )
            return cursor.fetchone()[0]
        except Error as e:
            return 0
        finally:
            cursor.close()
            conn.close()

    # ── Sources ────────────────────────────────────────────────────────

    def get_active_sources(self) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM sources WHERE status = 'active' ORDER BY quality_score DESC")
            return cursor.fetchall()
        except Error as e:
            logger.error("get_active_sources failed: %s", e)
            return []
        finally:
            cursor.close()
            conn.close()

    def get_source_quality(self, source_name: str) -> float:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT quality_score FROM sources WHERE source_name = %s", (source_name,))
            row = cursor.fetchone()
            return row[0] if row else 50.0
        except Error:
            return 50.0
        finally:
            cursor.close()
            conn.close()

    # ── Crawl Jobs ─────────────────────────────────────────────────────

    def insert_crawl_job(self, source_id: int = None, entity_id: int = None, query_id: int = None,
                         crawl_type: str = "standard", priority: float = 50.0,
                         urgency: str = "normal", extraction_mode: str = "serp") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO crawl_jobs (source_id, entity_id, query_id, crawl_type, priority_score, urgency_level, extraction_mode, scheduled_for)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())""",
                (source_id, entity_id, query_id, crawl_type, priority, urgency, extraction_mode),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_crawl_job failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_pending_crawl_jobs(self, limit: int = 20) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """SELECT cj.*, s.source_name, s.source_url
                   FROM crawl_jobs cj
                   LEFT JOIN sources s ON cj.source_id = s.id
                   WHERE cj.status = 'pending' AND cj.scheduled_for <= NOW()
                   ORDER BY cj.priority_score DESC LIMIT %s""",
                (limit,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_pending_crawl_jobs failed: %s", e)
            return []
        finally:
            cursor.close()
            conn.close()

    def update_crawl_job_status(self, job_id: int, status: str) -> None:
        self._exec("UPDATE crawl_jobs SET status = %s WHERE id = %s", (status, job_id))

    # ── Policies ───────────────────────────────────────────────────────

    def get_policy(self, name: str) -> float:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT current_value FROM agent_policies WHERE policy_name = %s", (name,))
            row = cursor.fetchone()
            return row[0] if row else 0.0
        except Error:
            return 0.0
        finally:
            cursor.close()
            conn.close()

    # ── Trigger Log ────────────────────────────────────────────────────

    def insert_trigger_log(self, trigger_reason: str, signals_before: int, signals_after: int) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO trigger_log (trigger_reason, signals_before, signals_after) VALUES (%s, %s, %s)",
                (trigger_reason, signals_before, signals_after),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_trigger_log failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Generated Queries (v1 compat) ──────────────────────────────────

    def insert_generated_query(self, query_text: str, source_thesis_id: int) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO generated_queries (query_text, source_thesis_id) VALUES (%s, %s)",
                (query_text, source_thesis_id),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_generated_query failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_unused_queries(self, limit: int = 10) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM generated_queries WHERE used = FALSE ORDER BY created_at ASC LIMIT %s", (limit,))
            return cursor.fetchall()
        except Error as e:
            logger.error("get_unused_queries failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def mark_query_used(self, query_id: int) -> None:
        self._exec("UPDATE generated_queries SET used = TRUE WHERE id = %s", (query_id,))

    # ── User Preferences ──────────────────────────────────────────────

    def upsert_user_preference(self, user_telegram_id: str, investment_focus: str) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO user_preferences (user_telegram_id, investment_focus) VALUES (%s, %s) ON DUPLICATE KEY UPDATE investment_focus = %s",
                (user_telegram_id, investment_focus, investment_focus),
            )
        except Error as e:
            logger.error("upsert_user_preference failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_user_preference(self, user_telegram_id: str) -> Optional[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM user_preferences WHERE user_telegram_id = %s", (user_telegram_id,))
            return cursor.fetchone()
        except Error as e:
            logger.error("get_user_preference failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()
