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

            # ── Phase 3 tables ─────────────────────────────────────

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS raw_documents (
                    id                    INT AUTO_INCREMENT PRIMARY KEY,
                    source_id             INT,
                    url                   VARCHAR(500),
                    title                 TEXT,
                    raw_text              LONGTEXT,
                    fetch_timestamp       DATETIME DEFAULT CURRENT_TIMESTAMP,
                    published_timestamp   DATETIME NULL,
                    extraction_confidence INT DEFAULT 0,
                    content_hash          VARCHAR(64),
                    language              VARCHAR(10) DEFAULT 'en',
                    metadata_json         TEXT,
                    INDEX idx_source (source_id),
                    INDEX idx_hash (content_hash),
                    INDEX idx_fetch (fetch_timestamp)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signal_links (
                    id                INT AUTO_INCREMENT PRIMARY KEY,
                    signal_id_1       INT NOT NULL,
                    signal_id_2       INT NOT NULL,
                    relationship_type VARCHAR(50) NOT NULL,
                    strength_score    INT DEFAULT 0,
                    created_at        DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sig1 (signal_id_1),
                    INDEX idx_sig2 (signal_id_2),
                    INDEX idx_type (relationship_type)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_health (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    component_name  VARCHAR(100) NOT NULL,
                    status          VARCHAR(20) DEFAULT 'unknown',
                    error_rate      FLOAT DEFAULT 0,
                    avg_latency_ms  INT DEFAULT 0,
                    backlog_size    INT DEFAULT 0,
                    recovery_action TEXT,
                    checked_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_component (component_name)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS source_performance_daily (
                    id                        INT AUTO_INCREMENT PRIMARY KEY,
                    source_id                 INT NOT NULL,
                    date                      DATE NOT NULL,
                    fetch_success_rate        FLOAT DEFAULT 0,
                    useful_signal_count       INT DEFAULT 0,
                    avg_novelty_score         FLOAT DEFAULT 0,
                    avg_reliability_score     FLOAT DEFAULT 0,
                    false_signal_rate_estimate FLOAT DEFAULT 0,
                    created_at                DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE INDEX idx_source_date (source_id, date)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reports (
                    id          INT AUTO_INCREMENT PRIMARY KEY,
                    report_type VARCHAR(50) DEFAULT 'daily_brief',
                    report_date DATE,
                    html_content LONGTEXT,
                    sent_via    VARCHAR(30),
                    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trigger_events (
                    id              INT AUTO_INCREMENT PRIMARY KEY,
                    trigger_type    VARCHAR(50) NOT NULL,
                    urgency_level   VARCHAR(20) DEFAULT 'medium',
                    target_entities TEXT,
                    reason          TEXT,
                    recommended_cycle VARCHAR(20) DEFAULT 'standard',
                    created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_type (trigger_type)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS investigations (
                    id            INT AUTO_INCREMENT PRIMARY KEY,
                    thesis_id     INT NOT NULL,
                    trigger_reason TEXT,
                    status        VARCHAR(20) DEFAULT 'pending',
                    result_type   VARCHAR(30),
                    findings_summary TEXT,
                    signals_found INT DEFAULT 0,
                    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                    completed_at  DATETIME,
                    INDEX idx_thesis (thesis_id),
                    INDEX idx_status (status)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS policy_log (
                    id            INT AUTO_INCREMENT PRIMARY KEY,
                    action_type   VARCHAR(50) NOT NULL,
                    blocked       BOOLEAN DEFAULT FALSE,
                    reason        TEXT,
                    context_json  TEXT,
                    created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_action (action_type)
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
                    ('query_expiry_days', 7, 3, 14, 'Phase 1 default'),
                    ('max_source_auto_adds_per_day', 3, 1, 10, 'Phase 2 default'),
                    ('source_probation_days', 7, 3, 14, 'Phase 2 default'),
                    ('min_alert_score_telegram', 85, 60, 95, 'Phase 2 default'),
                    ('min_alert_score_channel', 92, 80, 100, 'Phase 2 default'),
                    ('narrative_lockin_hours', 48, 24, 96, 'Phase 2 default'),
                    ('api_calls_per_hour_limit', 100, 20, 500, 'Phase 2 default')
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

            logger.info("All v2 tables created / verified (Phase 1-3)")
        except Error as e:
            logger.error("Table creation failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

        # Upgrade existing theses table if columns missing
        self._upgrade_theses_table()

    def _upgrade_theses_table(self) -> None:
        """Add v2 columns to existing tables (idempotent)."""
        new_columns = [
            ("theses", "thesis_state", "VARCHAR(20) DEFAULT 'candidate'"),
            ("theses", "urgency_score", "FLOAT DEFAULT 0.0"),
            ("theses", "user_relevance_score", "FLOAT DEFAULT 0.0"),
            ("theses", "contradiction_score", "FLOAT DEFAULT 0.0"),
            ("theses", "evidence_strength_score", "FLOAT DEFAULT 0.0"),
            ("theses", "source_diversity_score", "FLOAT DEFAULT 0.0"),
            ("theses", "alert_score", "FLOAT DEFAULT 0.0"),
            ("theses", "alert_status", "VARCHAR(20) DEFAULT 'none'"),
            ("contradictions", "action_taken", "VARCHAR(50) DEFAULT 'none'"),
        ]
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            for table, col_name, col_def in new_columns:
                try:
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_def}")
                    logger.debug("Added column %s to %s", col_name, table)
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
                # Pre-calculate next version number (TiDB can't self-reference in INSERT subquery)
                cursor.execute("SELECT COALESCE(MAX(version_number),0)+1 FROM thesis_versions WHERE thesis_id=%s", (thesis_id,))
                next_version = cursor.fetchone()[0]
                cursor.execute(
                    """INSERT INTO thesis_versions (thesis_id, version_number, thesis_statement, thesis_state, confidence_score, change_reason, generated_by_agent)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (thesis_id, next_version, row[0], row[1], row[2], reason, agent),
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

    # ── Raw Documents ──────────────────────────────────────────────────

    def insert_raw_document(self, source_id: int, url: str, title: str, raw_text: str,
                            extraction_confidence: int = 0, content_hash: str = "") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO raw_documents (source_id, url, title, raw_text, extraction_confidence, content_hash)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (source_id, url, title, raw_text, extraction_confidence, content_hash),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_raw_document failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_raw_documents_recent(self, hours: int = 24) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM raw_documents WHERE fetch_timestamp >= NOW() - INTERVAL %s HOUR ORDER BY fetch_timestamp DESC LIMIT 200",
                (hours,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_raw_documents_recent failed: %s", e)
            return []
        finally:
            cursor.close()
            conn.close()

    def check_content_hash_exists(self, content_hash: str) -> bool:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1 FROM raw_documents WHERE content_hash = %s LIMIT 1", (content_hash,))
            return cursor.fetchone() is not None
        except Error:
            return False
        finally:
            cursor.close()
            conn.close()

    # ── Signal Links ───────────────────────────────────────────────────

    def insert_signal_link(self, signal_id_1: int, signal_id_2: int, relationship_type: str, strength_score: int = 50) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO signal_links (signal_id_1, signal_id_2, relationship_type, strength_score) VALUES (%s,%s,%s,%s)",
                (signal_id_1, signal_id_2, relationship_type, strength_score),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_signal_link failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_signal_links_recent(self, hours: int = 24, limit: int = 50) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM signal_links WHERE created_at >= NOW() - INTERVAL %s HOUR ORDER BY created_at DESC LIMIT %s",
                (hours, limit),
            )
            return cursor.fetchall()
        except Error as e:
            return []
        finally:
            cursor.close()
            conn.close()

    # ── System Health ──────────────────────────────────────────────────

    def upsert_system_health(self, component_name: str, status: str, error_rate: float = 0,
                             avg_latency_ms: int = 0, backlog_size: int = 0, recovery_action: str = None) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO system_health (component_name, status, error_rate, avg_latency_ms, backlog_size, recovery_action, checked_at)
                   VALUES (%s,%s,%s,%s,%s,%s,NOW())
                   ON DUPLICATE KEY UPDATE status=%s, error_rate=%s, avg_latency_ms=%s, backlog_size=%s, recovery_action=%s, checked_at=NOW()""",
                (component_name, status, error_rate, avg_latency_ms, backlog_size, recovery_action,
                 status, error_rate, avg_latency_ms, backlog_size, recovery_action),
            )
        except Error as e:
            logger.error("upsert_system_health failed: %s", e)
        finally:
            cursor.close()
            conn.close()

    def get_system_health(self) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM system_health ORDER BY component_name")
            return cursor.fetchall()
        except Error as e:
            return []
        finally:
            cursor.close()
            conn.close()

    # ── Source Performance ─────────────────────────────────────────────

    def upsert_source_performance_daily(self, source_id: int, date: str, fetch_success_rate: float,
                                        useful_signal_count: int, avg_novelty: float, avg_reliability: float,
                                        false_signal_rate: float = 0) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO source_performance_daily (source_id, date, fetch_success_rate, useful_signal_count,
                   avg_novelty_score, avg_reliability_score, false_signal_rate_estimate)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE fetch_success_rate=%s, useful_signal_count=%s, avg_novelty_score=%s,
                   avg_reliability_score=%s, false_signal_rate_estimate=%s""",
                (source_id, date, fetch_success_rate, useful_signal_count, avg_novelty, avg_reliability, false_signal_rate,
                 fetch_success_rate, useful_signal_count, avg_novelty, avg_reliability, false_signal_rate),
            )
        except Error as e:
            logger.error("upsert_source_performance_daily failed: %s", e)
        finally:
            cursor.close()
            conn.close()

    def get_source_performance_30d(self, source_id: int) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                "SELECT * FROM source_performance_daily WHERE source_id = %s AND date >= CURDATE() - INTERVAL 30 DAY ORDER BY date DESC",
                (source_id,),
            )
            return cursor.fetchall()
        except Error as e:
            return []
        finally:
            cursor.close()
            conn.close()

    def update_source_quality(self, source_id: int, quality_score: float, reliability_score: float) -> None:
        self._exec("UPDATE sources SET quality_score=%s, reliability_score=%s WHERE id=%s",
                    (quality_score, reliability_score, source_id))

    def update_source_status(self, source_id: int, status: str) -> None:
        self._exec("UPDATE sources SET status=%s WHERE id=%s", (status, source_id))

    def get_sources_by_status(self, status: str) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM sources WHERE status=%s ORDER BY quality_score DESC", (status,))
            return cursor.fetchall()
        except Error as e:
            return []
        finally:
            cursor.close()
            conn.close()

    def insert_source(self, source_name: str, source_url: str, source_type: str = "news",
                      status: str = "candidate", added_by: str = "oracle") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO sources (source_name, source_url, source_type, status, added_by) VALUES (%s,%s,%s,%s,%s)",
                (source_name, source_url, source_type, status, added_by),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_source failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Entities ───────────────────────────────────────────────────────

    def upsert_entity(self, canonical_name: str, entity_type: str = "company",
                      sector: str = None, geography: str = "Singapore") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """INSERT INTO entities (canonical_name, entity_type, sector, geography)
                   VALUES (%s,%s,%s,%s)
                   ON DUPLICATE KEY UPDATE sector=COALESCE(%s, sector)""",
                (canonical_name, entity_type, sector, geography, sector),
            )
            cursor.execute("SELECT id FROM entities WHERE canonical_name=%s", (canonical_name,))
            row = cursor.fetchone()
            return row[0] if row else 0
        except Error as e:
            logger.error("upsert_entity failed: %s", e)
            return 0
        finally:
            cursor.close()
            conn.close()

    def get_entity_by_name(self, name: str) -> Optional[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM entities WHERE canonical_name=%s", (name,))
            return cursor.fetchone()
        except Error:
            return None
        finally:
            cursor.close()
            conn.close()

    # ── Investigations ─────────────────────────────────────────────────

    def insert_investigation(self, thesis_id: int, trigger_reason: str) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO investigations (thesis_id, trigger_reason) VALUES (%s,%s)",
                (thesis_id, trigger_reason),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_investigation failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def update_investigation(self, inv_id: int, status: str, result_type: str = None,
                             findings: str = None, signals_found: int = 0) -> None:
        self._exec(
            "UPDATE investigations SET status=%s, result_type=%s, findings_summary=%s, signals_found=%s, completed_at=NOW() WHERE id=%s",
            (status, result_type, findings, signals_found, inv_id),
        )

    def get_recent_investigations(self, limit: int = 10) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """SELECT i.*, t.company, t.thesis_text FROM investigations i
                   JOIN theses t ON i.thesis_id = t.id ORDER BY i.created_at DESC LIMIT %s""",
                (limit,),
            )
            return cursor.fetchall()
        except Error as e:
            return []
        finally:
            cursor.close()
            conn.close()

    def count_investigations_last_hour(self) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM investigations WHERE created_at >= NOW() - INTERVAL 1 HOUR")
            return cursor.fetchone()[0]
        except Error:
            return 0
        finally:
            cursor.close()
            conn.close()

    # ── Trigger Events ─────────────────────────────────────────────────

    def insert_trigger_event(self, trigger_type: str, urgency: str, target_entities: str,
                             reason: str, recommended_cycle: str = "standard") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO trigger_events (trigger_type, urgency_level, target_entities, reason, recommended_cycle) VALUES (%s,%s,%s,%s,%s)",
                (trigger_type, urgency, target_entities, reason, recommended_cycle),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_trigger_event failed: %s", e)
            return 0
        finally:
            cursor.close()
            conn.close()

    # ── Reports ────────────────────────────────────────────────────────

    def insert_report(self, report_type: str, report_date: str, html_content: str, sent_via: str = "telegram") -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO reports (report_type, report_date, html_content, sent_via) VALUES (%s,%s,%s,%s)",
                (report_type, report_date, html_content, sent_via),
            )
            return cursor.lastrowid
        except Error as e:
            logger.error("insert_report failed: %s", e)
            return 0
        finally:
            cursor.close()
            conn.close()

    # ── Policy Log ─────────────────────────────────────────────────────

    def insert_policy_log(self, action_type: str, blocked: bool, reason: str, context: str = None) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO policy_log (action_type, blocked, reason, context_json) VALUES (%s,%s,%s,%s)",
                (action_type, blocked, reason, context),
            )
        except Error:
            pass
        finally:
            cursor.close()
            conn.close()

    def update_policy(self, name: str, new_value: float, adjusted_by: str, reason: str) -> None:
        self._exec(
            "UPDATE agent_policies SET current_value=%s, last_adjusted_by=%s, adjustment_reason=%s, updated_at=NOW() WHERE policy_name=%s",
            (new_value, adjusted_by, reason, name),
        )

    def get_all_policies(self) -> list[dict]:
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM agent_policies ORDER BY policy_name")
            return cursor.fetchall()
        except Error:
            return []
        finally:
            cursor.close()
            conn.close()
