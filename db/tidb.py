import os
import logging
from typing import Optional

import certifi
import mysql.connector
from mysql.connector import pooling, Error
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


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
        """Initialise the connection pool."""
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

    # ── Schema ────────────────────────────────────────────────────────

    def create_tables(self) -> None:
        """Create all required tables if they don't exist."""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
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
                    timestamp    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_company (company),
                    INDEX idx_confidence (confidence)
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
            logger.info("All tables created / verified")
        except Error as e:
            logger.error("Table creation failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Signals ───────────────────────────────────────────────────────

    def insert_signal(
        self,
        source: str,
        company: str,
        signal_text: str,
        signal_type: str,
        confidence_score: float = 0.0,
    ) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO signals (source, company, signal_text, signal_type, confidence_score)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (source, company, signal_text, signal_type, confidence_score),
            )
            signal_id = cursor.lastrowid
            logger.debug("Inserted signal %s for %s", signal_id, company)
            return signal_id
        except Error as e:
            logger.error("insert_signal failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_signals(
        self,
        company: Optional[str] = None,
        signal_type: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
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
                """
                SELECT * FROM signals
                WHERE timestamp >= NOW() - INTERVAL %s HOUR
                ORDER BY timestamp DESC LIMIT %s
                """,
                (hours, limit),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_recent_signals failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Theses ────────────────────────────────────────────────────────

    def insert_thesis(
        self,
        company: str,
        thesis_text: str,
        confidence: float,
        evidence_ids: list[int],
    ) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            ids_str = ",".join(str(i) for i in evidence_ids)
            cursor.execute(
                """
                INSERT INTO theses (company, thesis_text, confidence, evidence_ids)
                VALUES (%s, %s, %s, %s)
                """,
                (company, thesis_text, confidence, ids_str),
            )
            thesis_id = cursor.lastrowid
            logger.debug("Inserted thesis %s for %s (confidence %.1f)", thesis_id, company, confidence)
            return thesis_id
        except Error as e:
            logger.error("insert_thesis failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def get_theses(
        self,
        company: Optional[str] = None,
        min_confidence: float = 0.0,
        limit: int = 20,
    ) -> list[dict]:
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

    def get_high_confidence_theses(self, threshold: float = 75.0) -> list[dict]:
        """Fetch high-confidence theses that have NOT been alerted yet."""
        conn = self._get_conn()
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                """
                SELECT * FROM theses
                WHERE confidence >= %s AND alerted = FALSE
                ORDER BY timestamp DESC
                """,
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
        """Mark a thesis as alerted so it won't trigger duplicate alerts."""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE theses SET alerted = TRUE WHERE id = %s",
                (thesis_id,),
            )
            logger.debug("Marked thesis %s as alerted", thesis_id)
        except Error as e:
            logger.error("mark_thesis_alerted failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Trigger Engine ─────────────────────────────────────────────────

    def get_signal_count_between_hours(self, hours_ago_start: int, hours_ago_end: int) -> int:
        """Count signals between two hour offsets from now. E.g. (0,1) = last hour."""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT COUNT(*) FROM signals
                WHERE timestamp >= NOW() - INTERVAL %s HOUR
                  AND timestamp < NOW() - INTERVAL %s HOUR
                """,
                (hours_ago_end, hours_ago_start),
            )
            return cursor.fetchone()[0]
        except Error as e:
            logger.error("get_signal_count_between_hours failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def insert_trigger_log(self, trigger_reason: str, signals_before: int, signals_after: int) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO trigger_log (trigger_reason, signals_before, signals_after)
                VALUES (%s, %s, %s)
                """,
                (trigger_reason, signals_before, signals_after),
            )
            log_id = cursor.lastrowid
            logger.debug("Inserted trigger log %s: %s", log_id, trigger_reason)
            return log_id
        except Error as e:
            logger.error("insert_trigger_log failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── Generated Queries ─────────────────────────────────────────────

    def insert_generated_query(self, query_text: str, source_thesis_id: int) -> int:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO generated_queries (query_text, source_thesis_id)
                VALUES (%s, %s)
                """,
                (query_text, source_thesis_id),
            )
            query_id = cursor.lastrowid
            logger.debug("Inserted generated query %s for thesis %s", query_id, source_thesis_id)
            return query_id
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
            cursor.execute(
                """
                SELECT * FROM generated_queries
                WHERE used = FALSE
                ORDER BY created_at ASC LIMIT %s
                """,
                (limit,),
            )
            return cursor.fetchall()
        except Error as e:
            logger.error("get_unused_queries failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def mark_query_used(self, query_id: int) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE generated_queries SET used = TRUE WHERE id = %s",
                (query_id,),
            )
            logger.debug("Marked query %s as used", query_id)
        except Error as e:
            logger.error("mark_query_used failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    # ── User Preferences ──────────────────────────────────────────────

    def upsert_user_preference(self, user_telegram_id: str, investment_focus: str) -> None:
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                INSERT INTO user_preferences (user_telegram_id, investment_focus)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE investment_focus = %s
                """,
                (user_telegram_id, investment_focus, investment_focus),
            )
            logger.debug("Upserted preference for user %s", user_telegram_id)
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
            cursor.execute(
                "SELECT * FROM user_preferences WHERE user_telegram_id = %s",
                (user_telegram_id,),
            )
            return cursor.fetchone()
        except Error as e:
            logger.error("get_user_preference failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()
