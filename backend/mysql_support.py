from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any, Iterable, Sequence

import pymysql
from pymysql.cursors import DictCursor


class MySQLClientError(RuntimeError):
    pass


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def mysql_schema_statements() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS buildings (
            building_id VARCHAR(128) NOT NULL PRIMARY KEY,
            site_id VARCHAR(64) NULL,
            primaryspaceusage VARCHAR(128) NULL,
            sub_primaryspaceusage VARCHAR(128) NULL,
            peer_category VARCHAR(64) NULL,
            display_category VARCHAR(64) NULL,
            display_name VARCHAR(255) NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            KEY idx_buildings_peer_category (peer_category),
            KEY idx_buildings_site_id (site_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS energy_timeseries (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            record_id BIGINT NULL,
            building_id VARCHAR(128) NOT NULL,
            building_name VARCHAR(255) NOT NULL,
            building_type VARCHAR(64) NULL,
            metric_type VARCHAR(32) NOT NULL,
            ts DATETIME NOT NULL,
            hour_of_day TINYINT NOT NULL,
            value DOUBLE NOT NULL,
            unit VARCHAR(32) NOT NULL,
            source VARCHAR(64) NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY uq_energy_metric_building_ts (metric_type, building_id, ts),
            KEY idx_energy_building_ts (building_id, ts),
            KEY idx_energy_metric_ts (metric_type, ts)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS weather_timeseries (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            site_id VARCHAR(64) NOT NULL,
            ts DATETIME NOT NULL,
            temperature_c DOUBLE NOT NULL,
            wind_speed DOUBLE NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY uq_weather_site_ts (site_id, ts),
            KEY idx_weather_site_ts (site_id, ts)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS peer_energy_daily (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            metric_type VARCHAR(32) NOT NULL,
            building_id VARCHAR(128) NOT NULL,
            day DATE NOT NULL,
            total_value DOUBLE NOT NULL,
            sample_count SMALLINT NOT NULL,
            avg_value DOUBLE NOT NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY uq_peer_energy_daily_metric_building_day (metric_type, building_id, day),
            KEY idx_peer_energy_daily_building_day (building_id, day),
            KEY idx_peer_energy_daily_metric_day (metric_type, day)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS anomaly_actions (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            anomaly_id BIGINT NOT NULL,
            action_name VARCHAR(32) NOT NULL,
            status_before VARCHAR(32) NULL,
            status_after VARCHAR(32) NOT NULL,
            assignee VARCHAR(128) NULL,
            note TEXT NULL,
            created_at DATETIME NOT NULL,
            KEY idx_anomaly_actions_anomaly_time (anomaly_id, created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS anomaly_notes (
            id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            anomaly_id BIGINT NOT NULL,
            cause_confirmed TEXT NOT NULL,
            action_taken TEXT NOT NULL,
            result_summary TEXT NOT NULL,
            recurrence_risk VARCHAR(16) NOT NULL,
            reviewer VARCHAR(128) NULL,
            updated_at DATETIME NOT NULL,
            KEY idx_anomaly_notes_anomaly_time (anomaly_id, updated_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS ai_calls (
            trace_id VARCHAR(64) NOT NULL PRIMARY KEY,
            event_time DATETIME NOT NULL,
            requested_provider VARCHAR(32) NULL,
            provider VARCHAR(64) NULL,
            scene VARCHAR(32) NULL,
            building_id VARCHAR(128) NULL,
            anomaly_id BIGINT NULL,
            has_message TINYINT(1) NOT NULL DEFAULT 0,
            result_risk_level VARCHAR(32) NULL,
            knowledge_source VARCHAR(32) NULL,
            retrieval_hit_count INT NULL,
            retrieval_error_type VARCHAR(64) NULL,
            fallback_used TINYINT(1) NOT NULL DEFAULT 0,
            field_complete TINYINT(1) NOT NULL DEFAULT 0,
            latency_ms INT NOT NULL DEFAULT 0,
            success TINYINT(1) NOT NULL DEFAULT 1,
            error_type VARCHAR(64) NULL,
            feedback_label VARCHAR(32) NULL,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            KEY idx_ai_calls_event_time (event_time),
            KEY idx_ai_calls_scene (scene),
            KEY idx_ai_calls_building (building_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
        """
        CREATE TABLE IF NOT EXISTS system_snapshots (
            snapshot_key VARCHAR(64) NOT NULL PRIMARY KEY,
            payload_json LONGTEXT NOT NULL,
            updated_at DATETIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """.strip(),
    ]


class MySQLClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        connect_timeout_sec: int = 10,
        read_timeout_sec: int = 60,
        write_timeout_sec: int = 60,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.connect_timeout_sec = int(connect_timeout_sec)
        self.read_timeout_sec = int(read_timeout_sec)
        self.write_timeout_sec = int(write_timeout_sec)

    @classmethod
    def from_env(cls) -> "MySQLClient":
        return cls(
            host=os.getenv("MYSQL_HOST", "127.0.0.1").strip() or "127.0.0.1",
            port=int(os.getenv("MYSQL_PORT", "3306") or "3306"),
            database=os.getenv("MYSQL_DATABASE", "a8").strip() or "a8",
            user=os.getenv("MYSQL_USER", "root").strip() or "root",
            password=os.getenv("MYSQL_PASSWORD", "root").strip() or "root",
            connect_timeout_sec=int(os.getenv("MYSQL_CONNECT_TIMEOUT_SEC", "10") or "10"),
            read_timeout_sec=int(os.getenv("MYSQL_READ_TIMEOUT_SEC", "60") or "60"),
            write_timeout_sec=int(os.getenv("MYSQL_WRITE_TIMEOUT_SEC", "60") or "60"),
        )

    @property
    def configured(self) -> bool:
        return bool(self.host and self.database and self.user)

    @property
    def available(self) -> bool:
        return True

    def _connection_kwargs(self, include_database: bool = True, autocommit: bool = True) -> dict[str, Any]:
        kwargs = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "charset": "utf8mb4",
            "cursorclass": DictCursor,
            "autocommit": autocommit,
            "connect_timeout": self.connect_timeout_sec,
            "read_timeout": self.read_timeout_sec,
            "write_timeout": self.write_timeout_sec,
        }
        if include_database and self.database:
            kwargs["database"] = self.database
        return kwargs

    @contextmanager
    def connect(self, include_database: bool = True, autocommit: bool = True):
        conn = None
        try:
            conn = pymysql.connect(**self._connection_kwargs(include_database=include_database, autocommit=autocommit))
            yield conn
        except pymysql.MySQLError as exc:
            raise MySQLClientError(str(exc)) from exc
        finally:
            if conn is not None:
                conn.close()

    def execute(
        self,
        sql: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
        include_database: bool = True,
        autocommit: bool = True,
        timeout_sec: int | None = None,
    ) -> int:
        with self.connect(include_database=include_database, autocommit=autocommit) as conn:
            with conn.cursor() as cursor:
                count = cursor.execute(sql, params)
            if not autocommit:
                conn.commit()
            return int(count or 0)

    def execute_many(
        self,
        sql: str,
        params_seq: Iterable[Sequence[Any] | dict[str, Any]],
        include_database: bool = True,
        autocommit: bool = True,
        timeout_sec: int | None = None,
    ) -> int:
        params_list = list(params_seq)
        if not params_list:
            return 0
        with self.connect(include_database=include_database, autocommit=autocommit) as conn:
            with conn.cursor() as cursor:
                count = cursor.executemany(sql, params_list)
            if not autocommit:
                conn.commit()
            return int(count or 0)

    def query_rows(
        self,
        sql: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
        include_database: bool = True,
        timeout_sec: int | None = None,
    ) -> list[dict[str, Any]]:
        with self.connect(include_database=include_database, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def query_json_rows(
        self,
        sql: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
        include_database: bool = True,
        timeout_sec: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.query_rows(sql, params=params, include_database=include_database, timeout_sec=timeout_sec)
        parsed: list[dict[str, Any]] = []
        for row in rows:
            if not row:
                continue
            if len(row) == 1:
                value = next(iter(row.values()))
                if value is None:
                    continue
                if isinstance(value, (bytes, bytearray)):
                    value = value.decode("utf-8", errors="ignore")
                if isinstance(value, dict):
                    parsed.append(value)
                    continue
                if not isinstance(value, str):
                    raise MySQLClientError("invalid mysql json row payload type")
                try:
                    parsed.append(json.loads(value))
                except json.JSONDecodeError as exc:
                    raise MySQLClientError(f"invalid mysql json row: {value[:120]}") from exc
                continue
            parsed.append(row)
        return parsed

    def query_scalar(
        self,
        sql: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
        include_database: bool = True,
        timeout_sec: int | None = None,
    ) -> Any:
        rows = self.query_rows(sql, params=params, include_database=include_database, timeout_sec=timeout_sec)
        if not rows:
            return ""
        row = rows[0]
        if not row:
            return ""
        return next(iter(row.values()))

    def ensure_database(self) -> None:
        sql = f"CREATE DATABASE IF NOT EXISTS `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        self.execute(sql, include_database=False)

    def ensure_schema(self) -> None:
        self.ensure_database()
        for statement in mysql_schema_statements():
            self.execute(statement, include_database=True)

    def health(self) -> dict[str, Any]:
        connected = False
        error = ""
        if self.configured:
            try:
                self.query_scalar("SELECT 1", include_database=False)
                connected = True
            except Exception as exc:
                error = str(exc)
        else:
            error = "mysql env not configured"
        return {
            "configured": self.configured,
            "available": self.available,
            "connected": connected,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "error": error,
        }
