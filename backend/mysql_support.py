from __future__ import annotations

import json
import os
import shutil
import subprocess
from typing import Any


MYSQL_DEFAULT_BIN = r"C:\Program Files\MySQL\MySQL Server 8.0\bin\mysql.exe"


class MySQLCLIError(RuntimeError):
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


class MySQLCLIClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        mysql_bin: str | None = None,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.mysql_bin = mysql_bin or shutil.which("mysql") or MYSQL_DEFAULT_BIN

    @classmethod
    def from_env(cls) -> "MySQLCLIClient":
        return cls(
            host=os.getenv("MYSQL_HOST", "127.0.0.1").strip() or "127.0.0.1",
            port=int(os.getenv("MYSQL_PORT", "3306") or "3306"),
            database=os.getenv("MYSQL_DATABASE", "a8").strip() or "a8",
            user=os.getenv("MYSQL_USER", "root").strip() or "root",
            password=os.getenv("MYSQL_PASSWORD", "").strip(),
            mysql_bin=os.getenv("MYSQL_BIN", "").strip() or None,
        )

    @property
    def configured(self) -> bool:
        return bool(self.host and self.database and self.user)

    @property
    def available(self) -> bool:
        return bool(self.mysql_bin) and os.path.exists(self.mysql_bin)

    def _command(self, include_database: bool = True) -> list[str]:
        command = [
            self.mysql_bin,
            f"--host={self.host}",
            f"--port={self.port}",
            f"--user={self.user}",
            "--default-character-set=utf8mb4",
            "--batch",
            "--raw",
            "--silent",
        ]
        if include_database and self.database:
            command.append(self.database)
        return command

    def _run(self, sql: str, include_database: bool = True, timeout_sec: int = 60) -> str:
        if not self.available:
            raise MySQLCLIError("mysql client not found")
        command = self._command(include_database=include_database)
        command.extend(["--execute", sql])
        env = os.environ.copy()
        if self.password:
            env["MYSQL_PWD"] = self.password
        try:
            proc = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=timeout_sec,
                env=env,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or exc.stdout or "").strip()
            raise MySQLCLIError(stderr or "mysql command failed") from exc
        except subprocess.TimeoutExpired as exc:
            raise MySQLCLIError("mysql command timeout") from exc
        return proc.stdout or ""

    def execute(self, sql: str, include_database: bool = True, timeout_sec: int = 60) -> None:
        self._run(sql, include_database=include_database, timeout_sec=timeout_sec)

    def query_json_rows(self, sql: str, include_database: bool = True, timeout_sec: int = 60) -> list[dict[str, Any]]:
        output = self._run(sql, include_database=include_database, timeout_sec=timeout_sec)
        rows: list[dict[str, Any]] = []
        for line in output.splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                rows.append(json.loads(text))
            except json.JSONDecodeError as exc:
                raise MySQLCLIError(f"invalid mysql json row: {text[:120]}") from exc
        return rows

    def query_scalar(self, sql: str, include_database: bool = True, timeout_sec: int = 60) -> str:
        output = self._run(sql, include_database=include_database, timeout_sec=timeout_sec)
        for line in output.splitlines():
            text = line.strip()
            if text:
                return text
        return ""

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
        if self.configured and self.available:
            try:
                self.query_scalar("SELECT 1", include_database=False, timeout_sec=10)
                connected = True
            except Exception as exc:
                error = str(exc)
        elif not self.available:
            error = "mysql client not found"
        elif not self.configured:
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
