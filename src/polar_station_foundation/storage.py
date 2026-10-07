"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sampling_plans (
    plan_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS containers (
    container_id TEXT PRIMARY KEY,
    plan_id TEXT REFERENCES sampling_plans(plan_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    holder_actor_id TEXT REFERENCES actors(actor_id),
    holder_location TEXT,
    seal_id TEXT,
    sealed_by TEXT,
    mass_grams REAL,
    version INTEGER NOT NULL CHECK(version >= 1),
    temp_excursion INTEGER NOT NULL DEFAULT 0 CHECK(temp_excursion IN (0, 1)),
    seal_anomaly INTEGER NOT NULL DEFAULT 0 CHECK(seal_anomaly IN (0, 1)),
    last_device_id TEXT,
    source_event_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS container_parents (
    container_id TEXT NOT NULL REFERENCES containers(container_id),
    parent_id TEXT NOT NULL REFERENCES containers(container_id),
    event_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY (container_id, parent_id)
);
CREATE TABLE IF NOT EXISTS custody_events (
    event_id TEXT PRIMARY KEY,
    device_id TEXT,
    local_sequence INTEGER,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    expected_version TEXT,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    plan_id TEXT,
    analysis_id TEXT,
    adjudication_json TEXT,
    UNIQUE(device_id, local_sequence)
);
CREATE TABLE IF NOT EXISTS custody_event_containers (
    event_id TEXT NOT NULL,
    container_id TEXT NOT NULL,
    role TEXT NOT NULL,
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (event_id, container_id, role)
);
CREATE TABLE IF NOT EXISTS analyses (
    analysis_id TEXT PRIMARY KEY,
    container_id TEXT NOT NULL REFERENCES containers(container_id),
    method TEXT NOT NULL,
    result_json TEXT NOT NULL,
    result_hash TEXT NOT NULL,
    instrument_json TEXT,
    event_id TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    last_seen_at TEXT,
    last_sequence INTEGER,
    last_occurred_at TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_event_receipts (
    device_id TEXT NOT NULL,
    local_sequence INTEGER NOT NULL,
    request_id TEXT,
    payload_hash TEXT NOT NULL,
    outcome TEXT NOT NULL,
    event_id TEXT,
    case_id TEXT,
    response_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (device_id, local_sequence)
);
CREATE TABLE IF NOT EXISTS quarantine_cases (
    case_id TEXT PRIMARY KEY,
    device_id TEXT,
    local_sequence INTEGER,
    request_id TEXT,
    event_type TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    actor_id TEXT,
    occurred_at TEXT,
    reason_code TEXT NOT NULL,
    reason_detail TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending', 'accepted', 'rejected')),
    created_event_id TEXT,
    decided_by TEXT REFERENCES actors(actor_id),
    decided_at TEXT,
    decision_note TEXT,
    applied_event_id TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quarantine_device_sequence
    ON quarantine_cases(device_id, local_sequence);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
