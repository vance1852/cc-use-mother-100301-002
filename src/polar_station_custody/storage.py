"""样品监管链的 SQLite 表结构，叠加在基础服务之上。"""

from __future__ import annotations

from pathlib import Path

from polar_station_foundation.storage import Database


CUSTODY_SCHEMA = """
CREATE TABLE IF NOT EXISTS sampling_plans (
    plan_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    title TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    unit TEXT NOT NULL,
    max_temperature_c REAL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS containers (
    container_id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL REFERENCES sampling_plans(plan_id),
    kind TEXT NOT NULL CHECK(kind IN ('root','aliquot','pool','analysis')),
    label TEXT NOT NULL,
    quantity REAL NOT NULL CHECK(quantity >= 0),
    unit TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active','lent','exhausted','consumed','merged')),
    custodian_id TEXT NOT NULL REFERENCES actors(actor_id),
    location TEXT NOT NULL,
    seal_id TEXT,
    due_back_at TEXT,
    result_id TEXT UNIQUE,
    head_event_id TEXT NOT NULL,
    state_version INTEGER NOT NULL CHECK(state_version >= 1),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS container_parents (
    container_id TEXT NOT NULL REFERENCES containers(container_id),
    parent_id TEXT NOT NULL REFERENCES containers(container_id),
    event_id TEXT NOT NULL,
    PRIMARY KEY(container_id, parent_id)
);
CREATE TABLE IF NOT EXISTS field_devices (
    device_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    label TEXT NOT NULL,
    registered_by TEXT NOT NULL REFERENCES actors(actor_id),
    registered_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reference_docs (
    doc_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    category TEXT NOT NULL CHECK(category IN ('weighing_scale','seal','personnel','calibration','environment')),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    content_json TEXT NOT NULL,
    registered_by TEXT NOT NULL REFERENCES actors(actor_id),
    registered_at TEXT NOT NULL,
    PRIMARY KEY(doc_id, version)
);
CREATE TABLE IF NOT EXISTS custody_events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    device_id TEXT,
    local_sequence INTEGER,
    event_type TEXT NOT NULL,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    subject_container_id TEXT,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    submitted_by TEXT NOT NULL REFERENCES actors(actor_id),
    status TEXT NOT NULL CHECK(status IN ('confirmed','quarantined')),
    temperature_c REAL,
    UNIQUE(device_id, local_sequence)
);
CREATE INDEX IF NOT EXISTS idx_custody_events_subject ON custody_events(subject_container_id);
CREATE TABLE IF NOT EXISTS event_effects (
    event_id TEXT NOT NULL REFERENCES custody_events(event_id),
    container_id TEXT NOT NULL REFERENCES containers(container_id),
    role TEXT NOT NULL CHECK(role IN ('subject','parent','child','source','target')),
    quantity_before REAL,
    quantity_after REAL,
    container_version INTEGER NOT NULL CHECK(container_version >= 1),
    PRIMARY KEY(event_id, container_id, role)
);
CREATE INDEX IF NOT EXISTS idx_event_effects_container ON event_effects(container_id);
CREATE TABLE IF NOT EXISTS quarantine_cases (
    case_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES custody_events(event_id),
    reason TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','accepted','rejected')),
    decided_by TEXT REFERENCES actors(actor_id),
    decided_at TEXT,
    decision_note TEXT
);
"""


class CustodyDatabase(Database):
    """在基础服务表结构之上追加样品监管链表。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        super().__init__(path)
        self.connection.executescript(CUSTODY_SCHEMA)
