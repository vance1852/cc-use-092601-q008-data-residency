"""供应服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS supply_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor','compliance','tenant')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS market_index_quotes (
    quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
    market_index TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    close_cny TEXT NOT NULL,
    source_revision TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    supersedes_quote_id INTEGER REFERENCES market_index_quotes(quote_id),
    recorded_by TEXT NOT NULL REFERENCES supply_users(user_id),
    recorded_at TEXT NOT NULL,
    UNIQUE(market_index, trade_date, source_revision)
);

CREATE INDEX IF NOT EXISTS idx_quotes_series
ON market_index_quotes(market_index, trade_date, quote_id);

CREATE TABLE IF NOT EXISTS facilities (
    facility_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    timezone TEXT NOT NULL,
    capacity_gpu_hours TEXT NOT NULL,
    region TEXT,
    network_zone TEXT,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS routes (
    route_id TEXT PRIMARY KEY,
    origin_id TEXT NOT NULL REFERENCES facilities(facility_id),
    destination_id TEXT NOT NULL REFERENCES facilities(facility_id),
    product TEXT NOT NULL,
    daily_capacity TEXT NOT NULL,
    loss_basis_points INTEGER NOT NULL,
    transit_hours INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN ('active','suspended','retired')),
    created_at TEXT NOT NULL,
    CHECK(origin_id <> destination_id)
);

CREATE TABLE IF NOT EXISTS route_outages (
    outage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_id TEXT NOT NULL REFERENCES routes(route_id),
    starts_at TEXT NOT NULL,
    ends_at TEXT,
    capacity_percent TEXT NOT NULL,
    reason TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'announced' CHECK(state IN ('announced','active','closed','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_outages_route_time
ON route_outages(route_id, starts_at, ends_at);

CREATE TABLE IF NOT EXISTS inventory_lots (
    lot_id TEXT PRIMARY KEY,
    facility_id TEXT NOT NULL REFERENCES facilities(facility_id),
    product TEXT NOT NULL,
    grade TEXT NOT NULL,
    quantity_gpu_hours TEXT NOT NULL,
    available_gpu_hours TEXT NOT NULL,
    unit_cost_cny TEXT NOT NULL,
    received_at TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_inventory_available
ON inventory_lots(facility_id, product, received_at);

CREATE TABLE IF NOT EXISTS inventory_adjustments (
    adjustment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    lot_id TEXT NOT NULL REFERENCES inventory_lots(lot_id),
    delta_gpu_hours TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    note TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nominations (
    nomination_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL REFERENCES routes(route_id),
    shipper_id TEXT NOT NULL,
    service_date TEXT NOT NULL,
    requested_gpu_hours TEXT NOT NULL,
    allocated_gpu_hours TEXT NOT NULL DEFAULT '0',
    delivered_gpu_hours TEXT NOT NULL DEFAULT '0',
    priority INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'submitted'
        CHECK(state IN ('submitted','allocated','in_transit','delivered','cancelled')),
    revision INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL UNIQUE,
    submitted_by TEXT NOT NULL REFERENCES supply_users(user_id),
    submitted_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_nominations_schedule
ON nominations(route_id, service_date, priority, submitted_at);

CREATE TABLE IF NOT EXISTS allocation_runs (
    allocation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_id TEXT NOT NULL REFERENCES routes(route_id),
    service_date TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    available_capacity TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(route_id, service_date, input_sha256)
);

CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    nomination_id TEXT NOT NULL UNIQUE REFERENCES nominations(nomination_id),
    inventory_lot_id TEXT NOT NULL REFERENCES inventory_lots(lot_id),
    loaded_gpu_hours TEXT NOT NULL,
    expected_delivered_gpu_hours TEXT NOT NULL,
    departed_at TEXT NOT NULL,
    arrived_at TEXT,
    state TEXT NOT NULL DEFAULT 'in_transit' CHECK(state IN ('in_transit','delivered','disputed')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS supply_scenarios (
    scenario_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'draft' CHECK(state IN ('draft','approved','retired')),
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scenario_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scenario_id TEXT NOT NULL REFERENCES supply_scenarios(scenario_id),
    as_of_date TEXT NOT NULL,
    input_sha256 TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(scenario_id, as_of_date, input_sha256)
);

CREATE TABLE IF NOT EXISTS supply_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS supply_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_supply_audit_entity
ON supply_audit_events(entity_type, entity_id, event_id);

-- 数据集驻留合规与调度联动
CREATE TABLE IF NOT EXISTS dataset_versions (
    dataset_id TEXT NOT NULL,
    version TEXT NOT NULL,
    name TEXT NOT NULL,
    regions_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    registered_by TEXT NOT NULL REFERENCES supply_users(user_id),
    registered_at TEXT NOT NULL,
    PRIMARY KEY(dataset_id, version)
);

CREATE TABLE IF NOT EXISTS dataset_authorizations (
    authorization_id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id TEXT NOT NULL,
    version TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    granted_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'granted' CHECK(state IN ('granted','withdrawn')),
    withdrawn_at TEXT,
    withdraw_reason TEXT,
    batch_id TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL,
    UNIQUE(dataset_id, version, subject_id),
    FOREIGN KEY(dataset_id, version) REFERENCES dataset_versions(dataset_id, version)
);

CREATE INDEX IF NOT EXISTS idx_authorizations_lookup
ON dataset_authorizations(dataset_id, version, subject_id, state);

CREATE TABLE IF NOT EXISTS authorization_batches (
    batch_id TEXT PRIMARY KEY,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES supply_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS training_plans (
    plan_id TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    version TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    product TEXT NOT NULL,
    requested_gpu_hours TEXT NOT NULL,
    network_zone TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'blocked'
        CHECK(state IN ('ready','blocked','launching','running','blocked_running','halted','cancelled')),
    blocked_reason TEXT,
    candidate_sites_json TEXT NOT NULL,
    selected_site TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(dataset_id, version) REFERENCES dataset_versions(dataset_id, version)
);

CREATE INDEX IF NOT EXISTS idx_training_plans_subject
ON training_plans(subject_id, state, created_at);

CREATE TABLE IF NOT EXISTS compliance_access_records (
    access_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL,
    dataset_id TEXT NOT NULL,
    version TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    site_id TEXT,
    event_type TEXT NOT NULL CHECK(event_type IN ('plan.submitted','plan.launching','plan.ran','authorization.withdrawn_block','authorization.withdrawn_manual','plan.manual_disposition','plan.halted')),
    detail_json TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_access_records_dataset
ON compliance_access_records(dataset_id, version, subject_id, access_id);
"""


def _column_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate_facility_boundaries(connection: sqlite3.Connection) -> None:
    """既有库可能缺少地域与网络边界列，按列补齐，不重建表。"""
    existing = _column_names(connection, "facilities")
    if "region" not in existing:
        connection.execute("ALTER TABLE facilities ADD COLUMN region TEXT")
    if "network_zone" not in existing:
        connection.execute("ALTER TABLE facilities ADD COLUMN network_zone TEXT")


def _migrate_user_roles(connection: sqlite3.Connection) -> None:
    """旧库的 supply_users 角色约束缺少 compliance/tenant，检测后安全重建。"""
    sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='supply_users'"
    ).fetchone()
    if sql is not None and "'compliance'" in sql["sql"]:
        return
    foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()[0]
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA legacy_alter_table=ON")
    try:
        connection.executescript(
            """
            ALTER TABLE supply_users RENAME TO supply_users_legacy;
            CREATE TABLE supply_users (
                user_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor','compliance','tenant')),
                active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                created_at TEXT NOT NULL
            );
            INSERT INTO supply_users(user_id,display_name,role,active,created_at)
            SELECT user_id,display_name,role,active,created_at FROM supply_users_legacy;
            DROP TABLE supply_users_legacy;
            """
        )
    finally:
        connection.execute("PRAGMA legacy_alter_table=OFF")
        connection.execute(f"PRAGMA foreign_keys={'ON' if foreign_keys else 'OFF'}")


def connect(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)
    _migrate_facility_boundaries(connection)
    _migrate_user_roles(connection)


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)
