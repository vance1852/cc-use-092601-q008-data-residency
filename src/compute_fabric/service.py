"""算力单价、算力库存、互联通道和提名的事务用例。"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .clock import SystemClock, parse_utc, utc_text
from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import (
    AuthorizationGrant,
    DatasetVersion,
    IndexQuote,
    Facility,
    InventoryLot,
    NominationRequest,
    Route,
    SupplyScenario,
    TrainingPlanRequest,
    identifier,
)
from .compliance import SiteContext, blocked_reason_code, evaluate_site
from .planning import (
    AllocationRequest,
    PricePoint,
    allocate_capacity,
    canonical_json,
    decimal_text,
    delivered_after_loss,
    digest,
    effective_capacity,
    latest_streak,
    moving_average,
    quantize_volume,
    scenario_projection,
    weighted_inventory_cost,
)
from .storage import initialize, transaction


ROLE_PERMISSIONS = {
    "planner": {"quote.write", "catalog.write", "scenario.write", "scenario.run"},
    "dispatcher": {
        "nomination.write",
        "allocation.run",
        "transfer.write",
        "inventory.write",
        "plan.read",
        "plan.launch",
    },
    "risk": {"outage.write", "scenario.approve", "report.read"},
    "auditor": {"report.read", "audit.read", "compliance.read"},
    "compliance": {"dataset.write", "authorization.write", "authorization.revoke", "compliance.read", "plan.read"},
    "tenant": {"plan.write", "plan.read.self"},
}


class SupplyService:
    def __init__(self, connection: sqlite3.Connection, clock=None) -> None:
        self.connection = connection
        self.clock = clock or SystemClock()
        initialize(connection)

    def _now(self) -> str:
        return utc_text(self.clock.now())

    def _user(self, user_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM supply_users WHERE user_id=?", (user_id,)
        ).fetchone()
        if row is None:
            raise NotFound("用户不存在")
        if not row["active"]:
            raise Forbidden("用户已停用")
        return row

    def _require(self, user_id: str, permission: str) -> sqlite3.Row:
        user = self._user(user_id)
        if permission not in ROLE_PERMISSIONS[user["role"]]:
            raise Forbidden(f"角色 {user['role']} 无权执行 {permission}")
        return user

    def _audit(
        self,
        entity_type: str,
        entity_id: str,
        event_type: str,
        actor_id: str,
        payload: Mapping[str, Any],
    ) -> None:
        previous = self.connection.execute(
            "SELECT event_hash FROM supply_audit_events ORDER BY event_id DESC LIMIT 1"
        ).fetchone()
        previous_hash = "0" * 64 if previous is None else previous["event_hash"]
        body = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "payload": payload,
            "created_at": self._now(),
            "previous_hash": previous_hash,
        }
        event_hash = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
        self.connection.execute(
            "INSERT INTO supply_audit_events(entity_type,entity_id,event_type,actor_id,payload_json,"
            "previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                entity_type,
                entity_id,
                event_type,
                actor_id,
                canonical_json(payload),
                previous_hash,
                event_hash,
                body["created_at"],
            ),
        )

    def create_user(self, user_id: str, display_name: str, role: str) -> dict[str, Any]:
        if role not in ROLE_PERMISSIONS:
            raise ValidationFailed("未知角色")
        if not user_id.strip() or not display_name.strip():
            raise ValidationFailed("用户编号和名称不能为空")
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO supply_users(user_id,display_name,role,created_at) VALUES(?,?,?,?)",
                    (user_id.strip(), display_name.strip(), role, self._now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("用户已经存在") from exc
        return {"user_id": user_id.strip(), "role": role}

    def record_quote(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "quote.write")
        quote = IndexQuote.from_dict(raw)
        previous = self.connection.execute(
            "SELECT quote_id,source_revision FROM market_index_quotes WHERE market_index=? AND trade_date=? "
            "ORDER BY quote_id DESC LIMIT 1",
            (quote.market_index, quote.trade_date),
        ).fetchone()
        if previous is not None and previous["source_revision"] == quote.source_revision:
            raise Conflict("同一来源修订已登记")
        try:
            with transaction(self.connection, immediate=True):
                cursor = self.connection.execute(
                    "INSERT INTO market_index_quotes(market_index,trade_date,close_cny,source_revision,observed_at,"
                    "supersedes_quote_id,recorded_by,recorded_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        quote.market_index,
                        quote.trade_date,
                        decimal_text(quote.close_cny),
                        quote.source_revision,
                        quote.observed_at,
                        None if previous is None else previous["quote_id"],
                        actor_id,
                        self._now(),
                    ),
                )
                quote_id = int(cursor.lastrowid)
                self._audit(
                    "quote",
                    str(quote_id),
                    "quote.recorded",
                    actor_id,
                    {"market_index": quote.market_index, "trade_date": quote.trade_date},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("算力单价版本冲突") from exc
        return {"quote_id": quote_id, "market_index": quote.market_index, "trade_date": quote.trade_date}

    def price_summary(self, market_index: str, sessions: int = 20) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT q.trade_date,q.close_cny FROM market_index_quotes q "
            "JOIN (SELECT trade_date,max(quote_id) quote_id FROM market_index_quotes "
            "WHERE market_index=? GROUP BY trade_date) latest ON latest.quote_id=q.quote_id "
            "ORDER BY q.trade_date DESC LIMIT ?",
            (market_index.upper(), sessions),
        ).fetchall()
        points = [PricePoint(row["trade_date"], Decimal(row["close_cny"])) for row in rows]
        if not points:
            raise NotFound("没有基准算力单价")
        streak = latest_streak(points)
        average = moving_average(points, min(5, len(points)))
        latest = max(points, key=lambda item: item.trade_date)
        return {
            "market_index": market_index.upper(),
            "latest": {"trade_date": latest.trade_date, "close_cny": decimal_text(latest.close)},
            "latest_streak": None if streak is None else streak.as_dict(),
            "moving_average": None if average is None else decimal_text(average),
            "observations": len(points),
        }

    def create_facility(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        facility = Facility.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO facilities(facility_id,name,kind,timezone,capacity_gpu_hours,"
                    "region,network_zone,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        facility.facility_id,
                        facility.name,
                        facility.kind,
                        facility.timezone,
                        decimal_text(facility.capacity_gpu_hours),
                        facility.region,
                        facility.network_zone,
                        self._now(),
                    ),
                )
                self._audit("facility", facility.facility_id, "facility.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("设施编号已经存在") from exc
        return self.facility(facility.facility_id)

    def facility(self, facility_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM facilities WHERE facility_id=?", (facility_id,)).fetchone()
        if row is None:
            raise NotFound("设施不存在")
        return dict(row)

    def create_route(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "catalog.write")
        route = Route.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO routes(route_id,origin_id,destination_id,product,daily_capacity,"
                    "loss_basis_points,transit_hours,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (
                        route.route_id,
                        route.origin_id,
                        route.destination_id,
                        route.product,
                        decimal_text(route.daily_capacity),
                        route.loss_basis_points,
                        route.transit_hours,
                        self._now(),
                    ),
                )
                self._audit("route", route.route_id, "route.created", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("互联通道编号冲突或设施不存在") from exc
        return self.route(route.route_id)

    def route(self, route_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM routes WHERE route_id=?", (route_id,)).fetchone()
        if row is None:
            raise NotFound("互联通道不存在")
        return dict(row)

    def announce_outage(
        self,
        actor_id: str,
        route_id: str,
        starts_at: str,
        ends_at: str | None,
        capacity_percent: object,
        reason: str,
    ) -> dict[str, Any]:
        self._require(actor_id, "outage.write")
        self.route(route_id)
        try:
            start = parse_utc(starts_at, "starts_at")
            end = None if ends_at is None else parse_utc(ends_at, "ends_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        if end is not None and end <= start:
            raise ValidationFailed("ends_at 必须晚于 starts_at")
        percentage = Decimal(str(capacity_percent))
        if percentage < 0 or percentage > 100:
            raise ValidationFailed("capacity_percent 必须在 0 到 100 之间")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO route_outages(route_id,starts_at,ends_at,capacity_percent,reason,created_by,created_at) "
                "VALUES(?,?,?,?,?,?,?)",
                (route_id, utc_text(start), None if end is None else utc_text(end), decimal_text(percentage), reason, actor_id, self._now()),
            )
            outage_id = int(cursor.lastrowid)
            self._audit("route", route_id, "outage.announced", actor_id, {"outage_id": outage_id})
        return {"outage_id": outage_id, "route_id": route_id, "state": "announced"}

    def add_inventory_lot(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "inventory.write")
        lot = InventoryLot.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO inventory_lots(lot_id,facility_id,product,grade,quantity_gpu_hours,available_gpu_hours,"
                    "unit_cost_cny,received_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        lot.lot_id,
                        lot.facility_id,
                        lot.product,
                        lot.grade,
                        decimal_text(lot.quantity_gpu_hours),
                        decimal_text(lot.quantity_gpu_hours),
                        decimal_text(lot.unit_cost_cny),
                        lot.received_at,
                        actor_id,
                        self._now(),
                    ),
                )
                self._audit("inventory_lot", lot.lot_id, "inventory.received", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("加速卡资源批次冲突或设施不存在") from exc
        return self.inventory_lot(lot.lot_id)

    def inventory_lot(self, lot_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM inventory_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if row is None:
            raise NotFound("加速卡资源批次不存在")
        return dict(row)

    def inventory_summary(self, facility_id: str, product: str) -> dict[str, Any]:
        rows = self.connection.execute(
            "SELECT * FROM inventory_lots WHERE facility_id=? AND product=? ORDER BY received_at,lot_id",
            (facility_id, product),
        ).fetchall()
        return {"facility_id": facility_id, "product": product, **weighted_inventory_cost(rows)}

    def submit_nomination(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "nomination.write")
        nomination = NominationRequest.from_dict(raw)
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency WHERE scope='nomination' AND idempotency_key=?",
            (nomination.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同提名内容")
            return json.loads(stored["response_json"])
        route = self.route(nomination.route_id)
        if route["state"] != "active":
            raise InvalidState("互联通道当前不可提名")
        response = {
            "nomination_id": nomination.nomination_id,
            "route_id": nomination.route_id,
            "state": "submitted",
            "revision": 1,
        }
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO nominations(nomination_id,route_id,shipper_id,service_date,requested_gpu_hours,"
                    "priority,idempotency_key,submitted_by,submitted_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        nomination.nomination_id,
                        nomination.route_id,
                        nomination.shipper_id,
                        nomination.service_date,
                        decimal_text(nomination.requested_gpu_hours),
                        nomination.priority,
                        nomination.idempotency_key,
                        actor_id,
                        self._now(),
                    ),
                )
                self.connection.execute(
                    "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('nomination',?,?,?,?)",
                    (nomination.idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                self._audit("nomination", nomination.nomination_id, "nomination.submitted", actor_id, raw)
        except sqlite3.IntegrityError as exc:
            raise Conflict("提名编号或幂等键冲突") from exc
        return response

    def _capacity_for_date(self, route: sqlite3.Row, service_date: str) -> Decimal:
        start = service_date + "T00:00:00Z"
        end = service_date + "T23:59:59Z"
        rows = self.connection.execute(
            "SELECT capacity_percent FROM route_outages WHERE route_id=? AND state IN ('announced','active') "
            "AND starts_at<=? AND (ends_at IS NULL OR ends_at>=?) ORDER BY outage_id",
            (route["route_id"], end, start),
        ).fetchall()
        percentages = [Decimal(row["capacity_percent"]) for row in rows]
        return effective_capacity(Decimal(route["daily_capacity"]), percentages)

    def allocate(self, actor_id: str, route_id: str, service_date: str) -> dict[str, Any]:
        self._require(actor_id, "allocation.run")
        route = self.connection.execute("SELECT * FROM routes WHERE route_id=?", (route_id,)).fetchone()
        if route is None:
            raise NotFound("互联通道不存在")
        nominations = self.connection.execute(
            "SELECT * FROM nominations WHERE route_id=? AND service_date=? AND state='submitted' "
            "ORDER BY priority,submitted_at,nomination_id",
            (route_id, service_date),
        ).fetchall()
        if not nominations:
            raise InvalidState("没有待分配提名")
        requests = [
            AllocationRequest(
                row["nomination_id"],
                Decimal(row["requested_gpu_hours"]),
                int(row["priority"]),
                row["submitted_at"],
            )
            for row in nominations
        ]
        available = self._capacity_for_date(route, service_date)
        input_value = [dict(row) for row in nominations]
        input_sha256 = digest({"route": dict(route), "nominations": input_value, "capacity": str(available)})
        result_rows = allocate_capacity(available, requests)
        result = {
            "route_id": route_id,
            "service_date": service_date,
            "available_capacity": decimal_text(available),
            "allocations": result_rows,
        }
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO allocation_runs(route_id,service_date,input_sha256,available_capacity,result_json,"
                "created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (route_id, service_date, input_sha256, decimal_text(available), canonical_json(result), actor_id, self._now()),
            )
            for item in result_rows:
                state = "allocated" if Decimal(item["allocated_gpu_hours"]) > 0 else "cancelled"
                self.connection.execute(
                    "UPDATE nominations SET allocated_gpu_hours=?,state=?,revision=revision+1 "
                    "WHERE nomination_id=? AND state='submitted'",
                    (item["allocated_gpu_hours"], state, item["nomination_id"]),
                )
            allocation_id = int(cursor.lastrowid)
            self._audit("route", route_id, "allocation.completed", actor_id, {"allocation_id": allocation_id})
        return {"allocation_id": allocation_id, **result}

    def dispatch_transfer(
        self,
        actor_id: str,
        transfer_id: str,
        nomination_id: str,
        lot_id: str,
        expected_revision: int,
    ) -> dict[str, Any]:
        self._require(actor_id, "transfer.write")
        nomination = self.connection.execute(
            "SELECT n.*,r.loss_basis_points,r.transit_hours,r.origin_id FROM nominations n "
            "JOIN routes r ON r.route_id=n.route_id WHERE n.nomination_id=?",
            (nomination_id,),
        ).fetchone()
        if nomination is None:
            raise NotFound("提名不存在")
        if nomination["state"] != "allocated" or nomination["revision"] != expected_revision:
            raise InvalidState("提名不是当前可交付版本")
        lot = self.connection.execute("SELECT * FROM inventory_lots WHERE lot_id=?", (lot_id,)).fetchone()
        if lot is None:
            raise NotFound("加速卡资源批次不存在")
        allocated = Decimal(nomination["allocated_gpu_hours"])
        available = Decimal(lot["available_gpu_hours"])
        if lot["facility_id"] != nomination["origin_id"] or lot["product"] != self.route(nomination["route_id"])["product"]:
            raise Conflict("加速卡资源批次与互联通道起点或资源类型不匹配")
        if available < allocated:
            raise Conflict("算力库存不足以完成分配")
        expected_delivery = delivered_after_loss(allocated, int(nomination["loss_basis_points"]))
        departed_at = self._now()
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE inventory_lots SET available_gpu_hours=?,revision=revision+1 WHERE lot_id=? AND revision=?",
                (decimal_text(quantize_volume(available - allocated)), lot_id, lot["revision"]),
            )
            self.connection.execute(
                "UPDATE nominations SET state='in_transit',revision=revision+1 WHERE nomination_id=? AND revision=?",
                (nomination_id, expected_revision),
            )
            self.connection.execute(
                "INSERT INTO transfers(transfer_id,nomination_id,inventory_lot_id,loaded_gpu_hours,"
                "expected_delivered_gpu_hours,departed_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
                (
                    transfer_id,
                    nomination_id,
                    lot_id,
                    decimal_text(allocated),
                    decimal_text(expected_delivery),
                    departed_at,
                    actor_id,
                    departed_at,
                ),
            )
            self._audit("transfer", transfer_id, "transfer.dispatched", actor_id, {"nomination_id": nomination_id})
        return {
            "transfer_id": transfer_id,
            "state": "in_transit",
            "loaded_gpu_hours": decimal_text(allocated),
            "expected_delivered_gpu_hours": decimal_text(expected_delivery),
            "expected_arrival": utc_text(parse_utc(departed_at) + timedelta(hours=int(nomination["transit_hours"]))),
        }

    def create_scenario(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "scenario.write")
        scenario = SupplyScenario.from_dict(raw)
        definition = canonical_json(raw)
        content_sha256 = hashlib.sha256(definition.encode("utf-8")).hexdigest()
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO supply_scenarios(scenario_id,name,definition_json,content_sha256,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (scenario.scenario_id, scenario.name, definition, content_sha256, actor_id, self._now()),
                )
                self._audit("scenario", scenario.scenario_id, "scenario.created", actor_id, {"sha256": content_sha256})
        except sqlite3.IntegrityError as exc:
            raise Conflict("情景编号或内容已经存在") from exc
        return {"scenario_id": scenario.scenario_id, "state": "draft", "sha256": content_sha256}

    def approve_scenario(self, actor_id: str, scenario_id: str, expected_revision: int) -> dict[str, Any]:
        self._require(actor_id, "scenario.approve")
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE supply_scenarios SET state='approved',revision=revision+1 "
                "WHERE scenario_id=? AND state='draft' AND revision=?",
                (scenario_id, expected_revision),
            )
            if cursor.rowcount != 1:
                raise InvalidState("情景不是当前草稿版本")
            self._audit("scenario", scenario_id, "scenario.approved", actor_id, {})
        return {"scenario_id": scenario_id, "state": "approved", "revision": expected_revision + 1}

    def run_scenario(self, actor_id: str, scenario_id: str, as_of_date: str) -> dict[str, Any]:
        self._require(actor_id, "scenario.run")
        row = self.connection.execute(
            "SELECT * FROM supply_scenarios WHERE scenario_id=?", (scenario_id,)
        ).fetchone()
        if row is None:
            raise NotFound("情景不存在")
        if row["state"] != "approved":
            raise InvalidState("只有已批准情景可以运行")
        scenario = SupplyScenario.from_dict(json.loads(row["definition_json"]))
        price_row = self.connection.execute(
            "SELECT close_cny FROM market_index_quotes WHERE trade_date<=? ORDER BY trade_date DESC,quote_id DESC LIMIT 1",
            (as_of_date,),
        ).fetchone()
        if price_row is None:
            raise InvalidState("截止日期没有可用算力单价")
        routes = self.connection.execute("SELECT * FROM routes WHERE state='active' ORDER BY route_id").fetchall()
        inventory = self.connection.execute(
            "SELECT facility_id,product,sum(CAST(available_gpu_hours AS REAL)) available_gpu_hours "
            "FROM inventory_lots GROUP BY facility_id,product ORDER BY facility_id,product"
        ).fetchall()
        input_value = {
            "scenario_sha256": row["content_sha256"],
            "as_of_date": as_of_date,
            "price": price_row["close_cny"],
            "routes": [dict(item) for item in routes],
            "inventory": [dict(item) for item in inventory],
        }
        input_sha256 = digest(input_value)
        existing = self.connection.execute(
            "SELECT run_id,result_json FROM scenario_runs WHERE scenario_id=? AND as_of_date=? AND input_sha256=?",
            (scenario_id, as_of_date, input_sha256),
        ).fetchone()
        if existing is not None:
            return {"run_id": existing["run_id"], **json.loads(existing["result_json"]), "replayed": True}
        result = scenario_projection(
            current_price=Decimal(price_row["close_cny"]),
            market_index_drop_percent=scenario.market_index_drop_percent,
            routes=routes,
            inventory=inventory,
            route_capacity_changes=scenario.route_capacity_changes,
            demand_changes=scenario.demand_changes,
        )
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "INSERT INTO scenario_runs(scenario_id,as_of_date,input_sha256,result_json,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (scenario_id, as_of_date, input_sha256, canonical_json(result), actor_id, self._now()),
            )
            run_id = int(cursor.lastrowid)
            self._audit("scenario", scenario_id, "scenario.executed", actor_id, {"run_id": run_id})
        return {"run_id": run_id, **result, "replayed": False}

    # ------------------------------------------------------------------
    # 数据集驻留合规与调度联动
    # ------------------------------------------------------------------

    def register_dataset(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "dataset.write")
        dataset = DatasetVersion.from_dict(raw)
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO dataset_versions(dataset_id,version,name,regions_json,content_sha256,"
                    "registered_by,registered_at) VALUES(?,?,?,?,?,?,?)",
                    (
                        dataset.dataset_id,
                        dataset.version,
                        dataset.name,
                        canonical_json(list(dataset.regions)),
                        dataset.content_sha256,
                        actor_id,
                        dataset.registered_at,
                    ),
                )
                self._audit(
                    "dataset",
                    f"{dataset.dataset_id}@{dataset.version}",
                    "dataset.registered",
                    actor_id,
                    {
                        "dataset_id": dataset.dataset_id,
                        "version": dataset.version,
                        "regions": list(dataset.regions),
                        "content_sha256": dataset.content_sha256,
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("数据集确定版本已经登记") from exc
        return self._dataset_view(dataset.dataset_id, dataset.version)

    def _dataset_row(self, dataset_id: str, version: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM dataset_versions WHERE dataset_id=? AND version=?",
            (dataset_id, version),
        ).fetchone()
        if row is None:
            raise NotFound("数据集版本不存在")
        return row

    def _dataset_view(self, dataset_id: str, version: str) -> dict[str, Any]:
        row = self._dataset_row(dataset_id, version)
        return {
            "dataset_id": row["dataset_id"],
            "version": row["version"],
            "name": row["name"],
            "regions": json.loads(row["regions_json"]),
            "content_sha256": row["content_sha256"],
            "registered_at": row["registered_at"],
        }

    def import_authorizations(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """批量导入主体授权，全有或全无；同一批次重复提交保持稳定结果。"""
        self._require(actor_id, "authorization.write")
        batch_id = identifier(raw.get("batch_id"), "batch_id")
        items_raw = raw.get("grants")
        if not isinstance(items_raw, list) or not items_raw:
            raise ValidationFailed("grants 必须是非空数组")
        grants = [AuthorizationGrant.from_dict(item) for item in items_raw]
        request_digest = digest({"batch_id": batch_id, "grants": [dict(item) for item in items_raw]})
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM authorization_batches WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("批次号对应不同授权导入内容")
            return json.loads(stored["response_json"])

        # 批次内同一 (数据集, 版本, 主体) 不允许重复，否则语义不确定。
        keys = [(g.dataset_id, g.version, g.subject_id) for g in grants]
        if len(set(keys)) != len(keys):
            raise ValidationFailed("批次内不能包含同一主体对同一数据集版本的重复授权")

        inserted_ids: list[int] = []
        try:
            with transaction(self.connection, immediate=True):
                # 预校验全部条目，任何一条不满足则整批回滚。
                for grant in grants:
                    self._dataset_row(grant.dataset_id, grant.version)
                    existing = self.connection.execute(
                        "SELECT state FROM dataset_authorizations WHERE dataset_id=? AND version=? AND subject_id=?",
                        (grant.dataset_id, grant.version, grant.subject_id),
                    ).fetchone()
                    if existing is not None:
                        raise Conflict(
                            f"主体 {grant.subject_id} 对 {grant.dataset_id}@{grant.version} 的授权已经存在"
                        )
                for grant in grants:
                    cursor = self.connection.execute(
                        "INSERT INTO dataset_authorizations(dataset_id,version,subject_id,granted_at,"
                        "expires_at,state,batch_id,created_by,created_at) VALUES(?,?,?,?,?,'granted',?,?,?)",
                        (
                            grant.dataset_id,
                            grant.version,
                            grant.subject_id,
                            grant.granted_at,
                            grant.expires_at,
                            batch_id,
                            actor_id,
                            self._now(),
                        ),
                    )
                    inserted_ids.append(int(cursor.lastrowid))
                response = {
                    "batch_id": batch_id,
                    "state": "imported",
                    "imported": len(grants),
                    "authorization_ids": inserted_ids,
                }
                self.connection.execute(
                    "INSERT INTO authorization_batches(batch_id,request_sha256,response_json,created_by,created_at) "
                    "VALUES(?,?,?,?,?)",
                    (batch_id, request_digest, canonical_json(response), actor_id, self._now()),
                )
                self._audit(
                    "authorization_batch",
                    batch_id,
                    "authorization.imported",
                    actor_id,
                    {"imported": len(grants), "request_sha256": request_digest},
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("授权批次冲突或引用了不存在的数据集版本") from exc
        return response

    def _effective_authorization(
        self, dataset_id: str, version: str, subject_id: str
    ) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM dataset_authorizations WHERE dataset_id=? AND version=? AND subject_id=?",
            (dataset_id, version, subject_id),
        ).fetchone()

    def revoke_authorization(
        self, actor_id: str, dataset_id: str, version: str, subject_id: str, reason: str
    ) -> dict[str, Any]:
        """撤回授权：立即阻断未启动计划；运行中计划转人工处置，不静默迁移。"""
        self._require(actor_id, "authorization.revoke")
        self._dataset_row(dataset_id, version)
        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("撤回原因不能为空")
        reason = reason.strip()
        now = self.clock.now()
        with transaction(self.connection, immediate=True):
            cursor = self.connection.execute(
                "UPDATE dataset_authorizations SET state='withdrawn',withdrawn_at=?,withdraw_reason=? "
                "WHERE dataset_id=? AND version=? AND subject_id=? AND state='granted'",
                (self._now(), reason, dataset_id, version, subject_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("授权不存在或已经撤回")

            blocked: list[str] = []
            manual: list[str] = []
            pending = self.connection.execute(
                "SELECT * FROM training_plans WHERE dataset_id=? AND version=? AND subject_id=? "
                "AND state IN ('ready','blocked') ORDER BY created_at,plan_id",
                (dataset_id, version, subject_id),
            ).fetchall()
            for plan in pending:
                self.connection.execute(
                    "UPDATE training_plans SET state='cancelled',blocked_reason='authorization_withdrawn',"
                    "updated_at=? WHERE plan_id=?",
                    (self._now(), plan["plan_id"]),
                )
                self._access_record(
                    plan,
                    "authorization.withdrawn_block",
                    {"reason": reason, "previous_state": plan["state"]},
                    actor_id,
                )
                blocked.append(plan["plan_id"])

            running = self.connection.execute(
                "SELECT * FROM training_plans WHERE dataset_id=? AND version=? AND subject_id=? "
                "AND state IN ('launching','running') ORDER BY created_at,plan_id",
                (dataset_id, version, subject_id),
            ).fetchall()
            for plan in running:
                self.connection.execute(
                    "UPDATE training_plans SET state='blocked_running',blocked_reason='authorization_withdrawn',"
                    "updated_at=? WHERE plan_id=?",
                    (self._now(), plan["plan_id"]),
                )
                self._access_record(
                    plan,
                    "authorization.withdrawn_manual",
                    {"reason": reason, "previous_state": plan["state"]},
                    actor_id,
                )
                manual.append(plan["plan_id"])

            self._audit(
                "authorization",
                f"{dataset_id}@{version}:{subject_id}",
                "authorization.revoked",
                actor_id,
                {"reason": reason, "blocked_plans": blocked, "manual_plans": manual},
            )
        return {
            "dataset_id": dataset_id,
            "version": version,
            "subject_id": subject_id,
            "state": "withdrawn",
            "revoked_at": utc_text(now),
            "blocked_plans": blocked,
            "manual_disposition_plans": manual,
        }

    def _access_record(
        self,
        plan: sqlite3.Row,
        event_type: str,
        detail: Mapping[str, Any],
        actor_id: str,
        *,
        site_id: str | None = None,
    ) -> None:
        """合规访问记录只追加，不修改或删除，撤回不能抹去已发生的访问。"""
        self.connection.execute(
            "INSERT INTO compliance_access_records(plan_id,dataset_id,version,subject_id,site_id,"
            "event_type,detail_json,actor_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                plan["plan_id"],
                plan["dataset_id"],
                plan["version"],
                plan["subject_id"],
                site_id,
                event_type,
                canonical_json(detail),
                actor_id,
                self._now(),
            ),
        )

    def _site_contexts(self, product: str) -> list[SiteContext]:
        rows = self.connection.execute(
            "SELECT f.facility_id,f.region,f.network_zone,f.active,l.available_gpu_hours "
            "FROM facilities f LEFT JOIN inventory_lots l "
            "ON l.facility_id=f.facility_id AND l.product=? ORDER BY f.facility_id",
            (product,),
        ).fetchall()
        totals: dict[str, Decimal] = {}
        meta: dict[str, tuple[str | None, str | None, bool]] = {}
        for row in rows:
            meta[row["facility_id"]] = (row["region"], row["network_zone"], bool(row["active"]))
            if row["available_gpu_hours"] is not None:
                totals[row["facility_id"]] = totals.get(row["facility_id"], Decimal(0)) + Decimal(
                    row["available_gpu_hours"]
                )
        return [
            SiteContext(
                facility_id=facility_id,
                region=meta[facility_id][0],
                network_zone=meta[facility_id][1],
                active=meta[facility_id][2],
                available_by_product={product: quantize_volume(totals.get(facility_id, Decimal(0)))},
            )
            for facility_id in sorted(meta)
        ]

    def submit_training_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        """提交训练作业计划，引用确定版本，只在三边界同时满足的站点生成候选。"""
        self._require(actor_id, "plan.write")
        request = TrainingPlanRequest.from_dict(raw)
        if request.subject_id != actor_id:
            raise Forbidden("租户只能为自己的主体登记作业计划")
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency "
            "WHERE scope='training_plan' AND idempotency_key=?",
            (request.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同作业计划内容")
            return json.loads(stored["response_json"])

        dataset = self._dataset_row(request.dataset_id, request.version)
        regions = json.loads(dataset["regions_json"])
        authorization = self._effective_authorization(
            request.dataset_id, request.version, request.subject_id
        )
        now = self.clock.now()
        subject_authorized = authorization is not None and authorization["state"] == "granted"
        expires_at = None
        if authorization is not None:
            expires_at = parse_utc(authorization["expires_at"])
            if now >= expires_at:
                subject_authorized = False

        evaluations = [
            evaluate_site(
                site=site,
                required_product=request.product,
                required_hours=request.requested_gpu_hours,
                required_zone=request.network_zone,
                subject_id=request.subject_id,
                allowed_regions=regions,
                authorization_expires_at=expires_at,
                subject_authorized=subject_authorized,
                now=now,
            )
            for site in self._site_contexts(request.product)
        ]
        eligible_sites = sorted(
            item["facility_id"] for item in evaluations if item["eligible"]
        )
        expired = expires_at is None or now >= expires_at
        reason = blocked_reason_code(
            authorization_state=None if authorization is None else authorization["state"],
            expired=expired,
            any_eligible=bool(eligible_sites),
        )
        state = "ready" if eligible_sites and subject_authorized and not expired else "blocked"
        response = {
            "plan_id": request.plan_id,
            "state": state,
            "dataset_id": request.dataset_id,
            "version": request.version,
            "subject_id": request.subject_id,
            "product": request.product,
            "requested_gpu_hours": decimal_text(request.requested_gpu_hours),
            "network_zone": request.network_zone,
            "candidate_sites": eligible_sites,
            "blocked_reason": None if state == "ready" else reason,
            "site_evaluations": evaluations,
            "evaluated_at": utc_text(now),
        }
        try:
            with transaction(self.connection, immediate=True):
                self.connection.execute(
                    "INSERT INTO training_plans(plan_id,dataset_id,version,subject_id,product,"
                    "requested_gpu_hours,network_zone,state,blocked_reason,candidate_sites_json,"
                    "idempotency_key,created_by,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        request.plan_id,
                        request.dataset_id,
                        request.version,
                        request.subject_id,
                        request.product,
                        decimal_text(request.requested_gpu_hours),
                        request.network_zone,
                        state,
                        response["blocked_reason"],
                        canonical_json(evaluations),
                        request.idempotency_key,
                        actor_id,
                        self._now(),
                        self._now(),
                    ),
                )
                self.connection.execute(
                    "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('training_plan',?,?,?,?)",
                    (request.idempotency_key, request_digest, canonical_json(response), self._now()),
                )
                plan_row = self.connection.execute(
                    "SELECT * FROM training_plans WHERE plan_id=?", (request.plan_id,)
                ).fetchone()
                self._access_record(plan_row, "plan.submitted", {"state": state, "candidate_sites": eligible_sites}, actor_id)
                self._audit(
                    "training_plan",
                    request.plan_id,
                    "plan.submitted",
                    actor_id,
                    {
                        "state": state,
                        "candidate_sites": eligible_sites,
                        "blocked_reason": response["blocked_reason"],
                    },
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("作业计划编号或幂等键冲突") from exc
        return response

    def launch_plan(self, actor_id: str, plan_id: str, site_id: str) -> dict[str, Any]:
        """调度员在候选站点启动作业；启动前再次校验授权与边界，禁止静默迁移。"""
        self._require(actor_id, "plan.launch")
        plan = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFound("作业计划不存在")
        if plan["state"] != "ready":
            raise InvalidState("只有候选已就绪的计划可以启动")
        evaluations = json.loads(plan["candidate_sites_json"])
        chosen = next((item for item in evaluations if item["facility_id"] == site_id), None)
        if chosen is None:
            raise ValidationFailed("站点未参与该计划的边界评估")
        with transaction(self.connection, immediate=True):
            # 重新评估必须在写事务内完成，防止撤回/到期与启动并发交错。
            locked = self.connection.execute(
                "SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)
            ).fetchone()
            if locked["state"] != "ready":
                raise InvalidState("计划在启动前已被阻断或取消")
            fresh = self._reevaluate_plan(locked)
            fresh_chosen = next(item for item in fresh if item["facility_id"] == site_id)
            if not fresh_chosen["eligible"]:
                raise InvalidState("授权或边界已变化，该站点不再满足启动条件")
            cursor = self.connection.execute(
                "UPDATE training_plans SET state='launching',selected_site=?,updated_at=? "
                "WHERE plan_id=? AND state='ready'",
                (site_id, self._now(), plan_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("计划在启动前已被阻断或取消")
            updated = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
            self._access_record(
                updated, "plan.launching", {"site_id": site_id}, actor_id, site_id=site_id
            )
            self._audit("training_plan", plan_id, "plan.launching", actor_id, {"site_id": site_id})
        return {"plan_id": plan_id, "state": "launching", "selected_site": site_id}

    def mark_plan_running(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        self._require(actor_id, "plan.launch")
        plan = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFound("作业计划不存在")
        if plan["state"] != "launching":
            raise InvalidState("只有启动中的计划可以进入运行状态")
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE training_plans SET state='running',updated_at=? WHERE plan_id=?",
                (self._now(), plan_id),
            )
            updated = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
            self._access_record(
                updated,
                "plan.ran",
                {"site_id": plan["selected_site"]},
                actor_id,
                site_id=plan["selected_site"],
            )
            self._audit("training_plan", plan_id, "plan.ran", actor_id, {"site_id": plan["selected_site"]})
        return {"plan_id": plan_id, "state": "running", "selected_site": plan["selected_site"]}

    def dispose_running_plan(self, actor_id: str, plan_id: str, decision: str, note: str) -> dict[str, Any]:
        """对撤回后运行中的作业做人工处置：终止或继续受控运行，不允许静默迁移。"""
        self._require(actor_id, "authorization.revoke")
        if decision not in {"halt", "continue_monitored"}:
            raise ValidationFailed("decision 必须是 halt 或 continue_monitored")
        if not isinstance(note, str) or not note.strip():
            raise ValidationFailed("人工处置说明不能为空")
        plan = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFound("作业计划不存在")
        if plan["state"] != "blocked_running":
            raise InvalidState("只有等待人工处置的运行中作业可以处置")
        new_state = "halted" if decision == "halt" else "running"
        with transaction(self.connection, immediate=True):
            self.connection.execute(
                "UPDATE training_plans SET state=?,updated_at=? WHERE plan_id=?",
                (new_state, self._now(), plan_id),
            )
            updated = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
            self._access_record(
                updated,
                "plan.manual_disposition",
                {"decision": decision, "note": note.strip(), "site_id": plan["selected_site"]},
                actor_id,
                site_id=plan["selected_site"],
            )
            self._audit(
                "training_plan",
                plan_id,
                "plan.manual_disposition",
                actor_id,
                {"decision": decision, "site_id": plan["selected_site"]},
            )
        return {"plan_id": plan_id, "state": new_state, "selected_site": plan["selected_site"]}

    def _reevaluate_plan(self, plan: sqlite3.Row) -> list[dict[str, Any]]:
        dataset = self._dataset_row(plan["dataset_id"], plan["version"])
        regions = json.loads(dataset["regions_json"])
        authorization = self._effective_authorization(
            plan["dataset_id"], plan["version"], plan["subject_id"]
        )
        now = self.clock.now()
        subject_authorized = authorization is not None and authorization["state"] == "granted"
        expires_at = None if authorization is None else parse_utc(authorization["expires_at"])
        if expires_at is not None and now >= expires_at:
            subject_authorized = False
        return [
            evaluate_site(
                site=site,
                required_product=plan["product"],
                required_hours=Decimal(plan["requested_gpu_hours"]),
                required_zone=plan["network_zone"],
                subject_id=plan["subject_id"],
                allowed_regions=regions,
                authorization_expires_at=expires_at,
                subject_authorized=subject_authorized,
                now=now,
            )
            for site in self._site_contexts(plan["product"])
        ]

    # ---- 最小必要视图：租户 / 调度员 / 审计人员 ------------------------

    def _plan_row_or_404(self, plan_id: str) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM training_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if row is None:
            raise NotFound("作业计划不存在")
        return row

    def tenant_plan_view(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        """租户视图：只看本主体计划，含候选与排除原因，不含其他主体信息。"""
        self._require(actor_id, "plan.read.self")
        plan = self._plan_row_or_404(plan_id)
        if plan["subject_id"] != actor_id:
            raise Forbidden("只能查询本主体的作业计划")
        return {
            "plan_id": plan["plan_id"],
            "state": plan["state"],
            "dataset_id": plan["dataset_id"],
            "version": plan["version"],
            "product": plan["product"],
            "requested_gpu_hours": plan["requested_gpu_hours"],
            "candidate_sites": [
                item["facility_id"]
                for item in json.loads(plan["candidate_sites_json"])
                if item["eligible"]
            ],
            "site_explanations": [
                {"facility_id": item["facility_id"], "eligible": item["eligible"],
                 "failed_rules": [rule["rule"] for rule in item["rules"] if not rule["passed"]],
                 "reasons": [rule["detail"] for rule in item["rules"] if not rule["passed"]]}
                for item in json.loads(plan["candidate_sites_json"])
            ],
            "blocked_reason": plan["blocked_reason"],
        }

    def dispatcher_plan_view(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        """调度员视图：跨主体的调度字段、候选站点与逐条边界规则。"""
        self._require(actor_id, "plan.read")
        plan = self._plan_row_or_404(plan_id)
        evaluations = json.loads(plan["candidate_sites_json"])
        return {
            "plan_id": plan["plan_id"],
            "state": plan["state"],
            "subject_id": plan["subject_id"],
            "dataset_id": plan["dataset_id"],
            "version": plan["version"],
            "product": plan["product"],
            "requested_gpu_hours": plan["requested_gpu_hours"],
            "network_zone": plan["network_zone"],
            "candidate_sites": [item["facility_id"] for item in evaluations if item["eligible"]],
            "selected_site": plan["selected_site"],
            "blocked_reason": plan["blocked_reason"],
            "site_evaluations": evaluations,
            "updated_at": plan["updated_at"],
        }

    def auditor_compliance_view(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        """审计视图：完整边界判定、授权状态与只增访问记录。"""
        self._require(actor_id, "compliance.read")
        plan = self._plan_row_or_404(plan_id)
        dataset = self._dataset_row(plan["dataset_id"], plan["version"])
        authorization = self._effective_authorization(
            plan["dataset_id"], plan["version"], plan["subject_id"]
        )
        records = self.connection.execute(
            "SELECT access_id,site_id,event_type,detail_json,actor_id,created_at "
            "FROM compliance_access_records WHERE plan_id=? ORDER BY access_id",
            (plan_id,),
        ).fetchall()
        return {
            "plan_id": plan["plan_id"],
            "state": plan["state"],
            "subject_id": plan["subject_id"],
            "selected_site": plan["selected_site"],
            "dataset": {
                "dataset_id": dataset["dataset_id"],
                "version": dataset["version"],
                "regions": json.loads(dataset["regions_json"]),
                "content_sha256": dataset["content_sha256"],
                "registered_by": dataset["registered_by"],
                "registered_at": dataset["registered_at"],
            },
            "authorization": None
            if authorization is None
            else {
                "state": authorization["state"],
                "granted_at": authorization["granted_at"],
                "expires_at": authorization["expires_at"],
                "withdrawn_at": authorization["withdrawn_at"],
                "withdraw_reason": authorization["withdraw_reason"],
                "batch_id": authorization["batch_id"],
            },
            "site_evaluations": json.loads(plan["candidate_sites_json"]),
            "access_records": [
                {
                    "access_id": row["access_id"],
                    "site_id": row["site_id"],
                    "event_type": row["event_type"],
                    "detail": json.loads(row["detail_json"]),
                    "actor_id": row["actor_id"],
                    "created_at": row["created_at"],
                }
                for row in records
            ],
        }

    def audit_chain(self, actor_id: str) -> dict[str, Any]:
        self._require(actor_id, "audit.read")
        rows = self.connection.execute("SELECT * FROM supply_audit_events ORDER BY event_id").fetchall()
        previous_hash = "0" * 64
        valid = True
        for row in rows:
            body = {
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "event_type": row["event_type"],
                "actor_id": row["actor_id"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
                "previous_hash": row["previous_hash"],
            }
            calculated = hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
            if row["previous_hash"] != previous_hash or row["event_hash"] != calculated:
                valid = False
                break
            previous_hash = row["event_hash"]
        return {"valid": valid, "events": len(rows), "head_hash": previous_hash}
