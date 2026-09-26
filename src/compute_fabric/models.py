"""数据中心调度领域输入契约。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .clock import parse_utc
from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
PRICE_INDEXES = {"PEAK_VALLEY", "ON_DEMAND", "RESERVED", "SPOT", "INTERNAL", "CUSTOM"}
PRODUCTS = {"gpu-h100", "gpu-a100", "gpu-l40s", "accelerator-npu", "cpu-highmem", "storage-io"}
ROUTE_KINDS = {"interconnect", "inference-pool", "tenant", "storage", "edge-site"}


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def decimal_value(
    value: object,
    field: str,
    *,
    minimum: Decimal | None = None,
    maximum: Decimal | None = None,
) -> Decimal:
    if isinstance(value, bool):
        raise ValidationFailed(f"{field} 必须是数值")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValidationFailed(f"{field} 必须是十进制数值") from exc
    if not result.is_finite():
        raise ValidationFailed(f"{field} 必须是有限数值")
    if minimum is not None and result < minimum:
        raise ValidationFailed(f"{field} 不能小于 {minimum}")
    if maximum is not None and result > maximum:
        raise ValidationFailed(f"{field} 不能大于 {maximum}")
    return result


def positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValidationFailed(f"{field} 必须是正整数")
    return value


def date_text(value: object, field: str) -> str:
    result = required_text(value, field, 10)
    try:
        return date.fromisoformat(result).isoformat()
    except ValueError as exc:
        raise ValidationFailed(f"{field} 必须是 YYYY-MM-DD 日期") from exc


@dataclass(frozen=True, slots=True)
class IndexQuote:
    market_index: str
    trade_date: str
    close_cny: Decimal
    source_revision: str
    observed_at: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "IndexQuote":
        market_index = required_text(raw.get("market_index"), "market_index", 16).upper()
        if market_index not in PRICE_INDEXES - {"CUSTOM"}:
            raise ValidationFailed("market_index 必须是 PEAK_VALLEY、ON_DEMAND、RESERVED、SPOT 或 INTERNAL")
        observed_at = required_text(raw.get("observed_at"), "observed_at", 40)
        try:
            parse_utc(observed_at, "observed_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        return cls(
            market_index=market_index,
            trade_date=date_text(raw.get("trade_date"), "trade_date"),
            close_cny=decimal_value(raw.get("close_cny"), "close_cny", minimum=Decimal("0.01")),
            source_revision=identifier(raw.get("source_revision"), "source_revision"),
            observed_at=observed_at,
        )


@dataclass(frozen=True, slots=True)
class Facility:
    facility_id: str
    name: str
    kind: str
    timezone: str
    capacity_gpu_hours: Decimal
    region: str | None = None
    network_boundary: str | None = None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Facility":
        kind = required_text(raw.get("kind"), "kind", 24)
        if kind not in ROUTE_KINDS:
            raise ValidationFailed("kind 不是受支持的设施类型")
        timezone = required_text(raw.get("timezone"), "timezone", 64)
        if "/" not in timezone and timezone != "UTC":
            raise ValidationFailed("timezone 必须是 IANA 时区或 UTC")
        return cls(
            facility_id=identifier(raw.get("facility_id"), "facility_id"),
            name=required_text(raw.get("name"), "name"),
            kind=kind,
            timezone=timezone,
            capacity_gpu_hours=decimal_value(
                raw.get("capacity_gpu_hours"), "capacity_gpu_hours", minimum=Decimal("0")
            ),
            region=optional_identifier(raw.get("region"), "region"),
            network_boundary=optional_identifier(raw.get("network_boundary"), "network_boundary"),
        )


def optional_identifier(value: object, field: str) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return identifier(value, field)


def region_codes(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ValidationFailed(f"{field} 必须是非空地域代码数组")
    codes = {identifier(item, f"{field}[]") for item in value}
    return tuple(sorted(codes))


def sha256_text(value: object, field: str) -> str:
    result = required_text(value, field, 64).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", result):
        raise ValidationFailed(f"{field} 必须是 64 位十六进制 SHA-256")
    return result


def expires_at_text(value: object, field: str = "expires_at") -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    text = required_text(value, field, 40)
    try:
        return parse_utc(text, field).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ValidationFailed(str(exc)) from exc


@dataclass(frozen=True, slots=True)
class DatasetRegistration:
    dataset_id: str
    name: str
    owner_tenant_id: str
    description: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "DatasetRegistration":
        description = raw.get("description", "")
        if not isinstance(description, str):
            raise ValidationFailed("description 必须是字符串")
        description = description.strip()
        if len(description) > 1024:
            raise ValidationFailed("description 不能超过 1024 个字符")
        return cls(
            dataset_id=identifier(raw.get("dataset_id"), "dataset_id"),
            name=required_text(raw.get("name"), "name"),
            owner_tenant_id=identifier(raw.get("owner_tenant_id"), "owner_tenant_id"),
            description=description,
        )


@dataclass(frozen=True, slots=True)
class DatasetVersionInput:
    version_id: str
    dataset_id: str
    version_tag: str
    content_sha256: str
    region_scope: tuple[str, ...]
    network_boundary: str
    expires_at: str | None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "DatasetVersionInput":
        return cls(
            version_id=identifier(raw.get("version_id"), "version_id"),
            dataset_id=identifier(raw.get("dataset_id"), "dataset_id"),
            version_tag=identifier(raw.get("version_tag"), "version_tag"),
            content_sha256=sha256_text(raw.get("content_sha256"), "content_sha256"),
            region_scope=region_codes(raw.get("region_scope"), "region_scope"),
            network_boundary=identifier(raw.get("network_boundary"), "network_boundary"),
            expires_at=expires_at_text(raw.get("expires_at")),
        )


SUBJECT_TYPES = {"tenant", "user"}


@dataclass(frozen=True, slots=True)
class GrantInput:
    grant_id: str
    dataset_id: str
    version_id: str | None
    subject_id: str
    subject_type: str
    region_scope: tuple[str, ...]
    network_boundary: str
    expires_at: str | None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "GrantInput":
        subject_type = required_text(raw.get("subject_type"), "subject_type", 16).lower()
        if subject_type not in SUBJECT_TYPES:
            raise ValidationFailed("subject_type 必须是 tenant 或 user")
        return cls(
            grant_id=identifier(raw.get("grant_id"), "grant_id"),
            dataset_id=identifier(raw.get("dataset_id"), "dataset_id"),
            version_id=optional_identifier(raw.get("version_id"), "version_id"),
            subject_id=identifier(raw.get("subject_id"), "subject_id"),
            subject_type=subject_type,
            region_scope=region_codes(raw.get("region_scope"), "region_scope"),
            network_boundary=identifier(raw.get("network_boundary"), "network_boundary"),
            expires_at=expires_at_text(raw.get("expires_at")),
        )


@dataclass(frozen=True, slots=True)
class JobPlanInput:
    plan_id: str
    tenant_id: str
    version_id: str
    product: str
    scheduled_start_at: str
    scheduled_end_at: str | None
    required_gpu_hours: Decimal
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "JobPlanInput":
        product = required_text(raw.get("product"), "product", 32)
        if product not in PRODUCTS:
            raise ValidationFailed("product 不是受支持的资源类型")
        start_text = required_text(raw.get("scheduled_start_at"), "scheduled_start_at", 40)
        try:
            start = parse_utc(start_text, "scheduled_start_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        end = None
        if raw.get("scheduled_end_at") is not None:
            try:
                end = parse_utc(required_text(raw.get("scheduled_end_at"), "scheduled_end_at", 40), "scheduled_end_at")
            except ValueError as exc:
                raise ValidationFailed(str(exc)) from exc
            if end <= start:
                raise ValidationFailed("scheduled_end_at 必须晚于 scheduled_start_at")
        return cls(
            plan_id=identifier(raw.get("plan_id"), "plan_id"),
            tenant_id=identifier(raw.get("tenant_id"), "tenant_id"),
            version_id=identifier(raw.get("version_id"), "version_id"),
            product=product,
            scheduled_start_at=start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            scheduled_end_at=None if end is None else end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            required_gpu_hours=decimal_value(
                raw.get("required_gpu_hours"), "required_gpu_hours", minimum=Decimal("0.001")
            ),
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class Route:
    route_id: str
    origin_id: str
    destination_id: str
    product: str
    daily_capacity: Decimal
    loss_basis_points: int
    transit_hours: int

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Route":
        product = required_text(raw.get("product"), "product", 32)
        if product not in PRODUCTS:
            raise ValidationFailed("product 不是受支持的资源类型")
        loss = raw.get("loss_basis_points", 0)
        if isinstance(loss, bool) or not isinstance(loss, int) or not 0 <= loss <= 1000:
            raise ValidationFailed("loss_basis_points 必须是 0 到 1000 的整数")
        origin = identifier(raw.get("origin_id"), "origin_id")
        destination = identifier(raw.get("destination_id"), "destination_id")
        if origin == destination:
            raise ValidationFailed("互联通道起点和终点不能相同")
        return cls(
            route_id=identifier(raw.get("route_id"), "route_id"),
            origin_id=origin,
            destination_id=destination,
            product=product,
            daily_capacity=decimal_value(
                raw.get("daily_capacity"), "daily_capacity", minimum=Decimal("0.001")
            ),
            loss_basis_points=loss,
            transit_hours=positive_integer(raw.get("transit_hours"), "transit_hours"),
        )


@dataclass(frozen=True, slots=True)
class InventoryLot:
    lot_id: str
    facility_id: str
    product: str
    grade: str
    quantity_gpu_hours: Decimal
    unit_cost_cny: Decimal
    received_at: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "InventoryLot":
        product = required_text(raw.get("product"), "product", 32)
        if product not in PRODUCTS:
            raise ValidationFailed("product 不是受支持的资源类型")
        received_at = required_text(raw.get("received_at"), "received_at", 40)
        try:
            parse_utc(received_at, "received_at")
        except ValueError as exc:
            raise ValidationFailed(str(exc)) from exc
        return cls(
            lot_id=identifier(raw.get("lot_id"), "lot_id"),
            facility_id=identifier(raw.get("facility_id"), "facility_id"),
            product=product,
            grade=required_text(raw.get("grade"), "grade", 32).upper(),
            quantity_gpu_hours=decimal_value(
                raw.get("quantity_gpu_hours"), "quantity_gpu_hours", minimum=Decimal("0.001")
            ),
            unit_cost_cny=decimal_value(
                raw.get("unit_cost_cny"), "unit_cost_cny", minimum=Decimal("0")
            ),
            received_at=received_at,
        )


@dataclass(frozen=True, slots=True)
class NominationRequest:
    nomination_id: str
    route_id: str
    shipper_id: str
    service_date: str
    requested_gpu_hours: Decimal
    priority: int
    idempotency_key: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "NominationRequest":
        priority = raw.get("priority", 100)
        if isinstance(priority, bool) or not isinstance(priority, int) or not 1 <= priority <= 999:
            raise ValidationFailed("priority 必须是 1 到 999 的整数")
        return cls(
            nomination_id=identifier(raw.get("nomination_id"), "nomination_id"),
            route_id=identifier(raw.get("route_id"), "route_id"),
            shipper_id=identifier(raw.get("shipper_id"), "shipper_id"),
            service_date=date_text(raw.get("service_date"), "service_date"),
            requested_gpu_hours=decimal_value(
                raw.get("requested_gpu_hours"), "requested_gpu_hours", minimum=Decimal("0.001")
            ),
            priority=priority,
            idempotency_key=identifier(raw.get("idempotency_key"), "idempotency_key"),
        )


@dataclass(frozen=True, slots=True)
class SupplyScenario:
    scenario_id: str
    name: str
    market_index_drop_percent: Decimal
    route_capacity_changes: Mapping[str, Decimal]
    demand_changes: Mapping[str, Decimal]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "SupplyScenario":
        route_changes = raw.get("route_capacity_changes", {})
        demand_changes = raw.get("demand_changes", {})
        if not isinstance(route_changes, Mapping) or not isinstance(demand_changes, Mapping):
            raise ValidationFailed("情景变化必须是对象")
        parsed_routes = {
            identifier(key, "route_capacity_changes 键"): decimal_value(
                value, f"route_capacity_changes.{key}", minimum=Decimal("-100"), maximum=Decimal("500")
            )
            for key, value in route_changes.items()
        }
        parsed_demand = {
            identifier(key, "demand_changes 键"): decimal_value(
                value, f"demand_changes.{key}", minimum=Decimal("-100"), maximum=Decimal("500")
            )
            for key, value in demand_changes.items()
        }
        return cls(
            scenario_id=identifier(raw.get("scenario_id"), "scenario_id"),
            name=required_text(raw.get("name"), "name"),
            market_index_drop_percent=decimal_value(
                raw.get("market_index_drop_percent", 0),
                "market_index_drop_percent",
                minimum=Decimal("-500"),
                maximum=Decimal("100"),
            ),
            route_capacity_changes=parsed_routes,
            demand_changes=parsed_demand,
        )
