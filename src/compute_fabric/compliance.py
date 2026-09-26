"""数据集驻留合规与调度联动的纯规则计算。

站点候选必须同时满足数据边界、算力边界和网络边界。
本模块不接触数据库和时钟，全部输入显式给出，保证评估结果确定可复核。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from .planning import decimal_text, quantize_volume

# 固定顺序：解释站点被排除时按此顺序列出规则命中情况。
RULE_ORDER = (
    "site.registered",
    "data.region",
    "data.subject",
    "data.expiry",
    "compute.product",
    "compute.capacity",
    "network.zone",
)

BOUNDARY_OF_RULE = {
    "site.registered": "site",
    "data.region": "data",
    "data.subject": "data",
    "data.expiry": "data",
    "compute.product": "compute",
    "compute.capacity": "compute",
    "network.zone": "network",
}


@dataclass(frozen=True, slots=True)
class SiteContext:
    """站点在评估时刻的边界与算力快照。"""

    facility_id: str
    region: str | None
    network_zone: str | None
    active: bool
    available_by_product: Mapping[str, Decimal]


def _rule(code: str, passed: bool, detail: str) -> dict[str, object]:
    return {"rule": code, "boundary": BOUNDARY_OF_RULE[code], "passed": passed, "detail": detail}


def evaluate_site(
    *,
    site: SiteContext,
    required_product: str,
    required_hours: Decimal,
    required_zone: str,
    subject_id: str,
    allowed_regions: Sequence[str],
    authorization_expires_at,
    subject_authorized: bool,
    now,
) -> dict[str, object]:
    """评估单个站点是否能成为计划候选，返回逐条规则的命中解释。

    ``authorization_expires_at`` 为带时区的 datetime；主体未持有授权时为 None。
    数据侧规则在每个站点重复给出，保证任一站点的排除原因都完整可解释。
    """

    rules: list[dict[str, object]] = []

    registered = bool(site.active and site.region and site.network_zone)
    if not site.active:
        registered_detail = "站点已停用"
    elif not site.region or not site.network_zone:
        registered_detail = "站点未登记地域或网络边界"
    else:
        registered_detail = "站点边界已登记"
    rules.append(_rule("site.registered", registered, registered_detail))

    if site.region is None:
        region_passed = False
        region_detail = "站点地域未登记，无法匹配数据集驻留范围"
    else:
        region_passed = site.region in set(allowed_regions)
        region_detail = (
            f"站点地域 {site.region} 属于数据集允许范围 {sorted(allowed_regions)}"
            if region_passed
            else f"站点地域 {site.region} 不在数据集允许驻留范围 {sorted(allowed_regions)}"
        )
    rules.append(_rule("data.region", region_passed, region_detail))

    rules.append(
        _rule(
            "data.subject",
            subject_authorized,
            f"主体 {subject_id} 持有该数据集版本的有效授权"
            if subject_authorized
            else f"主体 {subject_id} 未持有该数据集版本的有效授权（未授权或已撤回）",
        )
    )

    if authorization_expires_at is None:
        expired = True
        expiry_text = "未登记授权"
    else:
        expired = now >= authorization_expires_at
        expiry_text = authorization_expires_at.isoformat()
    rules.append(
        _rule(
            "data.expiry",
            not expired,
            f"主体授权有效期至 {expiry_text}，评估时刻尚未到期"
            if not expired
            else (
                "主体未持有授权，没有到期时间"
                if authorization_expires_at is None
                else f"数据集主体授权已于 {expiry_text} 到期"
            ),
        )
    )

    available = quantize_volume(site.available_by_product.get(required_product, Decimal(0)))
    product_passed = available > 0
    rules.append(
        _rule(
            "compute.product",
            product_passed,
            f"站点提供 {required_product} 算力"
            if product_passed
            else f"站点不提供所需算力产品 {required_product}",
        )
    )

    needed = quantize_volume(required_hours)
    capacity_passed = available >= needed
    rules.append(
        _rule(
            "compute.capacity",
            capacity_passed,
            f"站点 {required_product} 可用 {decimal_text(available)} 满足需求 {decimal_text(needed)}"
            if capacity_passed
            else f"站点 {required_product} 可用 {decimal_text(available)} 小于计划需求 {decimal_text(needed)}",
        )
    )

    if site.network_zone is None:
        zone_passed = False
        zone_detail = "站点网络分区未登记，无法匹配网络边界"
    else:
        zone_passed = site.network_zone == required_zone
        zone_detail = (
            f"站点网络分区 {site.network_zone} 与计划要求 {required_zone} 一致"
            if zone_passed
            else f"站点网络分区 {site.network_zone} 与计划要求 {required_zone} 不一致"
        )
    rules.append(_rule("network.zone", zone_passed, zone_detail))

    eligible = all(item["passed"] for item in rules)
    return {
        "facility_id": site.facility_id,
        "eligible": eligible,
        "rules": sorted(rules, key=lambda item: RULE_ORDER.index(str(item["rule"]))),
    }


def blocked_reason_code(
    *,
    authorization_state: str | None,
    expired: bool,
    any_eligible: bool,
) -> str:
    """全部站点落选时，给出最主要的阻断原因代码。

    ``authorization_state`` 为 None 表示从未授权；撤回优先于到期，
    因为撤回是主动生效的合规事件。
    """

    if authorization_state is None:
        return "authorization_missing"
    if authorization_state == "withdrawn":
        return "authorization_withdrawn"
    if expired:
        return "authorization_expired"
    if not any_eligible:
        return "no_eligible_site"
    return ""
