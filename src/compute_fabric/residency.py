"""数据集驻留合规与调度联动的纯规则。

规则评估对每个候选站点依次检查数据边界（版本登记/到期/冻结）、
授权边界（主体、地域、网络、有效期）、算力边界（产品、库存）和
网络边界（站点网络域）。所有判定均为纯函数，便于复算与审计。
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence


# 规则代码 -> 中文解释模板，用于“为什么该站点被排除”。
RULE_MESSAGES: Mapping[str, str] = {
    "OK": "同时满足数据、授权、算力和网络边界",
    "DATA.VERSION_UNKNOWN": "数据集版本未登记，作业必须引用确定版本",
    "DATA.FROZEN": "数据集版本已冻结：{detail}",
    "DATA.EXPIRED": "数据集版本已于 {detail} 到期",
    "DATA.REGION_EXCLUDED": "站点地域 {detail} 不在数据集版本允许驻留的地域范围内",
    "GRANT.NONE": "租户 {detail} 没有该数据集的任何授权",
    "GRANT.SUBJECT_MISMATCH": "授权主体不包含当前租户 {detail}",
    "GRANT.REGION_EXCLUDED": "授权地域范围不覆盖站点地域 {detail}",
    "GRANT.NETWORK_EXCLUDED": "授权网络边界不包含站点网络域 {detail}",
    "GRANT.EXPIRED": "覆盖该站点的授权已于 {detail} 到期",
    "GRANT.REVOKED": "覆盖该站点的授权已被撤回（撤回不抹除历史访问）",
    "FACILITY.UNKNOWN": "站点不存在或未登记地域与网络边界",
    "FACILITY.INACTIVE": "站点已停用",
    "FACILITY.REGION_UNDECLARED": "站点未登记驻留地域，无法校验数据驻留",
    "FACILITY.NETWORK_UNDECLARED": "站点未登记网络边界",
    "COMPUTE.PRODUCT_UNAVAILABLE": "站点不提供所需算力产品 {detail}",
    "COMPUTE.INSUFFICIENT": "站点可用算力 {detail} 不足",
    "NETWORK.BOUNDARY_MISMATCH": "数据集要求网络域 {detail}，站点不满足",
}

BLOCKING_RULES = {code for code in RULE_MESSAGES if code != "OK"}


def explain(code: str, detail: str = "") -> str:
    template = RULE_MESSAGES.get(code, code)
    rendered = template.format(detail=detail) if detail else template.replace("{detail}", "")
    return " ".join(rendered.split())


@dataclass(frozen=True, slots=True)
class VersionSnapshot:
    version_id: str
    dataset_id: str
    region_scope: frozenset[str]
    network_boundary: str
    expires_at: str | None
    state: str  # registered | frozen
    frozen_reason: str | None


@dataclass(frozen=True, slots=True)
class GrantSnapshot:
    grant_id: str
    subject_id: str
    subject_type: str
    region_scope: frozenset[str]
    network_boundary: str
    expires_at: str | None
    state: str  # active | revoked | expired


@dataclass(frozen=True, slots=True)
class FacilitySnapshot:
    facility_id: str
    region: str | None
    network_boundary: str | None
    active: bool
    products: frozenset[str]
    available_gpu_hours: Decimal


@dataclass(frozen=True, slots=True)
class SiteVerdict:
    facility_id: str
    eligible: bool
    rule_code: str
    rule_detail: str
    rule_message: str

    def as_dict(self) -> dict[str, object]:
        return {
            "facility_id": self.facility_id,
            "eligible": self.eligible,
            "rule_code": self.rule_code,
            "rule_detail": self.rule_detail,
            "rule_message": self.rule_message,
        }


@dataclass(frozen=True, slots=True)
class EvaluationInput:
    tenant_id: str
    version: VersionSnapshot | None
    grants: Sequence[GrantSnapshot]
    facilities: Sequence[FacilitySnapshot]
    product: str
    required_gpu_hours: Decimal
    as_of: str
    user_id: str | None = None

    def subject_ids(self) -> set[tuple[str, str]]:
        subjects = {("tenant", self.tenant_id)}
        if self.user_id:
            subjects.add(("user", self.user_id))
        return subjects

    def canonical(self) -> dict[str, object]:
        return {
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "version": None
            if self.version is None
            else {
                "version_id": self.version.version_id,
                "dataset_id": self.version.dataset_id,
                "region_scope": sorted(self.version.region_scope),
                "network_boundary": self.version.network_boundary,
                "expires_at": self.version.expires_at,
                "state": self.version.state,
            },
            "grants": [
                {
                    "grant_id": grant.grant_id,
                    "subject_id": grant.subject_id,
                    "subject_type": grant.subject_type,
                    "region_scope": sorted(grant.region_scope),
                    "network_boundary": grant.network_boundary,
                    "expires_at": grant.expires_at,
                    "state": grant.state,
                }
                for grant in sorted(self.grants, key=lambda item: item.grant_id)
            ],
            "facilities": [
                {
                    "facility_id": item.facility_id,
                    "region": item.region,
                    "network_boundary": item.network_boundary,
                    "active": item.active,
                    "products": sorted(item.products),
                    "available_gpu_hours": str(item.available_gpu_hours),
                }
                for item in sorted(self.facilities, key=lambda item: item.facility_id)
            ],
            "product": self.product,
            "required_gpu_hours": str(self.required_gpu_hours),
            "as_of": self.as_of,
        }


# 多授权并存时的原因优先级：撤回最优先（必须显式可见），其次到期，再次边界。
_REASON_PRIORITY = {
    "GRANT.REVOKED": 0,
    "GRANT.EXPIRED": 1,
    "GRANT.REGION_EXCLUDED": 2,
    "GRANT.NETWORK_EXCLUDED": 3,
}


def _global_verdict(data: EvaluationInput) -> tuple[str, str] | None:
    version = data.version
    if version is None:
        return "DATA.VERSION_UNKNOWN", ""
    if version.state == "frozen":
        return "DATA.FROZEN", version.frozen_reason or "版本冻结"
    if version.expires_at is not None and version.expires_at < data.as_of:
        return "DATA.EXPIRED", version.expires_at
    return None


def _grant_failure(
    grant: GrantSnapshot,
    region: str,
    network_boundary: str,
    as_of: str,
) -> str:
    """主体已匹配时，返回该授权无法覆盖站点的规则代码；空串表示覆盖。"""
    if grant.state == "revoked":
        return "GRANT.REVOKED"
    if grant.state == "expired" or (grant.expires_at is not None and grant.expires_at < as_of):
        detail = grant.expires_at or as_of
        return "GRANT.EXPIRED"
    if region not in grant.region_scope:
        return "GRANT.REGION_EXCLUDED"
    if grant.network_boundary != network_boundary:
        return "GRANT.NETWORK_EXCLUDED"
    return ""


def evaluate_site(
    facility: FacilitySnapshot,
    data: EvaluationInput,
) -> SiteVerdict:
    """对单个站点做完整边界评估，返回首个阻断规则或 OK。"""
    version = data.version

    def fail(code: str, detail: str = "") -> SiteVerdict:
        return SiteVerdict(facility.facility_id, False, code, detail, explain(code, detail))

    # 0) 与站点无关的数据版本全局状态
    global_rule = _global_verdict(data)
    if global_rule is not None:
        return fail(*global_rule)

    # 1) 站点基础边界
    if not facility.active:
        return fail("FACILITY.INACTIVE")
    if facility.region is None:
        return fail("FACILITY.REGION_UNDECLARED")
    if facility.network_boundary is None:
        return fail("FACILITY.NETWORK_UNDECLARED")

    # 2) 数据驻留边界：版本允许地域 + 网络域
    assert version is not None
    if facility.region not in version.region_scope:
        return fail("DATA.REGION_EXCLUDED", facility.region)
    if version.network_boundary != facility.network_boundary:
        return fail("NETWORK.BOUNDARY_MISMATCH", version.network_boundary)

    # 3) 主体授权边界：在主体匹配的授权中寻找任一覆盖该站点者
    subjects = data.subject_ids()
    matched = [
        grant
        for grant in data.grants
        if (grant.subject_type, grant.subject_id) in subjects
    ]
    if not matched:
        return fail("GRANT.NONE", data.tenant_id)
    best_failure: str | None = None
    for grant in matched:
        reason = _grant_failure(grant, facility.region, facility.network_boundary, data.as_of)
        if not reason:
            best_failure = None
            break
        if best_failure is None or _REASON_PRIORITY[reason] < _REASON_PRIORITY[best_failure]:
            best_failure = reason
    if best_failure is not None:
        detail = {
            "GRANT.REGION_EXCLUDED": facility.region,
            "GRANT.NETWORK_EXCLUDED": facility.network_boundary,
        }.get(best_failure, "")
        if best_failure == "GRANT.EXPIRED":
            expired = next(
                (grant.expires_at for grant in matched
                 if _grant_failure(grant, facility.region, facility.network_boundary, data.as_of) == "GRANT.EXPIRED"),
                "",
            )
            detail = expired or ""
        return fail(best_failure, detail)

    # 4) 算力边界
    if data.product not in facility.products:
        return fail("COMPUTE.PRODUCT_UNAVAILABLE", data.product)
    if facility.available_gpu_hours < data.required_gpu_hours:
        return fail("COMPUTE.INSUFFICIENT", str(facility.available_gpu_hours))

    return SiteVerdict(facility.facility_id, True, "OK", "", explain("OK"))


def evaluate(data: EvaluationInput) -> dict[str, object]:
    """评估全部候选站点，输出确定性排序的候选与排除解释。"""
    verdicts = [evaluate_site(facility, data) for facility in data.facilities]
    verdicts.sort(key=lambda item: item.facility_id)
    eligible = [verdict.facility_id for verdict in verdicts if verdict.eligible]
    candidates = [verdict.as_dict() for verdict in verdicts]
    excluded = [verdict.as_dict() for verdict in verdicts if not verdict.eligible]
    if eligible:
        state, reason_code, reason_detail = "candidate_ready", None, None
    else:
        state = "blocked"
        reason_code = "NO_ELIGIBLE_SITE"
        global_rule = _global_verdict(data)
        reason_detail = global_rule[0] if global_rule is not None else (
            excluded[0]["rule_code"] if excluded else "FACILITY.UNKNOWN"
        )
    return {
        "state": state,
        "eligible_sites": eligible,
        "candidates": candidates,
        "excluded": excluded,
        "blocked_reason_code": reason_code,
        "blocked_reason_detail": reason_detail,
    }


# --- 最小必要视图 -------------------------------------------------------------

# 各角色在计划视图中可见的字段。租户不见内部库存数值；审计可见完整主体与授权链。
_TENANT_PLAN_FIELDS = (
    "plan_id", "tenant_id", "version_id", "dataset_id", "product",
    "scheduled_start_at", "scheduled_end_at", "state",
    "blocked_reason_code", "blocked_reason_detail",
    "eligible_sites", "candidates", "revision", "created_at", "updated_at",
)
_DISPATCHER_PLAN_FIELDS = (
    "plan_id", "tenant_id", "version_id", "dataset_id", "product",
    "required_gpu_hours", "scheduled_start_at", "scheduled_end_at", "state",
    "blocked_reason_code", "blocked_reason_detail",
    "eligible_sites", "candidates", "facility_id",
    "revision", "submitted_by", "created_at", "updated_at",
)
_AUDITOR_PLAN_FIELDS = (
    "plan_id", "tenant_id", "version_id", "dataset_id", "product",
    "required_gpu_hours", "scheduled_start_at", "scheduled_end_at", "state",
    "blocked_reason_code", "blocked_reason_detail",
    "eligible_sites", "candidates", "facility_id",
    "submitted_by", "created_at", "updated_at", "revision",
    "authorization", "accesses", "manual_dispositions",
)


def _project_candidate(candidate: Mapping[str, object], *, full: bool) -> dict[str, object]:
    # 租户视图不暴露内部库存数值，只保留规则解释。
    if full:
        return dict(candidate)
    return {
        key: candidate[key]
        for key in ("facility_id", "eligible", "rule_code", "rule_detail", "rule_message")
    }


def project_plan_view(
    role: str,
    plan: Mapping[str, object],
    *,
    include_candidates: bool = True,
) -> dict[str, object]:
    """按角色投影作业计划的最小必要视图。

    - tenant：只见本租户计划的合规结论、候选站点与排除原因；
    - dispatcher：额外可见资源需求、指派站点与提交人；
    - auditor：额外可见授权依据、合规访问记录与人工处置记录。
    """
    if role == "tenant":
        fields = _TENANT_PLAN_FIELDS
        full_candidate = False
    elif role == "dispatcher":
        fields = _DISPATCHER_PLAN_FIELDS
        full_candidate = True
    elif role == "auditor":
        fields = _AUDITOR_PLAN_FIELDS
        full_candidate = True
    else:
        raise ValueError(f"未知视图角色: {role}")

    result: dict[str, object] = {}
    for key in fields:
        if key not in plan:
            continue
        value = plan[key]
        if key == "candidates" and not include_candidates:
            continue
        if key in ("candidates",) and isinstance(value, list):
            value = [_project_candidate(item, full=full_candidate) for item in value]
        result[key] = value
    return result


def project_dataset_view(role: str, dataset: Mapping[str, object]) -> dict[str, object]:
    """数据集登记视图：租户看自身登记的版本与地域；调度员看可调度边界；审计看全量。"""
    base_keys = (
        "dataset_id", "name", "owner_tenant_id", "revision", "created_at",
    )
    version_keys = (
        "version_id", "version_tag", "region_scope", "network_boundary",
        "expires_at", "state", "frozen_reason", "created_at",
    )
    if role == "tenant":
        version_keys = version_keys
    elif role == "dispatcher":
        version_keys = tuple(k for k in version_keys if k != "frozen_reason")
    # auditor 额外保留 content_sha256 以核验版本指纹。
    result = {key: dataset[key] for key in base_keys if key in dataset}
    versions = []
    for version in dataset.get("versions", []):
        row = {key: version[key] for key in version_keys if key in version}
        if role == "auditor" and "content_sha256" in version:
            row["content_sha256"] = version["content_sha256"]
        versions.append(row)
    result["versions"] = versions
    if role in ("dispatcher", "auditor"):
        result["active_grants"] = dataset.get("active_grants", 0)
    if role == "auditor":
        result["grants"] = dataset.get("grants", [])
    return result
