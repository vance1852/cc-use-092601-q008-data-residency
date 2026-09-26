"""贯通算力单价、互联通道、算力库存、提名和情景分析的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import SupplyService


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = SupplyService(connection, FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor"),
                          ("comp", "compliance"), ("tenant-east", "tenant")):
        service.create_user(user_id, user_id, role)
    for index, close in enumerate(("108", "105", "102", "100", "98", "96"), start=18):
        service.record_quote("plan", {"market_index": "PEAK_VALLEY", "trade_date": f"2026-09-{index}", "close_cny": close, "source_revision": f"rev-{index}", "observed_at": f"2026-09-{index}T21:00:00Z"})
    service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部数据中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_gpu_hours": "500000", "region": "CN-NORTH", "network_boundary": "priv"})
    service.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_gpu_hours": "800000", "region": "CN-EAST", "network_boundary": "priv"})
    service.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
    service.create_facility("plan", {"facility_id": "edge-sg", "name": "境外边缘站点", "kind": "edge-site", "timezone": "Asia/Singapore", "capacity_gpu_hours": "200000", "region": "SG", "network_boundary": "public"})
    service.add_inventory_lot("dispatch", {"lot_id": "lot-001", "facility_id": "cluster-a", "product": "gpu-h100", "grade": "PEAK_VALLEY", "quantity_gpu_hours": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_nomination("dispatch", {"nomination_id": "nom-001", "route_id": "fabric-a-b", "shipper_id": "tenant-east", "service_date": "2026-09-25", "requested_gpu_hours": "80000", "priority": 10, "idempotency_key": "nom-key-001"})
    allocation = service.allocate("dispatch", "fabric-a-b", "2026-09-25")
    transfer = service.dispatch_transfer("dispatch", "transfer-001", "nom-001", "lot-001", 2)
    service.create_scenario("plan", {"scenario_id": "fabric-recovery", "name": "关键机组检修恢复与需求回落", "market_index_drop_percent": "9", "route_capacity_changes": {"fabric-a-b": "20"}, "demand_changes": {"cluster-a:gpu-h100": "-5"}})
    service.approve_scenario("risk", "fabric-recovery", 1)
    scenario = service.run_scenario("plan", "fabric-recovery", "2026-09-23")

    # 数据集驻留合规与调度联动
    service.register_dataset("comp", {"dataset_id": "ds-train", "name": "跨区域训练集", "owner_tenant_id": "tenant-east", "description": "含地域驻留限制"})
    service.register_version("comp", {"version_id": "ver-train-1", "dataset_id": "ds-train", "version_tag": "v2026.09.24", "content_sha256": "ab" * 32, "region_scope": ["CN-NORTH", "CN-EAST"], "network_boundary": "priv", "expires_at": "2027-09-24T00:00:00Z"})
    service.import_grants("comp", {"idempotency_key": "grant-batch-001", "grants": [
        {"grant_id": "grt-1", "dataset_id": "ds-train", "version_id": "ver-train-1", "subject_id": "tenant-east", "subject_type": "tenant", "region_scope": ["CN-NORTH"], "network_boundary": "priv", "expires_at": "2027-09-24T00:00:00Z"},
        {"grant_id": "grt-2", "dataset_id": "ds-train", "version_id": "ver-train-1", "subject_id": "tenant-east", "subject_type": "tenant", "region_scope": ["CN-EAST"], "network_boundary": "priv", "expires_at": "2027-09-24T00:00:00Z"},
    ]})
    plan = service.submit_plan("tenant-east", {"plan_id": "job-train-1", "tenant_id": "tenant-east", "version_id": "ver-train-1", "product": "gpu-h100", "scheduled_start_at": "2026-09-25T02:00:00Z", "required_gpu_hours": "60000", "idempotency_key": "job-key-001"})
    exclusion = service.explain_exclusion("tenant-east", "job-train-1", "edge-sg")
    service.dispatch_plan("dispatch", "job-train-1", "cluster-a", plan["revision"])
    service.mark_running("dispatch", "job-train-1")
    service.revoke_grant("comp", "grt-1", "合规复核撤回北部授权")
    held_plan = service.get_plan("audit", "job-train-1")
    residency = {
        "plan_state": held_plan["state"],
        "facility_id": held_plan["facility_id"],
        "blocked_rule": held_plan["blocked_reason_code"],
        "candidate_sites": plan["eligible_sites"],
        "excluded_site_rule": exclusion["rule_code"],
        "excluded_site_message": exclusion["rule_message"],
        "compliance_accesses": len(held_plan["accesses"]),
        "manual_dispositions": len(held_plan["manual_dispositions"]),
    }

    result = {"status": "ok", "price": service.price_summary("PEAK_VALLEY"), "allocation_id": allocation["allocation_id"], "transfer": transfer, "scenario_run_id": scenario["run_id"], "residency": residency, "audit": service.audit_chain("audit"), "workspace": workspace.name}
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行数据中心调度服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
