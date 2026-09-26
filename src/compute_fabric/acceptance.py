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
    for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"), ("audit", "auditor"), ("legal", "compliance"), ("tenant-east", "tenant")):
        service.create_user(user_id, user_id, role)
    for index, close in enumerate(("108", "105", "102", "100", "98", "96"), start=18):
        service.record_quote("plan", {"market_index": "PEAK_VALLEY", "trade_date": f"2026-09-{index}", "close_cny": close, "source_revision": f"rev-{index}", "observed_at": f"2026-09-{index}T21:00:00Z"})
    service.create_facility("plan", {"facility_id": "cluster-a", "name": "北部数据中心", "kind": "storage", "timezone": "Asia/Shanghai", "capacity_gpu_hours": "500000", "region": "cn-north", "network_zone": "trusted-core"})
    service.create_facility("plan", {"facility_id": "pool-b", "name": "东部推理池", "kind": "inference-pool", "timezone": "Asia/Shanghai", "capacity_gpu_hours": "800000", "region": "cn-east", "network_zone": "trusted-core"})
    service.create_facility("plan", {"facility_id": "site-overseas", "name": "海外站点", "kind": "edge-site", "timezone": "UTC", "capacity_gpu_hours": "300000", "region": "eu-west", "network_zone": "external"})
    service.create_route("plan", {"route_id": "fabric-a-b", "origin_id": "cluster-a", "destination_id": "pool-b", "product": "gpu-h100", "daily_capacity": "100000", "loss_basis_points": 25, "transit_hours": 36})
    service.add_inventory_lot("dispatch", {"lot_id": "lot-001", "facility_id": "cluster-a", "product": "gpu-h100", "grade": "PEAK_VALLEY", "quantity_gpu_hours": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.add_inventory_lot("dispatch", {"lot_id": "lot-002", "facility_id": "site-overseas", "product": "gpu-h100", "grade": "PEAK_VALLEY", "quantity_gpu_hours": "150000", "unit_cost_cny": "91.25", "received_at": "2026-09-24T06:00:00Z"})
    service.submit_nomination("dispatch", {"nomination_id": "nom-001", "route_id": "fabric-a-b", "shipper_id": "tenant-east", "service_date": "2026-09-25", "requested_gpu_hours": "80000", "priority": 10, "idempotency_key": "nom-key-001"})
    allocation = service.allocate("dispatch", "fabric-a-b", "2026-09-25")
    transfer = service.dispatch_transfer("dispatch", "transfer-001", "nom-001", "lot-001", 2)
    service.create_scenario("plan", {"scenario_id": "fabric-recovery", "name": "关键机组检修恢复与需求回落", "market_index_drop_percent": "9", "route_capacity_changes": {"fabric-a-b": "20"}, "demand_changes": {"cluster-a:gpu-h100": "-5"}})
    service.approve_scenario("risk", "fabric-recovery", 1)
    scenario = service.run_scenario("plan", "fabric-recovery", "2026-09-23")

    # 数据集驻留合规与调度联动：登记确定版本与境内驻留范围，批量导入主体授权。
    service.register_dataset("legal", {
        "dataset_id": "ds-imaging", "version": "v3", "name": "跨院区影像训练集",
        "regions": ["cn-north", "cn-east"], "content_sha256": "f" * 64,
        "registered_at": "2026-09-20T00:00:00Z",
    })
    service.import_authorizations("legal", {
        "batch_id": "grant-batch-001",
        "grants": [{
            "dataset_id": "ds-imaging", "version": "v3", "subject_id": "tenant-east",
            "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z",
        }],
    })
    plan = service.submit_training_plan("tenant-east", {
        "plan_id": "plan-001", "dataset_id": "ds-imaging", "version": "v3",
        "subject_id": "tenant-east", "product": "gpu-h100",
        "requested_gpu_hours": "20000", "network_zone": "trusted-core",
        "idempotency_key": "plan-key-001",
    })
    launched = service.launch_plan("dispatch", "plan-001", plan["candidate_sites"][0])
    service.mark_plan_running("dispatch", "plan-001")
    # 第二个尚未启动的计划用于演示撤回立即阻断。
    queued = service.submit_training_plan("tenant-east", {
        "plan_id": "plan-002", "dataset_id": "ds-imaging", "version": "v3",
        "subject_id": "tenant-east", "product": "gpu-h100",
        "requested_gpu_hours": "10000", "network_zone": "trusted-core",
        "idempotency_key": "plan-key-002",
    })
    withdrawal = service.revoke_authorization("legal", "ds-imaging", "v3", "tenant-east", "主体合作终止")
    tenant_view = service.tenant_plan_view("tenant-east", "plan-001")
    auditor_view = service.auditor_compliance_view("audit", "plan-001")
    compliance = {
        "candidate_sites": plan["candidate_sites"],
        "excluded_site_rules": [
            {"facility_id": item["facility_id"], "failed_rules": [rule["rule"] for rule in item["rules"] if not rule["passed"]]}
            for item in plan["site_evaluations"] if not item["eligible"]
        ],
        "launched_site": launched["selected_site"],
        "withdrawn_blocked_plans": withdrawal["blocked_plans"],
        "withdrawn_manual_plans": withdrawal["manual_disposition_plans"],
        "queued_plan_state": service.dispatcher_plan_view("dispatch", "plan-002")["state"],
        "tenant_candidate_sites": tenant_view["candidate_sites"],
        "auditor_access_events": [row["event_type"] for row in auditor_view["access_records"]],
    }
    result = {"status": "ok", "price": service.price_summary("PEAK_VALLEY"), "allocation_id": allocation["allocation_id"], "transfer": transfer, "scenario_run_id": scenario["run_id"], "compliance": compliance, "audit": service.audit_chain("audit"), "workspace": workspace.name}
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
