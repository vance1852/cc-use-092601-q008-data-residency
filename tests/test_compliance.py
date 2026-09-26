from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone

from compute_fabric.api import JsonApplication
from compute_fabric.clock import FrozenClock
from compute_fabric.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from compute_fabric.service import SupplyService


def facility(facility_id: str, region: str, zone: str, *, kind: str = "storage") -> dict[str, object]:
    return {
        "facility_id": facility_id,
        "name": facility_id,
        "kind": kind,
        "timezone": "Asia/Shanghai",
        "capacity_gpu_hours": "500000",
        "region": region,
        "network_zone": zone,
    }


def lot(lot_id: str, facility_id: str, quantity: str = "100000") -> dict[str, object]:
    return {
        "lot_id": lot_id,
        "facility_id": facility_id,
        "product": "gpu-h100",
        "grade": "PEAK",
        "quantity_gpu_hours": quantity,
        "unit_cost_cny": "90",
        "received_at": "2026-09-20T06:00:00Z",
    }


class ComplianceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for user_id, role in (
            ("plan", "planner"),
            ("dispatch", "dispatcher"),
            ("risk", "risk"),
            ("audit", "auditor"),
            ("legal", "compliance"),
            ("tenant-acme", "tenant"),
        ):
            self.service.create_user(user_id, user_id, role)
        # 三个站点：cn-north 与 cn-east 在数据集地域内，eu-west 不在。
        self.service.create_facility("plan", facility("site-cn-north", "cn-north", "zone-a"))
        self.service.create_facility("plan", facility("site-cn-east", "cn-east", "zone-a"))
        self.service.create_facility("plan", facility("site-eu-west", "eu-west", "zone-a"))
        self.service.add_inventory_lot("dispatch", lot("lot-n", "site-cn-north"))
        self.service.add_inventory_lot("dispatch", lot("lot-e", "site-cn-east"))
        self.service.add_inventory_lot("dispatch", lot("lot-w", "site-eu-west"))
        self.dataset = {
            "dataset_id": "ds-medical",
            "version": "v3",
            "name": "跨院区影像集",
            "regions": ["cn-north", "cn-east"],
            "content_sha256": "a" * 64,
            "registered_at": "2026-09-20T00:00:00Z",
        }
        self.service.register_dataset("legal", self.dataset)

    def tearDown(self) -> None:
        self.connection.close()

    def grant(self, *, subject: str = "tenant-acme", expires: str = "2026-12-31T00:00:00Z", batch: str = "batch-1") -> dict[str, object]:
        return self.service.import_authorizations("legal", {
            "batch_id": batch,
            "grants": [{
                "dataset_id": "ds-medical",
                "version": "v3",
                "subject_id": subject,
                "granted_at": "2026-09-20T00:00:00Z",
                "expires_at": expires,
            }],
        })

    def plan_payload(self, key: str = "plan-key-1", *, zone: str = "zone-a", hours: str = "20000") -> dict[str, object]:
        return {
            "plan_id": f"plan-{key}",
            "dataset_id": "ds-medical",
            "version": "v3",
            "subject_id": "tenant-acme",
            "product": "gpu-h100",
            "requested_gpu_hours": hours,
            "network_zone": zone,
            "idempotency_key": key,
        }

    def test_registered_dataset_is_a_fixed_version(self) -> None:
        view = self.service.register_dataset("legal", {
            **self.dataset, "version": "v4", "content_sha256": "b" * 64
        })
        self.assertEqual(view["version"], "v4")
        self.assertEqual(view["regions"], ["cn-north", "cn-east"])
        with self.assertRaises(Conflict):
            self.service.register_dataset("legal", self.dataset)
        with self.assertRaises(Forbidden):
            self.service.register_dataset("dispatch", {**self.dataset, "version": "v9"})

    def test_plan_must_reference_existing_fixed_version(self) -> None:
        self.grant()
        with self.assertRaises(NotFound):
            self.service.submit_training_plan(
                "tenant-acme", self.plan_payload("k1") | {"version": "v-ghost"}
            )

    def test_candidates_only_at_sites_meeting_all_boundaries(self) -> None:
        self.grant()
        result = self.service.submit_training_plan("tenant-acme", self.plan_payload())
        self.assertEqual(result["state"], "ready")
        self.assertEqual(result["candidate_sites"], ["site-cn-east", "site-cn-north"])
        by_site = {item["facility_id"]: item for item in result["site_evaluations"]}
        excluded = by_site["site-eu-west"]
        self.assertFalse(excluded["eligible"])
        failed = {rule["rule"]: rule["detail"] for rule in excluded["rules"] if not rule["passed"]}
        self.assertEqual(set(failed), {"data.region"})
        self.assertIn("eu-west", failed["data.region"])
        self.assertIn("cn-east", failed["data.region"])

    def test_network_boundary_excludes_site(self) -> None:
        self.grant()
        result = self.service.submit_training_plan(
            "tenant-acme", self.plan_payload("k2", zone="zone-b")
        )
        self.assertEqual(result["state"], "blocked")
        self.assertEqual(result["candidate_sites"], [])
        for evaluation in result["site_evaluations"]:
            failed = {rule["rule"] for rule in evaluation["rules"] if not rule["passed"]}
            self.assertIn("network.zone", failed)

    def test_compute_capacity_explains_exclusion(self) -> None:
        self.grant()
        result = self.service.submit_training_plan(
            "tenant-acme", self.plan_payload("k3", hours="900000")
        )
        self.assertEqual(result["blocked_reason"], "no_eligible_site")
        for evaluation in result["site_evaluations"]:
            failed = {rule["rule"] for rule in evaluation["rules"] if not rule["passed"]}
            self.assertIn("compute.capacity", failed)

    def test_missing_and_expired_authorization_blocks_plan(self) -> None:
        result = self.service.submit_training_plan("tenant-acme", self.plan_payload("k4"))
        self.assertEqual(result["state"], "blocked")
        self.assertEqual(result["blocked_reason"], "authorization_missing")
        self.grant(expires="2026-09-24T07:59:59Z", batch="batch-exp")
        expired = self.service.submit_training_plan("tenant-acme", self.plan_payload("k5"))
        self.assertEqual(expired["blocked_reason"], "authorization_expired")

    def test_batch_import_is_all_or_nothing(self) -> None:
        payload = {
            "batch_id": "batch-atomic",
            "grants": [
                {"dataset_id": "ds-medical", "version": "v3", "subject_id": "s-ok",
                 "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z"},
                {"dataset_id": "ds-missing", "version": "v1", "subject_id": "s-bad",
                 "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z"},
            ],
        }
        with self.assertRaises(NotFound):
            self.service.import_authorizations("legal", payload)
        count = self.connection.execute("SELECT count(*) FROM dataset_authorizations").fetchone()[0]
        self.assertEqual(count, 0)

    def test_batch_import_replays_stably_and_conflicts_on_changed_payload(self) -> None:
        first = self.grant(batch="batch-stable")
        second = self.grant(batch="batch-stable")
        self.assertEqual(first, second)
        changed = {
            "batch_id": "batch-stable",
            "grants": [{
                "dataset_id": "ds-medical",
                "version": "v3",
                "subject_id": "tenant-acme",
                "granted_at": "2026-09-20T00:00:00Z",
                "expires_at": "2027-01-31T00:00:00Z",
            }],
        }
        with self.assertRaises(Conflict):
            self.service.import_authorizations("legal", changed)
        with self.assertRaises(ValidationFailed):
            self.service.import_authorizations("legal", {
                "batch_id": "batch-dup",
                "grants": [
                    {"dataset_id": "ds-medical", "version": "v3", "subject_id": "dup",
                     "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z"},
                    {"dataset_id": "ds-medical", "version": "v3", "subject_id": "dup",
                     "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z"},
                ],
            })

    def test_plan_idempotency_replay_is_stable(self) -> None:
        self.grant()
        payload = self.plan_payload()
        first = self.service.submit_training_plan("tenant-acme", payload)
        second = self.service.submit_training_plan("tenant-acme", payload)
        self.assertEqual(first, second)
        with self.assertRaises(Conflict):
            self.service.submit_training_plan(
                "tenant-acme", payload | {"requested_gpu_hours": "21000"}
            )

    def test_withdrawal_blocks_unstarted_plan_but_keeps_access_history(self) -> None:
        self.grant()
        plan = self.service.submit_training_plan("tenant-acme", self.plan_payload())
        self.assertEqual(plan["state"], "ready")
        outcome = self.service.revoke_authorization(
            "legal", "ds-medical", "v3", "tenant-acme", "主体合作终止"
        )
        self.assertEqual(outcome["blocked_plans"], ["plan-plan-key-1"])
        self.assertEqual(outcome["manual_disposition_plans"], [])
        stored = self.connection.execute(
            "SELECT state,blocked_reason FROM training_plans WHERE plan_id='plan-plan-key-1'"
        ).fetchone()
        self.assertEqual(stored["state"], "cancelled")
        records = self.connection.execute(
            "SELECT event_type FROM compliance_access_records WHERE plan_id='plan-plan-key-1' ORDER BY access_id"
        ).fetchall()
        self.assertEqual(
            [row[0] for row in records],
            ["plan.submitted", "authorization.withdrawn_block"],
        )
        # 撤回后无法再启动
        with self.assertRaises(InvalidState):
            self.service.launch_plan("dispatch", "plan-plan-key-1", "site-cn-north")

    def test_withdrawal_while_running_requires_manual_disposition_no_silent_migration(self) -> None:
        self.grant()
        self.service.submit_training_plan("tenant-acme", self.plan_payload())
        self.service.launch_plan("dispatch", "plan-plan-key-1", "site-cn-north")
        self.service.mark_plan_running("dispatch", "plan-plan-key-1")
        outcome = self.service.revoke_authorization(
            "legal", "ds-medical", "v3", "tenant-acme", "主体撤回授权"
        )
        self.assertEqual(outcome["blocked_plans"], [])
        self.assertEqual(outcome["manual_disposition_plans"], ["plan-plan-key-1"])
        row = self.connection.execute(
            "SELECT state,selected_site FROM training_plans WHERE plan_id='plan-plan-key-1'"
        ).fetchone()
        self.assertEqual(row["state"], "blocked_running")
        # 站点保持不变，系统没有静默迁移
        self.assertEqual(row["selected_site"], "site-cn-north")
        with self.assertRaises(InvalidState):
            self.service.launch_plan("dispatch", "plan-plan-key-1", "site-cn-east")
        with self.assertRaises(Forbidden):
            self.service.dispose_running_plan("dispatch", "plan-plan-key-1", "halt", "调度员不能自行处置")
        disposed = self.service.dispose_running_plan(
            "legal", "plan-plan-key-1", "halt", "立即终止并封存现场"
        )
        self.assertEqual(disposed["state"], "halted")
        self.assertEqual(disposed["selected_site"], "site-cn-north")
        events = [
            row[0] for row in self.connection.execute(
                "SELECT event_type FROM compliance_access_records WHERE plan_id='plan-plan-key-1' ORDER BY access_id"
            ).fetchall()
        ]
        self.assertEqual(
            events,
            ["plan.submitted", "plan.launching", "plan.ran",
             "authorization.withdrawn_manual", "plan.manual_disposition"],
        )

    def test_launch_revalidates_and_rejects_non_candidate_site(self) -> None:
        self.grant()
        self.service.submit_training_plan("tenant-acme", self.plan_payload())
        with self.assertRaises(InvalidState):
            self.service.launch_plan("dispatch", "plan-plan-key-1", "site-eu-west")
        self.service.launch_plan("dispatch", "plan-plan-key-1", "site-cn-north")
        self.service.mark_plan_running("dispatch", "plan-plan-key-1")
        # 授权在运行期间撤回
        self.service.revoke_authorization("legal", "ds-medical", "v3", "tenant-acme", "撤回")

    def test_three_minimum_views_and_role_separation(self) -> None:
        self.grant()
        self.service.submit_training_plan("tenant-acme", self.plan_payload())
        plan_id = "plan-plan-key-1"

        tenant_view = self.service.tenant_plan_view("tenant-acme", plan_id)
        self.assertEqual(tenant_view["candidate_sites"], ["site-cn-east", "site-cn-north"])
        explanation = {item["facility_id"]: item for item in tenant_view["site_explanations"]}
        self.assertEqual(explanation["site-eu-west"]["failed_rules"], ["data.region"])
        self.assertNotIn("network_zone", tenant_view)
        self.assertNotIn("authorization", tenant_view)
        with self.assertRaises(Forbidden):
            self.service.tenant_plan_view("dispatch", plan_id)

        dispatcher_view = self.service.dispatcher_plan_view("dispatch", plan_id)
        self.assertIn("site_evaluations", dispatcher_view)
        self.assertEqual(dispatcher_view["subject_id"], "tenant-acme")
        with self.assertRaises(Forbidden):
            self.service.dispatcher_plan_view("tenant-acme", plan_id)

        auditor_view = self.service.auditor_compliance_view("audit", plan_id)
        self.assertEqual(auditor_view["dataset"]["content_sha256"], "a" * 64)
        self.assertEqual(auditor_view["authorization"]["state"], "granted")
        self.assertEqual([row["event_type"] for row in auditor_view["access_records"]], ["plan.submitted"])
        with self.assertRaises(Forbidden):
            self.service.auditor_compliance_view("dispatch", plan_id)

    def test_auditor_sees_withdrawn_authorization_and_prior_access_preserved(self) -> None:
        self.grant()
        self.service.submit_training_plan("tenant-acme", self.plan_payload())
        self.service.launch_plan("dispatch", "plan-plan-key-1", "site-cn-north")
        self.service.mark_plan_running("dispatch", "plan-plan-key-1")
        self.service.revoke_authorization("legal", "ds-medical", "v3", "tenant-acme", "撤回")
        view = self.service.auditor_compliance_view("audit", "plan-plan-key-1")
        self.assertEqual(view["authorization"]["state"], "withdrawn")
        self.assertEqual(view["authorization"]["withdraw_reason"], "撤回")
        self.assertIn("plan.ran", [row["event_type"] for row in view["access_records"]])

    def test_tenant_cannot_register_dataset_or_submit_for_other_subject(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.register_dataset("tenant-acme", {**self.dataset, "version": "vX"})
        payload = self.plan_payload("k9")
        payload["subject_id"] = "tenant-other"
        with self.assertRaises(Forbidden):
            self.service.submit_training_plan("tenant-acme", payload)


class ComplianceApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        self.app = JsonApplication(self.service)
        for user_id, role in (("plan", "planner"), ("dispatch", "dispatcher"),
                              ("legal", "compliance"), ("audit", "auditor"),
                              ("tenant-acme", "tenant")):
            self.service.create_user(user_id, user_id, role)
        self.service.create_facility("plan", facility("site-cn-north", "cn-north", "zone-a"))
        self.service.add_inventory_lot("dispatch", lot("lot-n", "site-cn-north"))
        self.service.register_dataset("legal", {
            "dataset_id": "ds-medical", "version": "v3", "name": "影像集",
            "regions": ["cn-north"], "content_sha256": "a" * 64,
            "registered_at": "2026-09-20T00:00:00Z",
        })

    def tearDown(self) -> None:
        self.connection.close()

    def test_api_flow_through_http_boundary(self) -> None:
        def call(method: str, path: str, actor: str, payload: dict[str, object] | None = None):
            body = json.dumps(payload).encode("utf-8") if payload is not None else b""
            headers = {"X-Actor-Id": actor, "Content-Type": "application/json"}
            return self.app.handle(method, path, headers, body)

        imported = call("POST", "/authorizations/import", "legal", {
            "batch_id": "b1",
            "grants": [{"dataset_id": "ds-medical", "version": "v3", "subject_id": "tenant-acme",
                        "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z"}],
        })
        self.assertEqual(imported.status, 201)
        replay = call("POST", "/authorizations/import", "legal", {
            "batch_id": "b1",
            "grants": [{"dataset_id": "ds-medical", "version": "v3", "subject_id": "tenant-acme",
                        "granted_at": "2026-09-20T00:00:00Z", "expires_at": "2026-12-31T00:00:00Z"}],
        })
        self.assertEqual(replay.status, 201)
        self.assertEqual(replay.body, imported.body)

        plan = call("POST", "/training/plans", "tenant-acme", {
            "plan_id": "plan-1", "dataset_id": "ds-medical", "version": "v3",
            "subject_id": "tenant-acme", "product": "gpu-h100",
            "requested_gpu_hours": "20000", "network_zone": "zone-a",
            "idempotency_key": "pk1",
        })
        self.assertEqual(plan.status, 201)
        self.assertEqual(plan.body["candidate_sites"], ["site-cn-north"])

        tenant = call("GET", "/training/plans/plan-1/tenant", "tenant-acme")
        dispatcher = call("GET", "/training/plans/plan-1/dispatcher", "dispatch")
        auditor = call("GET", "/training/plans/plan-1/audit", "audit")
        self.assertEqual(tenant.status, dispatcher.status, auditor.status)
        self.assertEqual(tenant.status, 200)

        revoked = call("POST", "/authorizations/revoke", "legal", {
            "dataset_id": "ds-medical", "version": "v3", "subject_id": "tenant-acme",
            "reason": "主体撤回",
        })
        self.assertEqual(revoked.status, 200)
        self.assertEqual(revoked.body["blocked_plans"], ["plan-1"])

    def test_legacy_database_without_boundary_columns_still_boots(self) -> None:
        legacy = sqlite3.connect(":memory:", isolation_level=None)
        legacy.row_factory = sqlite3.Row
        legacy.executescript(
            """
            CREATE TABLE supply_users (
                user_id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
                role TEXT NOT NULL CHECK(role IN ('planner','dispatcher','risk','auditor')),
                active INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL
            );
            INSERT INTO supply_users VALUES('plan','plan','planner',1,'2026-01-01T00:00:00Z');
            """
        )
        service = SupplyService(legacy, self.clock)
        service.create_user("legal", "legal", "compliance")
        roles = {row[0]: row[1] for row in legacy.execute("SELECT user_id,role FROM supply_users")}
        self.assertEqual(roles["legal"], "compliance")
        self.assertEqual(roles["plan"], "planner")
        legacy.close()


if __name__ == "__main__":
    unittest.main()
