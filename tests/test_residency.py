from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone
from decimal import Decimal

from compute_fabric.api import JsonApplication
from compute_fabric.clock import FrozenClock
from compute_fabric.errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from compute_fabric.residency import (
    EvaluationInput,
    FacilitySnapshot,
    GrantSnapshot,
    VersionSnapshot,
    evaluate,
    explain,
    project_plan_view,
)
from compute_fabric.service import SupplyService


SHA = "a" * 64
PRODUCT = "gpu-h100"


def _facility(fid, region, network="priv", *, active=True, product=PRODUCT, qty="600"):
    return FacilitySnapshot(fid, region, network, active, frozenset({product}) if qty != "0" else frozenset(), Decimal(qty))


def _version(regions=("CN-NORTH", "CN-EAST"), network="priv", *, state="registered", expires=None):
    return VersionSnapshot("ver-1", "ds-1", frozenset(regions), network, expires, state, None)


def _grant(regions=("CN-NORTH",), network="priv", *, subject="tenant-a", stype="tenant", state="active", expires=None, gid="g1"):
    return GrantSnapshot(gid, subject, stype, frozenset(regions), network, expires, state)


def _input(version=None, grants=(), facilities=(), *, product=PRODUCT, required="100", as_of="2026-09-24T08:00:00Z"):
    return EvaluationInput("tenant-a", version, list(grants), list(facilities), product, Decimal(required), as_of)


class RuleEngineTests(unittest.TestCase):
    def test_site_passes_all_boundaries(self) -> None:
        result = evaluate(_input(
            _version(), [_grant()], [_facility("site-n", "CN-NORTH")]))
        self.assertEqual(result["state"], "candidate_ready")
        self.assertEqual(result["eligible_sites"], ["site-n"])
        self.assertEqual(result["candidates"][0]["rule_code"], "OK")

    def test_data_residency_region_excludes_site(self) -> None:
        result = evaluate(_input(
            _version(regions=("CN-NORTH",)),
            [_grant(regions=("CN-EAST",))],
            [_facility("site-e", "CN-EAST")],
        ))
        self.assertEqual(result["eligible_sites"], [])
        self.assertEqual(result["candidates"][0]["rule_code"], "DATA.REGION_EXCLUDED")
        self.assertIn("CN-EAST", result["candidates"][0]["rule_message"])

    def test_network_boundary_mismatch_excludes(self) -> None:
        result = evaluate(_input(
            _version(network="priv"),
            [_grant(network="priv")],
            [_facility("site-x", "CN-NORTH", network="public")],
        ))
        self.assertEqual(result["candidates"][0]["rule_code"], "NETWORK.BOUNDARY_MISMATCH")

    def test_grant_region_and_revocation_exclude(self) -> None:
        # 版本允许 CN-EAST，但授权只覆盖 CN-NORTH -> 授权地域排除
        result = evaluate(_input(
            _version(regions=("CN-NORTH", "CN-EAST")),
            [_grant(regions=("CN-NORTH",))],
            [_facility("site-e", "CN-EAST")],
        ))
        self.assertEqual(result["candidates"][0]["rule_code"], "GRANT.REGION_EXCLUDED")
        # 撤回授权 -> 撤回规则优先
        result = evaluate(_input(
            _version(), [_grant(state="revoked")], [_facility("site-n", "CN-NORTH")]))
        self.assertEqual(result["candidates"][0]["rule_code"], "GRANT.REVOKED")

    def test_missing_grant_and_expiry(self) -> None:
        result = evaluate(_input(
            _version(), [], [_facility("site-n", "CN-NORTH")]))
        self.assertEqual(result["candidates"][0]["rule_code"], "GRANT.NONE")
        result = evaluate(_input(
            _version(), [_grant(expires="2026-01-01T00:00:00Z")],
            [_facility("site-n", "CN-NORTH")]))
        self.assertEqual(result["candidates"][0]["rule_code"], "GRANT.EXPIRED")

    def test_compute_boundary_excludes(self) -> None:
        result = evaluate(_input(
            _version(), [_grant()], [_facility("site-n", "CN-NORTH", qty="50")], required="100"))
        self.assertEqual(result["candidates"][0]["rule_code"], "COMPUTE.INSUFFICIENT")
        result = evaluate(_input(
            _version(), [_grant()], [_facility("site-n", "CN-NORTH", product="cpu-highmem", qty="600")],
            product=PRODUCT))
        self.assertEqual(result["candidates"][0]["rule_code"], "COMPUTE.PRODUCT_UNAVAILABLE")

    def test_version_global_states_block_every_site(self) -> None:
        for state, code in ((None, "DATA.VERSION_UNKNOWN"), ):
            result = evaluate(_input(state and _version() or None, [_grant()],
                                     [_facility("a", "CN-NORTH"), _facility("b", "CN-EAST")]))
            self.assertTrue(all(c["rule_code"] == code for c in result["candidates"]))
        frozen = evaluate(_input(_version(state="frozen"), [_grant()], [_facility("a", "CN-NORTH")]))
        self.assertEqual(frozen["candidates"][0]["rule_code"], "DATA.FROZEN")
        expired = evaluate(_input(_version(expires="2026-01-01T00:00:00Z"), [_grant()],
                                  [_facility("a", "CN-NORTH")]))
        self.assertEqual(expired["candidates"][0]["rule_code"], "DATA.EXPIRED")

    def test_explain_has_human_message(self) -> None:
        self.assertIn("撤回", explain("GRANT.REVOKED"))
        self.assertIn("CN-NORTH", explain("GRANT.REGION_EXCLUDED", "CN-NORTH"))

    def test_projection_is_minimum_per_role(self) -> None:
        plan = {"plan_id": "p", "tenant_id": "t", "version_id": "v", "dataset_id": "d",
                "product": PRODUCT, "required_gpu_hours": "100", "state": "blocked",
                "scheduled_start_at": "2026-09-25T00:00:00Z", "scheduled_end_at": None,
                "blocked_reason_code": "NO_ELIGIBLE_SITE", "blocked_reason_detail": "GRANT.NONE",
                "eligible_sites": [], "candidates": [], "facility_id": None,
                "revision": 1, "submitted_by": "t", "created_at": "x", "updated_at": "x",
                "authorization": {}, "accesses": [], "manual_dispositions": []}
        tenant = project_plan_view("tenant", plan)
        self.assertNotIn("required_gpu_hours", tenant)
        self.assertNotIn("accesses", tenant)
        auditor = project_plan_view("auditor", plan)
        self.assertIn("accesses", auditor)
        self.assertIn("authorization", auditor)
        dispatcher = project_plan_view("dispatcher", plan)
        self.assertIn("required_gpu_hours", dispatcher)
        self.assertNotIn("accesses", dispatcher)


class ServiceResidencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 24, 8, 0, tzinfo=timezone.utc))
        self.service = SupplyService(self.connection, self.clock)
        for uid, role in (("plan", "planner"), ("dispatch", "dispatcher"), ("risk", "risk"),
                          ("audit", "auditor"), ("comp", "compliance"),
                          ("tenant-a", "tenant"), ("tenant-b", "tenant")):
            self.service.create_user(uid, uid, role)
        for fid, region, network in (("site-n", "CN-NORTH", "priv"),
                                     ("site-e", "CN-EAST", "priv"),
                                     ("site-sg", "SG", "public")):
            self.service.create_facility("plan", {
                "facility_id": fid, "name": fid, "kind": "interconnect",
                "timezone": "Asia/Shanghai", "capacity_gpu_hours": "1",
                "region": region, "network_boundary": network})
            self.service.add_inventory_lot("dispatch", {
                "lot_id": f"lot-{fid}", "facility_id": fid, "product": PRODUCT,
                "grade": "A", "quantity_gpu_hours": "600", "unit_cost_cny": "90",
                "received_at": "2026-09-24T06:00:00Z"})
        self.service.register_dataset("comp", {
            "dataset_id": "ds-1", "name": "训练集", "owner_tenant_id": "tenant-a", "description": ""})
        self.service.register_version("comp", {
            "version_id": "ver-1", "dataset_id": "ds-1", "version_tag": "v1",
            "content_sha256": SHA, "region_scope": ["CN-NORTH", "CN-EAST"],
            "network_boundary": "priv", "expires_at": "2026-12-31T00:00:00Z"})

    def tearDown(self) -> None:
        self.connection.close()

    def _grant(self, gid, regions, *, subject="tenant-a"):
        self.service.import_grants("comp", {"idempotency_key": f"key-{gid}", "grants": [{
            "grant_id": gid, "dataset_id": "ds-1", "version_id": "ver-1",
            "subject_id": subject, "subject_type": "tenant",
            "region_scope": regions, "network_boundary": "priv",
            "expires_at": "2026-12-31T00:00:00Z"}]})

    def _plan(self, plan_id="job-1", *, tenant="tenant-a", version="ver-1", idem=None, start="2026-09-25T01:00:00Z"):
        return self.service.submit_plan(tenant, {
            "plan_id": plan_id, "tenant_id": tenant, "version_id": version,
            "product": PRODUCT, "scheduled_start_at": start,
            "required_gpu_hours": "100", "idempotency_key": idem or f"idem-{plan_id}"})

    def test_plan_must_reference_registered_version(self) -> None:
        with self.assertRaises(NotFound):
            self._plan("job-x", version="ver-missing")

    def test_candidates_require_all_three_boundaries(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        plan = self._plan()
        self.assertEqual(plan["state"], "candidate_ready")
        self.assertEqual(plan["eligible_sites"], ["site-n"])
        by_site = {c["facility_id"]: c for c in plan["candidates"]}
        self.assertTrue(by_site["site-n"]["eligible"])
        self.assertEqual(by_site["site-e"]["rule_code"], "GRANT.REGION_EXCLUDED")
        self.assertEqual(by_site["site-sg"]["rule_code"], "DATA.REGION_EXCLUDED")

    def test_no_grant_blocks_plan(self) -> None:
        plan = self._plan()
        self.assertEqual(plan["state"], "blocked")
        self.assertEqual(plan["eligible_sites"], [])

    def test_batch_import_is_all_or_nothing(self) -> None:
        before = self.connection.execute("SELECT count(*) c FROM dataset_grants").fetchone()["c"]
        with self.assertRaises(NotFound):
            self.service.import_grants("comp", {"idempotency_key": "batch-bad", "grants": [
                {"grant_id": "g-ok", "dataset_id": "ds-1", "version_id": "ver-1",
                 "subject_id": "tenant-a", "subject_type": "tenant",
                 "region_scope": ["CN-NORTH"], "network_boundary": "priv"},
                {"grant_id": "g-bad", "dataset_id": "missing", "subject_id": "tenant-a",
                 "subject_type": "tenant", "region_scope": ["CN-NORTH"], "network_boundary": "priv"}]})
        after = self.connection.execute("SELECT count(*) c FROM dataset_grants").fetchone()["c"]
        self.assertEqual(before, after)

    def test_batch_import_replay_is_stable(self) -> None:
        payload = {"idempotency_key": "batch-1", "grants": [
            {"grant_id": "g1", "dataset_id": "ds-1", "version_id": "ver-1",
             "subject_id": "tenant-a", "subject_type": "tenant",
             "region_scope": ["CN-NORTH"], "network_boundary": "priv",
             "expires_at": "2026-12-31T00:00:00Z"},
            {"grant_id": "g2", "dataset_id": "ds-1", "version_id": "ver-1",
             "subject_id": "tenant-a", "subject_type": "tenant",
             "region_scope": ["CN-EAST"], "network_boundary": "priv",
             "expires_at": "2026-12-31T00:00:00Z"}]}
        first = self.service.import_grants("comp", payload)
        second = self.service.import_grants("comp", payload)
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["grant_ids"], second["grant_ids"])
        self.assertEqual(
            self.connection.execute("SELECT count(*) c FROM dataset_grants").fetchone()["c"], 2)
        changed = dict(payload)
        changed["grants"] = [dict(g, grant_id="g3") for g in payload["grants"]]
        with self.assertRaises(Conflict):
            self.service.import_grants("comp", changed)

    def test_plan_replay_is_stable(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        payload = {"plan_id": "job-1", "tenant_id": "tenant-a", "version_id": "ver-1",
                   "product": PRODUCT, "scheduled_start_at": "2026-09-25T01:00:00Z",
                   "required_gpu_hours": "100", "idempotency_key": "idem-1"}
        first = self.service.submit_plan("tenant-a", payload)
        second = self.service.submit_plan("tenant-a", payload)
        self.assertEqual(first, second)

    def test_revoke_blocks_only_unscheduled_plan_when_no_site_left(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        self._grant("g-e", ["CN-EAST"])
        plan = self._plan()
        self.assertEqual(plan["eligible_sites"], ["site-e", "site-n"])
        # 撤回北区授权：未启动计划仍可在东区派发
        self.service.revoke_grant("comp", "g-n", "撤回北区")
        view = self.service.get_plan("dispatch", "job-1")
        self.assertEqual(view["state"], "candidate_ready")
        self.assertEqual(view["eligible_sites"], ["site-e"])
        # 撤回最后一条：未启动计划立即 blocked
        self.service.revoke_grant("comp", "g-e", "撤回东区")
        view = self.service.get_plan("dispatch", "job-1")
        self.assertEqual(view["state"], "blocked")

    def test_revoke_running_job_enters_manual_hold_without_migration(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        plan = self._plan()
        self.service.dispatch_plan("dispatch", "job-1", "site-n", plan["revision"])
        self.service.mark_running("dispatch", "job-1")
        self.service.revoke_grant("comp", "g-n", "合规撤回")
        view = self.service.get_plan("audit", "job-1")
        self.assertEqual(view["state"], "manual_hold")
        self.assertEqual(view["facility_id"], "site-n")  # 未迁移
        self.assertEqual(view["blocked_reason_code"], "GRANT.REVOKED")
        # 已发生的合规访问不被抹除
        kinds = [a["access_kind"] for a in view["accesses"]]
        self.assertIn("dispatch", kinds)
        self.assertIn("run_start", kinds)
        dispositions = view["manual_dispositions"]
        self.assertTrue(any(d["action"] == "hold" for d in dispositions))

    def test_revoke_does_not_hold_running_job_still_covered(self) -> None:
        # 两条授权均覆盖北区；撤回其中一条，作业仍合规运行
        self._grant("g-n", ["CN-NORTH"])
        self._grant("g-n2", ["CN-NORTH", "CN-EAST"])
        plan = self._plan()
        self.service.dispatch_plan("dispatch", "job-1", "site-n", plan["revision"])
        self.service.mark_running("dispatch", "job-1")
        self.service.revoke_grant("comp", "g-n", "撤回其中一条")
        self.assertEqual(self.service.get_plan("dispatch", "job-1")["state"], "running")

    def test_resume_denied_while_noncompliant_then_allowed(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        plan = self._plan()
        self.service.dispatch_plan("dispatch", "job-1", "site-n", plan["revision"])
        self.service.mark_running("dispatch", "job-1")
        self.service.revoke_grant("comp", "g-n", "撤回")
        denied = self.service.dispose_plan("comp", "job-1", "resume", "恢复")
        # resume 被拒绝：状态保持 manual_hold，并留痕
        self.assertEqual(denied["state"], "manual_hold")
        self.assertEqual(denied["facility_id"], "site-n")
        # 恢复授权（重新导入覆盖授权）后 resume 成功
        self._grant("g-restored", ["CN-NORTH"])
        result = self.service.dispose_plan("comp", "job-1", "resume", "授权恢复，继续运行")
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["facility_id"], "site-n")

    def test_cannot_dispatch_to_excluded_site(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        plan = self._plan()
        with self.assertRaises(InvalidState):
            self.service.dispatch_plan("dispatch", "job-1", "site-sg", plan["revision"])

    def test_dispatch_rejects_stale_revision(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        plan = self._plan()
        self.service.revoke_grant  # no-op reference
        # 重新评估推进 revision
        self.service.evaluate_plan("dispatch", "job-1")
        with self.assertRaises(Conflict):
            self.service.dispatch_plan("dispatch", "job-1", "site-n", plan["revision"])

    def test_freeze_version_blocks_and_holds(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        planned = self._plan("job-1")
        plan2 = self._plan("job-2", idem="idem-job-2", start="2026-09-26T01:00:00Z")
        self.service.dispatch_plan("dispatch", "job-2", "site-n", plan2["revision"])
        self.service.mark_running("dispatch", "job-2")
        self.service.freeze_version("comp", "ver-1", "版本冻结送审")
        self.assertEqual(self.service.get_plan("dispatch", "job-1")["state"], "blocked")
        held = self.service.get_plan("audit", "job-2")
        self.assertEqual(held["state"], "manual_hold")
        self.assertEqual(held["blocked_reason_code"], "DATA.FROZEN")
        with self.assertRaises(InvalidState):
            self.service.freeze_version("comp", "ver-1", "重复冻结")

    def test_version_expiry_blocks_new_plan(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        self.clock.advance(days=400)  # 超过 2026-12-31
        plan = self._plan("job-late", start="2027-10-01T00:00:00Z")
        self.assertEqual(plan["state"], "blocked")
        self.assertEqual(plan["blocked_reason_detail"], "DATA.EXPIRED")

    def test_tenant_isolation_and_views(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        self._plan()
        with self.assertRaises(Forbidden):
            self.service.get_plan("tenant-b", "job-1")
        tenant_view = self.service.get_plan("tenant-a", "job-1")
        self.assertNotIn("required_gpu_hours", tenant_view)
        dispatcher = self.service.get_plan("dispatch", "job-1")
        self.assertIn("required_gpu_hours", dispatcher)
        listing = self.service.query_plans("tenant-a")
        self.assertEqual(listing["count"], 1)
        self.assertEqual(listing["plans"][0]["plan_id"], "job-1")
        self.assertEqual(
            self.service.query_plans("dispatch")["count"], 1)

    def test_explain_exclusion_returns_rule(self) -> None:
        self._grant("g-n", ["CN-NORTH"])
        self._plan()
        explanation = self.service.explain_exclusion("tenant-a", "job-1", "site-sg")
        self.assertFalse(explanation["eligible"])
        self.assertEqual(explanation["rule_code"], "DATA.REGION_EXCLUDED")
        self.assertTrue(explanation["rule_message"])

    def test_roles_are_enforced(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.register_dataset("tenant-a", {
                "dataset_id": "x", "name": "x", "owner_tenant_id": "tenant-a", "description": ""})
        with self.assertRaises(Forbidden):
            self.service.access_log("tenant-a")
        with self.assertRaises(Forbidden):
            self.service.revoke_grant("dispatch", "g-n", "无权撤回")
        # 审计可以读取访问记录
        self.assertIn("accesses", self.service.access_log("audit"))

    def test_tenant_cannot_submit_for_other_tenant(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.submit_plan("tenant-a", {
                "plan_id": "job-z", "tenant_id": "tenant-b", "version_id": "ver-1",
                "product": PRODUCT, "scheduled_start_at": "2026-09-25T01:00:00Z",
                "required_gpu_hours": "100", "idempotency_key": "idem-z"})

    def test_api_routes_residency(self) -> None:
        app = JsonApplication(self.service)
        self._grant("g-n", ["CN-NORTH"])
        resp = app.handle("POST", "/job-plans", {"X-Actor-Id": "tenant-a"}, b"""
        {"plan_id":"job-api","tenant_id":"tenant-a","version_id":"ver-1","product":"gpu-h100",
         "scheduled_start_at":"2026-09-25T01:00:00Z","required_gpu_hours":"100","idempotency_key":"k-api"}""")
        self.assertEqual(resp.status, 201)
        self.assertEqual(resp.body["state"], "candidate_ready")
        explanation = app.handle("GET", "/job-plans/job-api/sites/site-sg", {"X-Actor-Id": "tenant-a"})
        self.assertEqual(explanation.body["rule_code"], "DATA.REGION_EXCLUDED")
        accesses = app.handle("GET", "/compliance/accesses", {"X-Actor-Id": "audit"})
        self.assertEqual(accesses.status, 200)
        forbidden = app.handle("GET", "/compliance/accesses", {"X-Actor-Id": "tenant-a"})
        self.assertEqual(forbidden.status, 403)
        replay = app.handle("POST", "/job-plans", {"X-Actor-Id": "tenant-a"}, b"""
        {"plan_id":"job-api","tenant_id":"tenant-a","version_id":"ver-1","product":"gpu-h100",
         "scheduled_start_at":"2026-09-25T01:00:00Z","required_gpu_hours":"100","idempotency_key":"k-api"}""")
        self.assertEqual(replay.body["plan_id"], "job-api")


if __name__ == "__main__":
    unittest.main()
