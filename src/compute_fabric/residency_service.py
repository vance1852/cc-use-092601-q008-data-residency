"""数据集驻留合规与调度联动的事务用例（以 mixin 接入 SupplyService）。"""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .errors import Conflict, Forbidden, InvalidState, NotFound, ValidationFailed
from .models import DatasetRegistration, DatasetVersionInput, GrantInput, JobPlanInput
from .planning import canonical_json, decimal_text, digest
from .residency import (
    EvaluationInput,
    FacilitySnapshot,
    GrantSnapshot,
    VersionSnapshot,
    evaluate,
    project_dataset_view,
    project_plan_view,
)


NOT_STARTED_STATES = ("planned", "blocked", "candidate_ready")
RUNNING_STATES = ("dispatched", "running")


def _scope_text(codes: Sequence[str]) -> str:
    return canonical_json(sorted(set(codes)))


def _scope_load(text: str | None) -> frozenset[str]:
    if not text:
        return frozenset()
    return frozenset(json.loads(text))


class ResidencyServiceMixin:
    # 以下属性由 SupplyService 提供：connection / clock / _now / _require / _audit

    # --- 数据集与版本登记 -----------------------------------------------------

    def register_dataset(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "dataset.write")
        dataset = DatasetRegistration.from_dict(raw)
        try:
            with self._tx():
                self.connection.execute(
                    "INSERT INTO datasets(dataset_id,name,owner_tenant_id,description,created_by,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (dataset.dataset_id, dataset.name, dataset.owner_tenant_id, dataset.description, actor_id, self._now()),
                )
                self._audit("dataset", dataset.dataset_id, "dataset.registered", actor_id,
                            {"owner_tenant_id": dataset.owner_tenant_id})
        except sqlite3.IntegrityError as exc:
            raise Conflict("数据集编号已经存在") from exc
        return self.dataset_view(actor_id, dataset.dataset_id)

    def register_version(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "dataset.write")
        version = DatasetVersionInput.from_dict(raw)
        if self.connection.execute("SELECT 1 FROM datasets WHERE dataset_id=?", (version.dataset_id,)).fetchone() is None:
            raise NotFound("数据集不存在，无法登记版本")
        if version.expires_at is not None and version.expires_at <= self._now():
            raise ValidationFailed("版本到期时间必须晚于当前时间")
        try:
            with self._tx():
                self.connection.execute(
                    "INSERT INTO dataset_versions(version_id,dataset_id,version_tag,content_sha256,region_scope,"
                    "network_boundary,expires_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        version.version_id, version.dataset_id, version.version_tag, version.content_sha256,
                        _scope_text(version.region_scope), version.network_boundary, version.expires_at,
                        actor_id, self._now(),
                    ),
                )
                self.connection.execute(
                    "UPDATE datasets SET revision=revision+1 WHERE dataset_id=?", (version.dataset_id,)
                )
                self._audit("dataset_version", version.version_id, "version.registered", actor_id, {
                    "dataset_id": version.dataset_id,
                    "version_tag": version.version_tag,
                    "region_scope": sorted(version.region_scope),
                    "network_boundary": version.network_boundary,
                    "expires_at": version.expires_at,
                })
        except sqlite3.IntegrityError as exc:
            raise Conflict("版本编号、版本标签或内容指纹冲突") from exc
        return self._version_row(version.version_id)

    # --- 授权（含全有或全无批量导入） -----------------------------------------

    def import_grants(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        self._require(actor_id, "grant.write")
        items = raw.get("grants")
        if not isinstance(items, list) or not items:
            raise ValidationFailed("grants 必须是非空数组")
        grants = [GrantInput.from_dict(item) for item in items]
        idempotency_key = raw.get("idempotency_key")
        if not isinstance(idempotency_key, str) or not idempotency_key.strip():
            raise ValidationFailed("idempotency_key 不能为空")
        idempotency_key = idempotency_key.strip()
        request_digest = digest({"grants": [dict(item) for item in items]})
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency WHERE scope='grant_import' AND idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同授权批量内容")
            return {**json.loads(stored["response_json"]), "replayed": True}
        now = self._now()
        response = {
            "imported": len(grants),
            "grant_ids": sorted(grant.grant_id for grant in grants),
            "state": "imported",
            "replayed": False,
        }
        # 单一大事务：任一授权冲突则整批回滚，做到全有或全无。
        try:
            with self._tx():
                for grant in grants:
                    self._insert_grant(grant, actor_id, now)
                self.connection.execute(
                    "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('grant_import',?,?,?,?)",
                    (idempotency_key, request_digest, canonical_json({**response, "replayed": False}), now),
                )
                self._audit("grant", f"batch:{idempotency_key}", "grants.imported", actor_id,
                            {"count": len(grants), "grant_ids": response["grant_ids"]})
        except sqlite3.IntegrityError as exc:
            raise Conflict("授权批量中存在编号或主体冲突，整批未导入") from exc
        return response

    def _insert_grant(self, grant: GrantInput, actor_id: str, now: str) -> None:
        if grant.expires_at is not None and grant.expires_at <= now:
            raise ValidationFailed(f"授权 {grant.grant_id} 到期时间必须晚于当前时间")
        dataset = self.connection.execute(
            "SELECT dataset_id FROM datasets WHERE dataset_id=?", (grant.dataset_id,)
        ).fetchone()
        if dataset is None:
            raise NotFound(f"授权 {grant.grant_id} 引用的数据集不存在")
        if grant.version_id is not None:
            version = self.connection.execute(
                "SELECT dataset_id FROM dataset_versions WHERE version_id=?", (grant.version_id,)
            ).fetchone()
            if version is None or version["dataset_id"] != grant.dataset_id:
                raise NotFound(f"授权 {grant.grant_id} 引用的版本不属于该数据集")
        self.connection.execute(
            "INSERT INTO dataset_grants(grant_id,dataset_id,version_id,subject_id,subject_type,region_scope,"
            "network_boundary,granted_at,expires_at,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (
                grant.grant_id, grant.dataset_id, grant.version_id, grant.subject_id, grant.subject_type,
                _scope_text(grant.region_scope), grant.network_boundary, now, grant.expires_at, actor_id, now,
            ),
        )

    def revoke_grant(self, actor_id: str, grant_id: str, reason: str) -> dict[str, Any]:
        self._require(actor_id, "grant.revoke")
        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("撤回原因不能为空")
        grant = self.connection.execute("SELECT * FROM dataset_grants WHERE grant_id=?", (grant_id,)).fetchone()
        if grant is None:
            raise NotFound("授权不存在")
        now = self._now()
        with self._tx():
            cursor = self.connection.execute(
                "UPDATE dataset_grants SET state='revoked',revoked_at=?,revoke_reason=?,revoked_by=?,"
                "revision=revision+1 WHERE grant_id=? AND state='active'",
                (now, reason.strip(), actor_id, grant_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("授权已撤回或已失效，不能重复撤回")
            self._audit("grant", grant_id, "grant.revoked", actor_id,
                        {"dataset_id": grant["dataset_id"], "subject_id": grant["subject_id"], "reason": reason.strip()})
            self._cascade_subject_change(grant, "GRANT.REVOKED", actor_id, reason.strip(), now)
        return {"grant_id": grant_id, "state": "revoked", "revoked_at": now}

    def freeze_version(self, actor_id: str, version_id: str, reason: str) -> dict[str, Any]:
        self._require(actor_id, "version.freeze")
        if not isinstance(reason, str) or not reason.strip():
            raise ValidationFailed("冻结原因不能为空")
        version = self.connection.execute("SELECT * FROM dataset_versions WHERE version_id=?", (version_id,)).fetchone()
        if version is None:
            raise NotFound("数据集版本不存在")
        now = self._now()
        with self._tx():
            cursor = self.connection.execute(
                "UPDATE dataset_versions SET state='frozen',frozen_reason=?,revision=revision+1 "
                "WHERE version_id=? AND state='registered'",
                (reason.strip(), version_id),
            )
            if cursor.rowcount != 1:
                raise InvalidState("版本已冻结")
            self._audit("dataset_version", version_id, "version.frozen", actor_id, {"reason": reason.strip()})
            # 冻结影响该版本全部计划。
            for plan in self._plans_for_version(version_id, NOT_STARTED_STATES + RUNNING_STATES):
                self._apply_compliance_hold_or_block(plan, "DATA.FROZEN", reason.strip(), actor_id, now)
        return {"version_id": version_id, "state": "frozen"}

    def _cascade_subject_change(
        self, grant: sqlite3.Row, code: str, actor_id: str, reason: str, now: str
    ) -> None:
        """授权撤回/失效的级联：阻止未启动计划，运行中计划转人工处置。"""
        if grant["version_id"] is not None:
            rows = self.connection.execute(
                "SELECT * FROM job_plans WHERE version_id=? AND state IN (?,?,?,?,?)",
                (grant["version_id"],) + NOT_STARTED_STATES + RUNNING_STATES,
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT jp.* FROM job_plans jp JOIN dataset_versions dv ON dv.version_id=jp.version_id "
                "WHERE dv.dataset_id=? AND jp.state IN (?,?,?,?,?)",
                (grant["dataset_id"],) + NOT_STARTED_STATES + RUNNING_STATES,
            ).fetchall()
        for plan in rows:
            if grant["subject_type"] == "tenant" and plan["tenant_id"] != grant["subject_id"]:
                # 其它租户的计划不使用该主体授权，不受影响。
                continue
            if grant["subject_type"] == "user" and plan["submitted_by"] != grant["subject_id"]:
                continue
            self._apply_compliance_hold_or_block(plan, code, reason, actor_id, now)

    def _apply_compliance_hold_or_block(
        self, plan: sqlite3.Row, code: str, reason: str, actor_id: str, now: str
    ) -> None:
        if plan["state"] in RUNNING_STATES:
            # 运行中：只在原派发站点确实变得不合规时进入人工处置；
            # 若该站点仍被其它有效授权覆盖，则作业继续运行，绝不静默迁移。
            facility_id = plan["facility_id"]
            if facility_id:
                result = evaluate(self._build_evaluation(plan))
                verdict = next(
                    (candidate for candidate in result["candidates"]
                     if candidate["facility_id"] == facility_id),
                    None,
                )
                if verdict is not None and verdict["eligible"]:
                    return
            self.connection.execute(
                "UPDATE job_plans SET state='manual_hold',blocked_reason_code=?,blocked_reason_detail=?,"
                "revision=revision+1,updated_at=? WHERE plan_id=?",
                (code, reason, now, plan["plan_id"]),
            )
            self.connection.execute(
                "INSERT INTO manual_dispositions(plan_id,previous_state,action,note,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (plan["plan_id"], plan["state"], "hold",
                 f"合规状态变化（{code}）触发人工处置：{reason}", actor_id, now),
            )
            self._audit("job_plan", plan["plan_id"], "plan.manual_hold", actor_id, {"rule_code": code})
            return
        # 未启动：基于最新授权实时重算。仍有合规站点则保留可派发候选；
        # 只有全部站点被排除时才 blocked，且阻断原因取实际规则。
        self._recompute_plan(plan["plan_id"], now)

    # --- 作业计划 -------------------------------------------------------------

    def submit_plan(self, actor_id: str, raw: Mapping[str, Any]) -> dict[str, Any]:
        user = self._require(actor_id, "job.write")
        plan = JobPlanInput.from_dict(raw)
        if user["role"] == "tenant" and plan.tenant_id != actor_id:
            raise Forbidden("租户只能为自己提交作业计划")
        request_digest = digest(raw)
        stored = self.connection.execute(
            "SELECT request_sha256,response_json FROM supply_idempotency WHERE scope='job_plan' AND idempotency_key=?",
            (plan.idempotency_key,),
        ).fetchone()
        if stored is not None:
            if stored["request_sha256"] != request_digest:
                raise Conflict("幂等键对应不同作业计划内容")
            return json.loads(stored["response_json"])
        if self.connection.execute("SELECT 1 FROM dataset_versions WHERE version_id=?", (plan.version_id,)).fetchone() is None:
            raise NotFound("引用的数据集版本不存在，作业必须引用确定版本")
        now = self._now()
        plan_id = plan.plan_id
        try:
            with self._tx():
                self.connection.execute(
                    "INSERT INTO job_plans(plan_id,tenant_id,version_id,product,scheduled_start_at,scheduled_end_at,"
                    "requirements_json,state,idempotency_key,submitted_by,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        plan_id, plan.tenant_id, plan.version_id, plan.product, plan.scheduled_start_at,
                        plan.scheduled_end_at,
                        canonical_json({"required_gpu_hours": decimal_text(plan.required_gpu_hours)}),
                        "planned", plan.idempotency_key, actor_id, now, now,
                    ),
                )
                self._audit("job_plan", plan_id, "plan.submitted", actor_id,
                            {"version_id": plan.version_id, "tenant_id": plan.tenant_id})
                plan_row = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
                result = evaluate(self._build_evaluation(plan_row))
                self._persist_evaluation(plan_row, result, now)
                self.connection.execute(
                    "INSERT INTO supply_idempotency(scope,idempotency_key,request_sha256,response_json,created_at) "
                    "VALUES('job_plan',?,?,?,?)",
                    (plan.idempotency_key, request_digest, canonical_json(self._stored_plan_view(actor_id, plan_id)), now),
                )
                self._audit("job_plan", plan_id, "plan.evaluated", actor_id,
                            {"state": result["state"], "eligible_sites": result["eligible_sites"]})
        except sqlite3.IntegrityError as exc:
            raise Conflict("作业计划编号或幂等键冲突") from exc
        return self.get_plan(actor_id, plan_id)

    def _stored_plan_view(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        """幂等响应按提交者角色投影，保证重复提交返回与首次一致的最小视图。"""
        user = self.connection.execute("SELECT * FROM supply_users WHERE user_id=?", (actor_id,)).fetchone()
        plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
        payload = self._plan_payload(plan)
        return project_plan_view(self._view_role(user), payload)

    def _build_evaluation(self, plan: sqlite3.Row) -> EvaluationInput:
        version_row = self.connection.execute(
            "SELECT * FROM dataset_versions WHERE version_id=?", (plan["version_id"],)
        ).fetchone()
        version = None
        dataset_id = None
        if version_row is not None:
            dataset_id = version_row["dataset_id"]
            version = VersionSnapshot(
                version_id=version_row["version_id"],
                dataset_id=dataset_id,
                region_scope=_scope_load(version_row["region_scope"]),
                network_boundary=version_row["network_boundary"],
                expires_at=version_row["expires_at"],
                state=version_row["state"],
                frozen_reason=version_row["frozen_reason"],
            )
        grants: list[GrantSnapshot] = []
        if dataset_id is not None:
            for row in self.connection.execute(
                "SELECT * FROM dataset_grants WHERE dataset_id=?", (dataset_id,)
            ).fetchall():
                grants.append(GrantSnapshot(
                    grant_id=row["grant_id"],
                    subject_id=row["subject_id"],
                    subject_type=row["subject_type"],
                    region_scope=_scope_load(row["region_scope"]),
                    network_boundary=row["network_boundary"],
                    expires_at=row["expires_at"],
                    state=row["state"],
                ))
        requirements = json.loads(plan["requirements_json"])
        required = Decimal(str(requirements["required_gpu_hours"]))
        facilities: list[FacilitySnapshot] = []
        for facility in self.connection.execute("SELECT * FROM facilities ORDER BY facility_id").fetchall():
            totals = {
                row["product"]: Decimal(str(row["total"]))
                for row in self.connection.execute(
                    "SELECT product,sum(CAST(available_gpu_hours AS REAL)) AS total "
                    "FROM inventory_lots WHERE facility_id=? GROUP BY product",
                    (facility["facility_id"],),
                ).fetchall()
            }
            products = frozenset(product for product, total in totals.items() if total > 0)
            facilities.append(FacilitySnapshot(
                facility_id=facility["facility_id"],
                region=facility["region"],
                network_boundary=facility["network_boundary"],
                active=bool(facility["active"]),
                products=products,
                available_gpu_hours=totals.get(plan["product"], Decimal("0")),
            ))
        submitter = self.connection.execute(
            "SELECT role FROM supply_users WHERE user_id=?", (plan["submitted_by"],)
        ).fetchone()
        user_id = plan["submitted_by"] if submitter is not None and submitter["role"] == "tenant" else None
        return EvaluationInput(
            tenant_id=plan["tenant_id"],
            version=version,
            grants=grants,
            facilities=facilities,
            product=plan["product"],
            required_gpu_hours=required,
            as_of=self._now(),
            user_id=user_id,
        )

    def _recompute_plan(self, plan_id: str, now: str) -> dict[str, Any]:
        plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFound("作业计划不存在")
        if plan["state"] in ("running", "dispatched", "manual_hold", "completed", "cancelled"):
            # 已在执行/终态的计划不做静默重算改写。
            return self._plan_payload(plan)
        data = self._build_evaluation(plan)
        result = evaluate(data)
        self._persist_evaluation(plan, result, now)
        return self._plan_payload(
            self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
        )

    def _persist_evaluation(self, plan: sqlite3.Row, result: Mapping[str, Any], now: str) -> None:
        self.connection.execute(
            "UPDATE job_plans SET state=?,blocked_reason_code=?,blocked_reason_detail=?,"
            "last_evaluation_json=?,revision=revision+1,updated_at=? WHERE plan_id=?",
            (
                result["state"], result["blocked_reason_code"], result["blocked_reason_detail"],
                canonical_json(result), now, plan["plan_id"],
            ),
        )
        self.connection.execute("DELETE FROM job_plan_candidates WHERE plan_id=?", (plan["plan_id"],))
        for candidate in result["candidates"]:
            self.connection.execute(
                "INSERT INTO job_plan_candidates(plan_id,facility_id,eligible,rule_code,rule_detail,evaluated_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    plan["plan_id"], candidate["facility_id"], 1 if candidate["eligible"] else 0,
                    candidate["rule_code"], candidate["rule_message"], now,
                ),
            )

    def evaluate_plan(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        self._require(actor_id, "job.evaluate")
        now = self._now()
        with self._tx():
            self._recompute_plan(plan_id, now)
            self._audit("job_plan", plan_id, "plan.evaluated", actor_id, {})
        return self.get_plan(actor_id, plan_id)

    def dispatch_plan(
        self, actor_id: str, plan_id: str, facility_id: str, expected_revision: int
    ) -> dict[str, Any]:
        self._require(actor_id, "job.dispatch")
        now = self._now()
        with self._tx():
            plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if plan is None:
                raise NotFound("作业计划不存在")
            if plan["revision"] != expected_revision:
                raise Conflict("计划已被重新评估，请使用最新版本")
            if plan["state"] == "manual_hold":
                raise InvalidState("计划处于人工处置状态，必须由合规人员处置，不能派发")
            if plan["state"] not in ("candidate_ready", "planned", "blocked"):
                raise InvalidState(f"计划当前状态 {plan['state']} 不能派发")
            data = self._build_evaluation(plan)
            result = evaluate(data)
            if facility_id not in result["eligible_sites"]:
                verdict = next((c for c in result["candidates"] if c["facility_id"] == facility_id), None)
                message = verdict["rule_message"] if verdict is not None else "站点不在候选范围"
                raise InvalidState(f"站点 {facility_id} 当前不满足合规边界：{message}")
            self.connection.execute(
                "UPDATE job_plans SET state='dispatched',facility_id=?,revision=revision+1,updated_at=? WHERE plan_id=?",
                (facility_id, now, plan_id),
            )
            self._record_access(plan, facility_id, "dispatch", "active", now)
            self._audit("job_plan", plan_id, "plan.dispatched", actor_id, {"facility_id": facility_id})
        return self.get_plan(actor_id, plan_id)

    def mark_running(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        self._require(actor_id, "job.dispatch")
        now = self._now()
        with self._tx():
            plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if plan is None:
                raise NotFound("作业计划不存在")
            if plan["state"] != "dispatched":
                raise InvalidState("只有已派发计划可以标记运行")
            self.connection.execute(
                "UPDATE job_plans SET state='running',revision=revision+1,updated_at=? WHERE plan_id=?",
                (now, plan_id),
            )
            self._record_access(plan, plan["facility_id"], "run_start", "active", now)
            self._audit("job_plan", plan_id, "plan.running", actor_id, {})
        return self.get_plan(actor_id, plan_id)

    def _record_access(
        self, plan: sqlite3.Row, facility_id: str, kind: str, authorized_state: str, now: str
    ) -> None:
        grant_ids = [
            row["grant_id"]
            for row in self.connection.execute(
                "SELECT grant_id FROM dataset_grants WHERE dataset_id="
                "(SELECT dataset_id FROM dataset_versions WHERE version_id=?) AND state='active' "
                "AND (subject_id=? OR (subject_type='user' AND subject_id=?))",
                (plan["version_id"], plan["tenant_id"], plan["submitted_by"]),
            ).fetchall()
        ]
        self.connection.execute(
            "INSERT INTO compliance_accesses(plan_id,version_id,facility_id,subject_id,grant_id,accessed_at,"
            "access_kind,authorized_state,detail_json) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                plan["plan_id"], plan["version_id"], facility_id, plan["tenant_id"],
                grant_ids[0] if grant_ids else None, now, kind, authorized_state,
                canonical_json({"grant_ids": grant_ids}),
            ),
        )

    def dispose_plan(self, actor_id: str, plan_id: str, action: str, note: str) -> dict[str, Any]:
        """人工处置运行中/挂起计划；系统不做静默迁移。"""
        self._require(actor_id, "job.disposition")
        if action not in ("resume", "cancel", "complete"):
            raise ValidationFailed("处置动作必须是 resume、cancel 或 complete")
        if not isinstance(note, str) or not note.strip():
            raise ValidationFailed("处置说明不能为空")
        now = self._now()
        with self._tx():
            plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
            if plan is None:
                raise NotFound("作业计划不存在")
            if plan["state"] != "manual_hold":
                raise InvalidState("只有人工处置状态的计划需要处置")
            previous = plan["state"]
            if action == "cancel":
                new_state = "cancelled"
                self.connection.execute(
                    "UPDATE job_plans SET state=?,revision=revision+1,updated_at=? WHERE plan_id=?",
                    (new_state, now, plan_id),
                )
            elif action == "complete":
                new_state = "completed"
                self.connection.execute(
                    "UPDATE job_plans SET state=?,revision=revision+1,updated_at=? WHERE plan_id=?",
                    (new_state, now, plan_id),
                )
            else:
                # resume：只在原派发站点重新校验，合规恢复才回到运行；
                # 不满足则保持人工挂起，绝不静默迁移到其它站点。
                if not plan["facility_id"]:
                    raise InvalidState("计划尚未派发到站点，不能恢复运行")
                result = evaluate(self._build_evaluation(plan))
                verdict = next(
                    (c for c in result["candidates"] if c["facility_id"] == plan["facility_id"]), None
                )
                if verdict is None or not verdict["eligible"]:
                    self.connection.execute(
                        "INSERT INTO manual_dispositions(plan_id,previous_state,action,note,created_by,created_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (plan_id, previous, "hold",
                         f"恢复被拒绝：{None if verdict is None else verdict['rule_message']}；{note.strip()}",
                         actor_id, now),
                    )
                    self._audit("job_plan", plan_id, "plan.disposition.resume_denied", actor_id, {})
                    return self.get_plan(actor_id, plan_id)
                new_state = "running"
                self.connection.execute(
                    "UPDATE job_plans SET state='running',blocked_reason_code=NULL,blocked_reason_detail=NULL,"
                    "revision=revision+1,updated_at=? WHERE plan_id=?",
                    (now, plan_id),
                )
                self._record_access(plan, plan["facility_id"], "run_resume", "active", now)
            self.connection.execute(
                "INSERT INTO manual_dispositions(plan_id,previous_state,action,note,created_by,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (plan_id, previous, action, note.strip(), actor_id, now),
            )
            self._audit("job_plan", plan_id, f"plan.disposition.{action}", actor_id, {"note": note.strip()})
        return self.get_plan(actor_id, plan_id)

    # --- 查询视图 -------------------------------------------------------------

    def _version_row(self, version_id: str) -> dict[str, Any]:
        row = self.connection.execute("SELECT * FROM dataset_versions WHERE version_id=?", (version_id,)).fetchone()
        if row is None:
            raise NotFound("数据集版本不存在")
        return {
            "version_id": row["version_id"],
            "dataset_id": row["dataset_id"],
            "version_tag": row["version_tag"],
            "content_sha256": row["content_sha256"],
            "region_scope": sorted(_scope_load(row["region_scope"])),
            "network_boundary": row["network_boundary"],
            "expires_at": row["expires_at"],
            "state": row["state"],
            "frozen_reason": row["frozen_reason"],
            "created_at": row["created_at"],
        }

    def _plans_for_version(self, version_id: str, states: Sequence[str]) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in states)
        return self.connection.execute(
            f"SELECT * FROM job_plans WHERE version_id=? AND state IN ({placeholders})",
            (version_id, *states),
        ).fetchall()

    def dataset_view(self, actor_id: str, dataset_id: str) -> dict[str, Any]:
        user = self._require(actor_id, "dataset.read")
        row = self.connection.execute("SELECT * FROM datasets WHERE dataset_id=?", (dataset_id,)).fetchone()
        if row is None:
            raise NotFound("数据集不存在")
        if user["role"] == "tenant" and row["owner_tenant_id"] != actor_id:
            raise Forbidden("租户只能查看自己拥有的数据集")
        versions = [
            {
                "version_id": item["version_id"],
                "version_tag": item["version_tag"],
                "content_sha256": item["content_sha256"],
                "region_scope": sorted(_scope_load(item["region_scope"])),
                "network_boundary": item["network_boundary"],
                "expires_at": item["expires_at"],
                "state": item["state"],
                "frozen_reason": item["frozen_reason"],
                "created_at": item["created_at"],
            }
            for item in self.connection.execute(
                "SELECT * FROM dataset_versions WHERE dataset_id=? ORDER BY created_at,version_id", (dataset_id,)
            ).fetchall()
        ]
        grants = [
            {
                "grant_id": item["grant_id"],
                "version_id": item["version_id"],
                "subject_id": item["subject_id"],
                "subject_type": item["subject_type"],
                "region_scope": sorted(_scope_load(item["region_scope"])),
                "network_boundary": item["network_boundary"],
                "granted_at": item["granted_at"],
                "expires_at": item["expires_at"],
                "state": item["state"],
                "revoked_at": item["revoked_at"],
                "revoke_reason": item["revoke_reason"],
            }
            for item in self.connection.execute(
                "SELECT * FROM dataset_grants WHERE dataset_id=? ORDER BY grant_id", (dataset_id,)
            ).fetchall()
        ]
        payload = {
            "dataset_id": row["dataset_id"],
            "name": row["name"],
            "owner_tenant_id": row["owner_tenant_id"],
            "revision": row["revision"],
            "created_at": row["created_at"],
            "versions": versions,
            "grants": grants,
            "active_grants": sum(1 for grant in grants if grant["state"] == "active"),
        }
        return project_dataset_view(user["role"], payload)

    def _plan_payload(self, plan: sqlite3.Row) -> dict[str, Any]:
        evaluation = json.loads(plan["last_evaluation_json"]) if plan["last_evaluation_json"] else {}
        version = self.connection.execute(
            "SELECT dataset_id FROM dataset_versions WHERE version_id=?", (plan["version_id"],)
        ).fetchone()
        payload = {
            "plan_id": plan["plan_id"],
            "tenant_id": plan["tenant_id"],
            "version_id": plan["version_id"],
            "dataset_id": None if version is None else version["dataset_id"],
            "product": plan["product"],
            "required_gpu_hours": json.loads(plan["requirements_json"])["required_gpu_hours"],
            "scheduled_start_at": plan["scheduled_start_at"],
            "scheduled_end_at": plan["scheduled_end_at"],
            "state": plan["state"],
            "blocked_reason_code": plan["blocked_reason_code"],
            "blocked_reason_detail": plan["blocked_reason_detail"],
            "eligible_sites": evaluation.get("eligible_sites", []),
            "candidates": evaluation.get("candidates", []),
            "facility_id": plan["facility_id"],
            "revision": plan["revision"],
            "submitted_by": plan["submitted_by"],
            "created_at": plan["created_at"],
            "updated_at": plan["updated_at"],
        }
        return payload

    def _view_role(self, user: sqlite3.Row) -> str:
        return {
            "tenant": "tenant",
            "dispatcher": "dispatcher",
            "auditor": "auditor",
            "compliance": "auditor",
        }.get(user["role"], "dispatcher")

    def get_plan(self, actor_id: str, plan_id: str) -> dict[str, Any]:
        user = self._require(actor_id, "job.read")
        plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFound("作业计划不存在")
        if user["role"] == "tenant" and plan["tenant_id"] != actor_id:
            raise Forbidden("租户只能查看自己的作业计划")
        payload = self._plan_payload(plan)
        if self._view_role(user) == "auditor":
            payload["authorization"] = self._authorization_evidence(plan)
            payload["accesses"] = self._accesses(plan_id)
            payload["manual_dispositions"] = self._dispositions(plan_id)
        return project_plan_view(self._view_role(user), payload)

    def _authorization_evidence(self, plan: sqlite3.Row) -> dict[str, Any]:
        version = self.connection.execute(
            "SELECT * FROM dataset_versions WHERE version_id=?", (plan["version_id"],)
        ).fetchone()
        grants = self.connection.execute(
            "SELECT * FROM dataset_grants WHERE dataset_id="
            "(SELECT dataset_id FROM dataset_versions WHERE version_id=?) ORDER BY grant_id",
            (plan["version_id"],),
        ).fetchall()
        return {
            "version_state": None if version is None else version["state"],
            "version_expires_at": None if version is None else version["expires_at"],
            "grants": [
                {
                    "grant_id": row["grant_id"],
                    "subject_id": row["subject_id"],
                    "subject_type": row["subject_type"],
                    "region_scope": sorted(_scope_load(row["region_scope"])),
                    "network_boundary": row["network_boundary"],
                    "state": row["state"],
                    "expires_at": row["expires_at"],
                    "revoked_at": row["revoked_at"],
                    "revoke_reason": row["revoke_reason"],
                }
                for row in grants
            ],
        }

    def _accesses(self, plan_id: str) -> list[dict[str, Any]]:
        return [
            {
                "access_id": row["access_id"],
                "facility_id": row["facility_id"],
                "subject_id": row["subject_id"],
                "grant_id": row["grant_id"],
                "accessed_at": row["accessed_at"],
                "access_kind": row["access_kind"],
                "authorized_state": row["authorized_state"],
            }
            for row in self.connection.execute(
                "SELECT * FROM compliance_accesses WHERE plan_id=? ORDER BY access_id", (plan_id,)
            ).fetchall()
        ]

    def _dispositions(self, plan_id: str) -> list[dict[str, Any]]:
        return [
            {
                "disposition_id": row["disposition_id"],
                "previous_state": row["previous_state"],
                "action": row["action"],
                "note": row["note"],
                "created_by": row["created_by"],
                "created_at": row["created_at"],
            }
            for row in self.connection.execute(
                "SELECT * FROM manual_dispositions WHERE plan_id=? ORDER BY disposition_id", (plan_id,)
            ).fetchall()
        ]

    def query_plans(self, actor_id: str, state: str | None = None, tenant_id: str | None = None) -> dict[str, Any]:
        user = self._require(actor_id, "job.read")
        clauses: list[str] = []
        params: list[Any] = []
        if user["role"] == "tenant":
            clauses.append("tenant_id=?")
            params.append(actor_id)
        elif tenant_id is not None:
            clauses.append("tenant_id=?")
            params.append(tenant_id)
        if state is not None:
            clauses.append("state=?")
            params.append(state)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.connection.execute(
            f"SELECT plan_id FROM job_plans{where} ORDER BY scheduled_start_at,plan_id", params
        ).fetchall()
        plans = [
            project_plan_view(self._view_role(user), self._plan_payload(
                self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (row["plan_id"],)).fetchone()
            ), include_candidates=False)
            for row in rows
        ]
        return {"role": self._view_role(user), "count": len(plans), "plans": plans}

    def explain_exclusion(self, actor_id: str, plan_id: str, facility_id: str) -> dict[str, Any]:
        """解释某站点被排除的具体规则。"""
        user = self._require(actor_id, "job.read")
        plan = self.connection.execute("SELECT * FROM job_plans WHERE plan_id=?", (plan_id,)).fetchone()
        if plan is None:
            raise NotFound("作业计划不存在")
        if user["role"] == "tenant" and plan["tenant_id"] != actor_id:
            raise Forbidden("租户只能查看自己的作业计划")
        # 实时重算，确保解释反映最新合规状态，而不仅是历史快照。
        result = evaluate(self._build_evaluation(plan))
        verdict = next((c for c in result["candidates"] if c["facility_id"] == facility_id), None)
        if verdict is None:
            raise NotFound("该站点不在评估范围内")
        return {
            "plan_id": plan_id,
            "facility_id": facility_id,
            "eligible": verdict["eligible"],
            "rule_code": verdict["rule_code"],
            "rule_detail": verdict["rule_detail"],
            "rule_message": verdict["rule_message"],
        }

    def access_log(self, actor_id: str, plan_id: str | None = None) -> dict[str, Any]:
        """合规访问记录（只追加）：审计与合规人员可见，撤回不抹除。"""
        self._require(actor_id, "access.read")
        if plan_id is not None:
            rows = self.connection.execute(
                "SELECT * FROM compliance_accesses WHERE plan_id=? ORDER BY access_id", (plan_id,)
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM compliance_accesses ORDER BY access_id DESC LIMIT 200"
            ).fetchall()
        return {
            "count": len(rows),
            "accesses": [
                {
                    "access_id": row["access_id"],
                    "plan_id": row["plan_id"],
                    "version_id": row["version_id"],
                    "facility_id": row["facility_id"],
                    "subject_id": row["subject_id"],
                    "grant_id": row["grant_id"],
                    "accessed_at": row["accessed_at"],
                    "access_kind": row["access_kind"],
                    "authorized_state": row["authorized_state"],
                }
                for row in rows
            ],
        }

    def _tx(self):
        from .storage import transaction
        return transaction(self.connection, immediate=True)
