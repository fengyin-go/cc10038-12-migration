"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from typing import Any

from app.store import store
from app.services.schema_evolution import (
    COL_FORMAT,
    LEGACY_FORMAT,
    TYPED_FORMAT,
    TypeCoercionError,
    coerce_value,
    decode_field_defs,
    encode_descriptors,
    normalize_descriptors,
)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # 批内解析锁：同一批次的解析全程只允许一套完整字段解释，
    # 升级与解析并发时第二个并发解析直接拒绝而不是混用两套 Schema。
    _parse_locks: dict[str, threading.Lock] = {}
    _parse_locks_guard = threading.Lock()

    @classmethod
    def _lock_for_batch(cls, batch_no: str) -> threading.Lock:
        with cls._parse_locks_guard:
            lock = cls._parse_locks.get(batch_no)
            if lock is None:
                lock = threading.Lock()
                cls._parse_locks[batch_no] = lock
            return lock

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def _resolve_schema(
        self, source_id: str
    ) -> tuple[dict[str, Any] | None, list[dict[str, str]] | None, str]:
        """为数据源选定本次解析采用的 Schema 版本。

        优先匹配适用数据源、状态为已发布的版本；没有同源版本时回退到任意
        已发布版本。占位文本等无法解析的字段定义返回 ``(row, None, "")``，
        调用方回退到解析规则驱动的旧入口，保证存量兼容。
        """
        schema_versions = store.rows("schema_version")
        candidates = [sv for sv in schema_versions if sv.get("status") == "已发布"]
        # 新批次采用同源最新发布版本；已 pin 的批次不受影响（在调用处处理）
        source_candidates = [sv for sv in candidates if sv.get("适用数据源") == source_id]
        if source_candidates:
            chosen = source_candidates[-1]
        elif candidates:
            chosen = candidates[-1]
        else:
            chosen = None
        descriptors, fmt = decode_field_defs(chosen.get("字段定义"))
        return chosen, descriptors, fmt

    @staticmethod
    def _schema_snapshot(
        schema_ver: dict[str, Any] | None,
        descriptors: list[dict[str, str]] | None,
        fmt: str,
    ) -> dict[str, Any]:
        """固化本次处理采用的字段解释，随批次/明细写入，供日后复算回查。"""
        return {
            "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
            "schema_format": fmt or LEGACY_FORMAT,
            "field_definitions": copy.deepcopy(descriptors) if descriptors else [],
        }

    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
        reprocess: bool = False,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。

        一个批次只允许一套完整的字段解释：首次解析时把当时选定的 Schema
        版本号、字段定义形态和带类型描述固化到批次上（pin），之后重做旧批
        一律按 pin 住的原版本复算，升级发布新版本不影响在途批次。
        """
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        batches = store.rows("ingest_batch")
        batch_entry = next((b for b in batches if b.get("批次编号") == batch_no), None)
        if batch_entry is None:
            return {"ok": False, "message": f"批次 {batch_no} 不存在"}

        lock = self._lock_for_batch(batch_no)
        if not lock.acquire(blocking=False):
            return {
                "ok": False,
                "message": f"批次 {batch_no} 已有解析在执行，只允许一套完整字段解释",
            }
        try:
            return self._parse_batch_locked(
                batch_no, batch_entry, batch_raw, parser_rule_name, reprocess
            )
        finally:
            lock.release()

    def _parse_batch_locked(
        self,
        batch_no: str,
        batch_entry: dict[str, Any],
        batch_raw: list[dict[str, Any]],
        parser_rule_name: str | None,
        reprocess: bool,
    ) -> dict[str, Any]:
        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        source_id = batch_entry.get("所属数据源", "")

        # 复算旧批：沿用首次解析 pin 住的版本；首次解析：选定并固化版本。
        if reprocess and batch_entry.get("schema_pin"):
            snapshot = json.loads(batch_entry["schema_pin"])
            pinned_version = snapshot.get("schema_version")
            schema_ver = next(
                (sv for sv in store.rows("schema_version")
                 if sv.get("版本号") == pinned_version),
                None,
            )
            descriptors, fmt = decode_field_defs(
                schema_ver.get("字段定义") if schema_ver else ""
            )
            if not descriptors:
                descriptors = snapshot.get("field_definitions") or None
                fmt = snapshot.get("schema_format", LEGACY_FORMAT)
            snapshot = self._schema_snapshot(schema_ver, descriptors, fmt)
        elif batch_entry.get("schema_pin"):
            # 非复算的重复解析也不允许换版本，保证批内解释一致
            snapshot = json.loads(batch_entry["schema_pin"])
            pinned_version = snapshot.get("schema_version")
            schema_ver = next(
                (sv for sv in store.rows("schema_version")
                 if sv.get("版本号") == pinned_version),
                None,
            )
            descriptors, fmt = decode_field_defs(
                schema_ver.get("字段定义") if schema_ver else ""
            )
            if not descriptors:
                descriptors = snapshot.get("field_definitions") or None
                fmt = snapshot.get("schema_format", LEGACY_FORMAT)
        else:
            schema_ver, descriptors, fmt = self._resolve_schema(source_id)
            snapshot = self._schema_snapshot(schema_ver, descriptors, fmt)
            batch_entry["schema_pin"] = json.dumps(snapshot, ensure_ascii=False)

        parsed_entities = store.rows("parsed_entity")

        # 复算时保留首次解析的明细，旧明细标记被取代，不覆盖已有类型依据
        superseded_ids: list[int] = []
        if reprocess:
            for old in parsed_entities:
                if old.get("来源批次") == batch_no and old.get("superseded") is not True:
                    old["superseded"] = True
                    old["superseded_at"] = _now_iso()
                    superseded_ids.append(int(old["id"]))

        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                body = raw.get("原始正文", "")
                if descriptors:
                    fields, type_checks = self._parse_body_typed(body, rule, descriptors)
                else:
                    # 旧格式/占位 Schema：维持当前文本语义的解析入口
                    fields = self._parse_body(body, rule)
                    type_checks = {name: "text" for name in fields}
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": snapshot["schema_version"],
                    "schema_format": snapshot["schema_format"],
                    "schema_field_types": json.dumps(type_checks, ensure_ascii=False),
                    "schema_pin": json.dumps(snapshot, ensure_ascii=False),
                    "reprocess": bool(reprocess),
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                # 解析结果回写该次处理的来源清单（原始记录），历史批次可关联复算
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                raw["source_manifest"] = json.dumps(
                    {
                        "batch_no": batch_no,
                        "raw_record_id": raw_id,
                        "parsed_entity_id": ent_id,
                        "schema_version": snapshot["schema_version"],
                        "schema_format": snapshot["schema_format"],
                        "field_types": type_checks,
                        "reprocess": bool(reprocess),
                        "parsed_at": _now_iso(),
                    },
                    ensure_ascii=False,
                )
                success_count += 1
                parsed_ids.append(ent_id)
            except TypeCoercionError as exc:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                raw["last_error"] = str(exc)
                failure_count += 1
                failed_ids.append(raw_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batch_entry["status"] = "已完成" if failure_count == 0 else "部分失败"
        batch_entry["pending"] = failure_count > 0
        batch_entry["abnormal"] = failure_count > 0
        batch_entry["parsed_ids"] = json.dumps(parsed_ids)
        batch_entry["active_parsed_ids"] = json.dumps(parsed_ids)
        batch_entry["schema_version"] = snapshot["schema_version"]
        batch_entry["schema_format"] = snapshot["schema_format"]
        batch_entry["batch_version"] = batch_entry.get("batch_version", 1) + 1

        # 记录解析日志：批内可追到当时采用的版本
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "复算解析" if reprocess else "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
            "schema_version": snapshot["schema_version"],
            "schema_format": snapshot["schema_format"],
            "schema_pin": json.dumps(snapshot, ensure_ascii=False),
            "reprocess": bool(reprocess),
            "superseded_entity_ids": json.dumps(superseded_ids),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": snapshot["schema_version"],
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
            "schema_version": snapshot["schema_version"],
            "schema_format": snapshot["schema_format"],
            "field_types": json.loads(json.dumps(
                {d["name"]: d["type"] for d in (descriptors or [])},
                ensure_ascii=False,
            )),
            "reprocess": bool(reprocess),
            "superseded_entity_ids": superseded_ids,
        }

    def _parse_body_typed(
        self,
        body: str,
        rule: dict[str, Any],
        descriptors: list[dict[str, str]],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """按带类型的版本描述解析：字段名来自 Schema 描述，值按各字段类型核对。

        原始正文仍按 ``|`` 分隔，位置与描述符顺序对齐。已有字段以文本语义
        读取，新增数字类型才按 number 解析；任一数字字段核对失败即抛
        ``TypeCoercionError``，整条记录进入解析失败/重试。
        """
        parts = body.split("|") if "|" in body else [body]
        fields: dict[str, Any] = {}
        type_checks: dict[str, str] = {}
        for idx, descriptor in enumerate(descriptors):
            name = descriptor["name"]
            field_type = descriptor["type"]
            raw_text = parts[idx].strip() if idx < len(parts) else ""
            fields[name] = coerce_value(raw_text, field_type)
            type_checks[name] = field_type
        return fields, type_checks

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[Any],
        backward_compatible: bool = True,
        typed: bool = False,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。

        ``field_defs`` 支持两种入参：

        - 名称数组（``["time", "level"]``）：按旧格式存储，全部字段按文本读取，
          存量调用方无需改动；
        - 带类型定义（``[{"name": "latency", "type": "number"}]`` 或
          ``[("latency", "number")]`` 混写）：``typed=True`` 时存为带类型的
          版本描述，数字字段随版本记录类型。
        """
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        descriptors = normalize_descriptors(field_defs)
        if typed:
            stored_defs = encode_descriptors(descriptors)
            fmt = TYPED_FORMAT
        else:
            # 旧格式：只保留名称数组，兼容历史采集流水线和回滚后的解析入口
            stored_defs = json.dumps(
                [d["name"] for d in descriptors], ensure_ascii=False
            )
            fmt = LEGACY_FORMAT

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": stored_defs,
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(descriptors),
            COL_FORMAT: fmt,
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    def evolve_typed_schema(
        self,
        version: str,
        data_source: str,
        typed_fields: list[dict[str, str]],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """便捷入口：发布带字段类型的新 Schema 版本。"""
        return self.evolve_schema(
            version,
            data_source,
            normalize_descriptors(typed_fields),
            backward_compatible=backward_compatible,
            typed=True,
        )

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()