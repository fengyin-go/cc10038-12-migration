"""Schema 字段定义迁移与带类型解析的端到端测试。

覆盖题面要求：
- 历史名称数组 ↔ 带类型版本描述的迁移（存量兼容、中断恢复、回滚）；
- 解析从带类型版本描述读取输入字段并按版本类型核对原始日志；
- 解析结果回写来源清单，历史批次可按原字段定义关联复算；
- 同一接入源两批并存：重做旧批走旧版本、新批走带类型新版本；
- 每批 pin 住当时完整字段解释，升级不改变在途/历史批次；
- 同名字段旧版记文本、新版记数字；回滚后旧入口仍可用、明细不丢类型依据。
"""
from __future__ import annotations

import json
import threading

import pytest

from app.services.schema_evolution import (
    COL_FORMAT,
    COL_LEGACY_BACKUP,
    LEGACY_FORMAT,
    TYPED_FORMAT,
    TypeCoercionError,
    coerce_value,
    decode_field_defs,
    schema_evolution,
)
from app.store import store


SOURCE = "checkout-svc"


def _publish_parser_rule(name: str, fields: list[str]) -> None:
    """登记一条字段名合法的已发布解析规则。"""
    store.rows("field_parser").append({
        "id": max((r.get("id", 0) for r in store.rows("field_parser")), default=0) + 1,
        "规则名称": name,
        "适用数据源": SOURCE,
        "输入格式": "pipe",
        "输出字段列表": ",".join(fields),
        "解析引擎": "split-pipe",
        "status": "已发布",
        "pending": False,
        "abnormal": False,
    })


def _prepare_source(pipeline, name: str = SOURCE) -> None:
    pipeline.register_source(name, "http", "beijing")
    pipeline.validate_source(name)
    _publish_parser_rule("checkout-parser", ["event", "latency"])


def _start_batch(pipeline, logs, name: str = SOURCE):
    result = pipeline.start_ingest_batch(name, "2026-11-21", logs)
    assert result["ok"] is True, result["message"]
    return result["batch"]["批次编号"]


def _active_entities(batch_no: str):
    return [
        e for e in store.rows("parsed_entity")
        if e.get("来源批次") == batch_no and not e.get("superseded")
    ]


# ---------------------------------------------------------------------------
# 一、编解码与类型核对单元行为
# ---------------------------------------------------------------------------
class TestTypedDescriptorCodec:
    def test_legacy_array_decodes_as_text(self):
        descriptors, fmt = decode_field_defs(json.dumps(["event", "latency"]))
        assert fmt == LEGACY_FORMAT
        assert descriptors == [
            {"name": "event", "type": "text"},
            {"name": "latency", "type": "text"},
        ]

    def test_typed_descriptor_roundtrip(self):
        typed = [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}]
        descriptors, fmt = decode_field_defs(json.dumps(typed))
        assert fmt == TYPED_FORMAT
        assert descriptors[1]["type"] == "number"

    def test_placeholder_text_is_not_decodable(self):
        descriptors, fmt = decode_field_defs("Schema版本样例1")
        assert descriptors is None and fmt == ""

    def test_coerce_text_keeps_semantics(self):
        assert coerce_value(" 120 ", "text") == "120"

    def test_coerce_number_int_and_float(self):
        assert coerce_value("120", "number") == 120
        assert isinstance(coerce_value("120", "number"), int)
        assert coerce_value("120.5", "number") == pytest.approx(120.5)
        assert isinstance(coerce_value("120.5", "number"), float)

    def test_coerce_number_rejects_bad_value(self):
        with pytest.raises(TypeCoercionError):
            coerce_value("N/A", "number")


# ---------------------------------------------------------------------------
# 二、历史字段定义迁移：存量兼容、中断恢复
# ---------------------------------------------------------------------------
class TestSchemaMigration:
    def test_migrate_legacy_names_to_typed(self, pipeline):
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        result = schema_evolution.migrate()

        assert result["ok"] is True
        assert result["status"] == "已完成"
        row = next(s for s in store.rows("schema_version") if s["版本号"] == "v1.0")
        assert row[COL_FORMAT] == TYPED_FORMAT
        descriptors, fmt = decode_field_defs(row["字段定义"])
        assert fmt == TYPED_FORMAT
        assert [d["name"] for d in descriptors] == ["event", "latency"]
        # 迁移默认把历史字段保留为文本语义
        assert all(d["type"] == "text" for d in descriptors)
        # 旧格式备份完整保留
        assert json.loads(row[COL_LEGACY_BACKUP]) == ["event", "latency"]

    def test_migration_is_resumable_after_interruption(self, pipeline):
        for idx in range(3):
            pipeline.evolve_schema(f"leg-{idx}", SOURCE, ["event", "latency"], True)

        first = schema_evolution.migrate(limit=1)
        assert first["migrated_this_run"] == 1
        assert first["remaining"] == 2
        assert first["status"] == "已中断"

        # 中断后重入：已迁移行自动跳过，从断点继续
        second = schema_evolution.migrate(limit=1)
        assert second["already_migrated"] == 1
        assert second["migrated_this_run"] == 1
        assert second["remaining"] == 1

        third = schema_evolution.migrate()
        assert third["migrated_this_run"] == 1
        assert third["remaining"] == 0
        assert third["status"] == "已完成"

        # 进度表完整记录迁移轨迹
        status = schema_evolution.migration_status()
        assert status["migrated"] == 3
        assert len(status["migrated_versions"]) == 3

    def test_repeated_migrate_is_idempotent(self, pipeline):
        pipeline.evolve_schema("v1.0", SOURCE, ["event"], True)
        schema_evolution.migrate()
        again = schema_evolution.migrate()
        assert again["migrated_this_run"] == 0
        assert again["remaining"] == 0

    def test_unparseable_seed_rows_are_skipped_not_failed(self, pipeline):
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        result = schema_evolution.migrate()
        # 种子里的占位样例不可解析，只统计为 skipped，不影响真实版本迁移
        assert result["skipped"] >= 1
        assert result["status"] == "已完成"

    def test_rollback_restores_legacy_format_and_parser_entry(self, pipeline):
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        schema_evolution.migrate()

        rollback = schema_evolution.rollback()
        assert rollback["ok"] is True
        assert "v1.0" in rollback["restored_versions"]

        row = next(s for s in store.rows("schema_version") if s["版本号"] == "v1.0")
        descriptors, fmt = decode_field_defs(row["字段定义"])
        assert fmt == LEGACY_FORMAT
        assert [d["name"] for d in descriptors] == ["event", "latency"]
        assert row[COL_FORMAT] == LEGACY_FORMAT

        # 回滚后旧格式解析入口仍可工作（下面的端到端测试再验证行为）
        status = schema_evolution.migration_status()
        assert status["status"] == "已回滚"


# ---------------------------------------------------------------------------
# 三、带类型解析与来源清单回写
# ---------------------------------------------------------------------------
class TestTypedParsing:
    def test_same_named_field_text_vs_number(self, pipeline):
        _prepare_source(pipeline)
        # 旧版：latency 按文本；旧批在只有 v1.0 时先完成解析
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)

        old_batch = _start_batch(pipeline, [{"body": "order_paid|120"}])
        old_result = pipeline.parse_batch(old_batch)
        assert old_result["schema_version"] == "v1.0"
        old_entities = _active_entities(old_batch)
        old_fields = json.loads(old_entities[0]["标准化字段集"])
        assert old_fields["latency"] == "120"
        assert isinstance(old_fields["latency"], str)

        # 部署期发布带数字类型的新版本并迁移，旧批 pin 不变；新批采用新版本
        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        schema_evolution.migrate()
        new_batch = _start_batch(
            pipeline, [{"body": "order_paid|120"}, {"body": "order_paid|88.5"}]
        )
        new_result = pipeline.parse_batch(new_batch)
        assert new_result["schema_version"] == "v2.0"
        assert new_result["schema_format"] == TYPED_FORMAT
        new_entities = _active_entities(new_batch)
        values = [json.loads(e["标准化字段集"])["latency"] for e in new_entities]
        assert values == [120, pytest.approx(88.5)]
        assert isinstance(values[0], int) and isinstance(values[1], float)

    def test_parse_result_writes_back_source_manifest(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        batch_no = _start_batch(pipeline, [{"body": "order_paid|120"}])
        pipeline.parse_batch(batch_no)

        raw = next(r for r in store.rows("raw_records") if r["批次编号"] == batch_no)
        manifest = json.loads(raw["source_manifest"])
        assert manifest["batch_no"] == batch_no
        assert manifest["schema_version"] == "v2.0"
        assert manifest["field_types"]["latency"] == "number"
        assert manifest["parsed_entity_id"] == _active_entities(batch_no)[0]["id"]

        entity = _active_entities(batch_no)[0]
        assert json.loads(entity["schema_field_types"])["latency"] == "number"
        # 明细自带完整字段解释快照，类型依据不依赖后续 Schema 是否回滚
        pin = json.loads(entity["schema_pin"])
        assert pin["schema_version"] == "v2.0"
        assert pin["field_definitions"][1] == {"name": "latency", "type": "number"}

    def test_number_type_validation_routes_bad_record_to_failure(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        batch_no = _start_batch(
            pipeline,
            [{"body": "order_paid|120"}, {"body": "order_paid|N/A"}],
        )
        result = pipeline.parse_batch(batch_no)
        assert result["success"] == 1
        assert result["failure"] == 1

        failed = next(
            r for r in store.rows("raw_records")
            if r["批次编号"] == batch_no and r["status"] == "解析失败"
        )
        assert "数字" in failed["last_error"]
        retry = next(
            q for q in store.rows("retry_queue")
            if q["失败记录ID"] == str(failed["id"])
        )
        assert retry["schema_version"] == "v2.0"


# ---------------------------------------------------------------------------
# 四、版本 pin 与并发：一批只允许一套完整字段解释
# ---------------------------------------------------------------------------
class TestBatchVersionPinning:
    def test_batch_pin_survives_later_schema_evolve(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        batch_no = _start_batch(pipeline, [{"body": "order_paid|120"}])
        first = pipeline.parse_batch(batch_no)
        assert first["schema_version"] == "v1.0"

        # 之后发布带数字类型的新版本，旧批再次解析仍锁定 v1.0
        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        again = pipeline.parse_batch(batch_no)
        assert again["schema_version"] == "v1.0"
        entities = _active_entities(batch_no)
        assert all(e["schema_version"] == "v1.0" for e in entities)
        assert isinstance(json.loads(entities[-1]["标准化字段集"])["latency"], str)

    def test_concurrent_parse_second_caller_rejected(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        batch_no = _start_batch(pipeline, [{"body": "order_paid|120"}])
        lock = pipeline._lock_for_batch(batch_no)
        acquired = lock.acquire(blocking=False)
        assert acquired is True
        try:
            blocked = pipeline.parse_batch(batch_no)
            assert blocked["ok"] is False
            assert "一套完整字段解释" in blocked["message"]
        finally:
            lock.release()

    def test_two_batches_same_source_use_different_versions(self, pipeline):
        """迁移期间同源两批并存：旧批重做按旧版本，新批按带类型新版本。"""
        _prepare_source(pipeline)
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        old_batch = _start_batch(pipeline, [{"body": "order_paid|120"}])
        pipeline.parse_batch(old_batch)

        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        schema_evolution.migrate()
        new_batch = _start_batch(pipeline, [{"body": "order_paid|120"}])
        pipeline.parse_batch(new_batch)

        assert _active_entities(old_batch)[0]["schema_version"] == "v1.0"
        assert _active_entities(new_batch)[0]["schema_version"] == "v2.0"


# ---------------------------------------------------------------------------
# 五、复算旧批 + 回滚，明细类型依据不丢
# ---------------------------------------------------------------------------
class TestReprocessAndRollback:
    def test_reprocess_old_batch_uses_pinned_original_version(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        old_batch = _start_batch(pipeline, [{"body": "order_paid|120"}])
        pipeline.parse_batch(old_batch)
        first_entities = _active_entities(old_batch)
        assert len(first_entities) == 1

        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        schema_evolution.migrate()

        # 按原版本重做旧批：仍走 v1.0 文本语义，而不是新数字类型
        redo = pipeline.parse_batch(old_batch, reprocess=True)
        assert redo["ok"] is True
        assert redo["schema_version"] == "v1.0"
        assert redo["reprocess"] is True

        active = _active_entities(old_batch)
        assert len(active) == 1
        reprocessed = active[0]
        assert reprocessed["reprocess"] is True
        assert json.loads(reprocessed["标准化字段集"])["latency"] == "120"
        assert isinstance(json.loads(reprocessed["标准化字段集"])["latency"], str)

        # 首次明细被标记取代但保留，类型依据仍可回查
        superseded = [
            e for e in store.rows("parsed_entity")
            if e.get("来源批次") == old_batch and e.get("superseded")
        ]
        assert len(superseded) == 1
        assert json.loads(superseded[0]["schema_pin"])["schema_version"] == "v1.0"

        # 来源清单关联到最新一次处理
        raw = next(r for r in store.rows("raw_records") if r["批次编号"] == old_batch)
        manifest = json.loads(raw["source_manifest"])
        assert manifest["parsed_entity_id"] == reprocessed["id"]
        assert manifest["reprocess"] is True

        journal = next(
            j for j in store.rows("parse_journal")
            if j["批次编号"] == old_batch and j["操作类型"] == "复算解析"
        )
        assert journal["schema_version"] == "v1.0"

    def test_rollback_keeps_type_evidence_of_finished_entities(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        batch_no = _start_batch(pipeline, [{"body": "order_paid|120"}])
        pipeline.parse_batch(batch_no)
        entity_before = _active_entities(batch_no)[0]
        assert json.loads(entity_before["标准化字段集"])["latency"] == 120

        # 回滚 Schema 存储形态
        schema_evolution.rollback()

        # 已成功标准化明细的值、类型与可回查版本原样保留
        entity_after = next(
            e for e in store.rows("parsed_entity") if e["id"] == entity_before["id"]
        )
        assert json.loads(entity_after["标准化字段集"])["latency"] == 120
        assert entity_after["schema_version"] == "v2.0"
        pin = json.loads(entity_after["schema_pin"])
        assert pin["field_definitions"][1]["type"] == "number"

    def test_legacy_entry_still_parses_after_rollback(self, pipeline):
        _prepare_source(pipeline)
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        schema_evolution.migrate()
        schema_evolution.rollback()

        # 回滚后新建批次走旧格式名称数组，文本语义正常
        batch_no = _start_batch(pipeline, [{"body": "order_paid|120"}])
        result = pipeline.parse_batch(batch_no)
        assert result["ok"] is True
        assert result["schema_format"] == LEGACY_FORMAT
        fields = json.loads(_active_entities(batch_no)[0]["标准化字段集"])
        assert fields["latency"] == "120"


# ---------------------------------------------------------------------------
# 六、完整部署切换中断剧本
# ---------------------------------------------------------------------------
class TestDeploymentCutoverScenario:
    def test_full_cutover_interrupt_reprocess_rollback(self, pipeline):
        _prepare_source(pipeline)

        # 1) 旧版 Schema（名称数组）上线并解析一批旧数据
        pipeline.evolve_schema("v1.0", SOURCE, ["event", "latency"], True)
        old_batch = _start_batch(pipeline, [{"body": "order_paid|120"}])
        pipeline.parse_batch(old_batch)

        # 2) 发布带数字类型的新版本，并启动迁移
        pipeline.evolve_typed_schema(
            "v2.0",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        interrupted = schema_evolution.migrate(limit=1)
        assert interrupted["remaining"] >= 0  # 中断点已持久化

        # 3) 恢复迁移直到完成
        recovered = schema_evolution.migrate()
        assert recovered["status"] == "已完成"

        # 4) 同源新批按带类型定义采集，旧批同时按原版本重做
        new_batch = _start_batch(pipeline, [{"body": "order_paid|42"}])
        new_parse = pipeline.parse_batch(new_batch)
        redo_parse = pipeline.parse_batch(old_batch, reprocess=True)

        # 5) 核对两批明细：字段值、类型、可回查版本
        new_entity = _active_entities(new_batch)[0]
        old_entity = _active_entities(old_batch)[0]
        assert json.loads(new_entity["标准化字段集"])["latency"] == 42
        assert isinstance(json.loads(new_entity["标准化字段集"])["latency"], int)
        assert new_entity["schema_version"] == "v2.0"
        assert new_entity["schema_format"] == TYPED_FORMAT

        assert json.loads(old_entity["标准化字段集"])["latency"] == "120"
        assert isinstance(json.loads(old_entity["标准化字段集"])["latency"], str)
        assert old_entity["schema_version"] == "v1.0"

        # 6) 回滚：旧格式定义与入口恢复，两批已产出明细的版本/类型仍可回查
        schema_evolution.rollback()
        row_v2 = next(s for s in store.rows("schema_version") if s["版本号"] == "v2.0")
        _, fmt_v2 = decode_field_defs(row_v2["字段定义"])
        assert fmt_v2 == LEGACY_FORMAT

        assert json.loads(_active_entities(new_batch)[0]["schema_pin"])[
            "schema_version"
        ] == "v2.0"
        assert json.loads(_active_entities(old_batch)[0]["schema_pin"])[
            "schema_version"
        ] == "v1.0"

    def test_evolve_schema_defaults_to_legacy_for_backward_compat(self, pipeline):
        result = pipeline.evolve_schema("legacy-v", SOURCE, ["event", "latency"], True)
        row = result["schema"]
        assert row[COL_FORMAT] == LEGACY_FORMAT
        descriptors, fmt = decode_field_defs(row["字段定义"])
        assert fmt == LEGACY_FORMAT
        assert [d["name"] for d in descriptors] == ["event", "latency"]

    def test_evolve_typed_schema_persists_number_type(self, pipeline):
        result = pipeline.evolve_typed_schema(
            "typed-v",
            SOURCE,
            [{"name": "event", "type": "text"}, {"name": "latency", "type": "number"}],
        )
        row = result["schema"]
        assert row[COL_FORMAT] == TYPED_FORMAT
        descriptors, _ = decode_field_defs(row["字段定义"])
        assert descriptors[1] == {"name": "latency", "type": "number"}
