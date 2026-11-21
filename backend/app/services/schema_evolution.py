"""Schema 字段定义演进：名称数组(legacy) ↔ 带类型版本描述(typed_v2)。

背景：历史 ``schema_version.字段定义`` 只保存字段名称数组（JSON 字符串数组），
新增的数字字段无法随版本记录类型。本模块提供两种形态的编解码、存量迁移与回滚：

- ``legacy_names``：``["time", "level", "latency"]`` —— 旧格式，全部按文本语义读取；
- ``typed_v2``：``[{"name": "time", "type": "text"}, {"name": "latency", "type": "number"}]``
  —— 带类型的版本描述。

设计约束：

- 两种形态长期共存，解析入口对两者都可读，升级部署期间旧批仍能走旧格式；
- 迁移按版本行逐行原子推进，进度写入 ``schema_migration`` 表，中断后重入自动
  跳过已迁移行（幂等可恢复）；
- 回滚只恢复字段定义形态；已成功产出的标准化明细自带版本/类型快照，不受影响。
"""
from __future__ import annotations

import json
import time
from typing import Any

from app.store import store

# 字段定义形态标记
LEGACY_FORMAT = "legacy_names"
TYPED_FORMAT = "typed_v2"

# 支持的字段类型
TEXT_TYPE = "text"
NUMBER_TYPE = "number"

_TYPE_ALIASES: dict[str, str] = {
    "text": TEXT_TYPE,
    "string": TEXT_TYPE,
    "str": TEXT_TYPE,
    "文本": TEXT_TYPE,
    "字符": TEXT_TYPE,
    "number": NUMBER_TYPE,
    "numeric": NUMBER_TYPE,
    "decimal": NUMBER_TYPE,
    "float": NUMBER_TYPE,
    "int": NUMBER_TYPE,
    "integer": NUMBER_TYPE,
    "double": NUMBER_TYPE,
    "数字": NUMBER_TYPE,
    "数值": NUMBER_TYPE,
}

# 版本行上记录迁移状态的附加列
COL_FORMAT = "字段定义格式"
COL_LEGACY_BACKUP = "字段定义_旧格式"
COL_MIGRATION_STATUS = "迁移状态"
MIGRATION_DONE = "已迁移"
MIGRATION_ROLLED_BACK = "已回滚"

PROGRESS_TABLE = "schema_migration"


class TypeCoercionError(ValueError):
    """原始日志值无法按版本描述的字段类型核对时抛出。"""


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def normalize_type(type_name: str | None) -> str:
    """把外部传入的类型别名归一到平台支持的类型名。"""
    key = str(type_name or TEXT_TYPE).strip().lower()
    normalized = _TYPE_ALIASES.get(key)
    if normalized is None:
        raise TypeCoercionError(f"不支持的字段类型：{type_name!r}")
    return normalized


def normalize_descriptors(field_defs: list[Any]) -> list[dict[str, str]]:
    """把名称数组或混合定义归一为带类型的描述符列表。

    - 纯字符串条目按文本类型处理（保留历史字段的当前文本语义）；
    - 字典条目至少需要 ``name``，``type`` 缺省为文本。
    """
    descriptors: list[dict[str, str]] = []
    for item in field_defs:
        if isinstance(item, str):
            name = item.strip()
            if name:
                descriptors.append({"name": name, "type": TEXT_TYPE})
        elif isinstance(item, dict) and str(item.get("name", "")).strip():
            descriptors.append({
                "name": str(item["name"]).strip(),
                "type": normalize_type(item.get("type")),
            })
        else:
            raise TypeCoercionError(f"非法字段定义条目：{item!r}")
    return descriptors


def encode_descriptors(descriptors: list[dict[str, str]]) -> str:
    return json.dumps(descriptors, ensure_ascii=False)


def decode_field_defs(raw_value: Any) -> tuple[list[dict[str, str]] | None, str]:
    """解析版本行的 ``字段定义`` 列。

    返回 ``(描述符列表, 形态标记)``；无法解析的占位文本返回 ``(None, "")``，
    调用方据此回退到解析规则驱动的旧入口，而不是报错中断。
    """
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None, ""
    try:
        loaded = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return None, ""
    if not isinstance(loaded, list) or not loaded:
        return None, ""
    if all(isinstance(item, str) for item in loaded):
        return normalize_descriptors(loaded), LEGACY_FORMAT
    if all(isinstance(item, dict) and "name" in item for item in loaded):
        return normalize_descriptors(loaded), TYPED_FORMAT
    return None, ""


def coerce_value(raw_text: Any, field_type: str) -> Any:
    """按字段类型核对原始日志片段。

    - 文本类型：维持现有文本语义，返回去空白字符串；
    - 数字类型：整数记 int、小数/科学计数记 float；无法解析则抛
      ``TypeCoercionError``，由流水线把该记录打入解析失败/重试。
    """
    text = "" if raw_text is None else str(raw_text).strip()
    normalized = normalize_type(field_type)
    if normalized == TEXT_TYPE:
        return text
    if not text:
        raise TypeCoercionError("数字字段收到空值")
    try:
        if any(ch in text for ch in ".eE"):
            return float(text)
        return int(text)
    except ValueError as exc:
        raise TypeCoercionError(f"值 {text!r} 无法按数字类型解析") from exc


class SchemaEvolutionService:
    """Schema 字段定义迁移状态机：迁移、进度查询、回滚。"""

    # ---------- 进度表 ----------
    def _progress_row(self) -> dict[str, Any] | None:
        rows = store.rows(PROGRESS_TABLE)
        return rows[0] if rows else None

    def _save_progress(self, **fields: Any) -> dict[str, Any]:
        rows = store.rows(PROGRESS_TABLE)
        if rows:
            row = rows[0]
        else:
            row = {"id": 1}
            rows.append(row)
        row.update(fields)
        row["更新时间"] = _now_iso()
        return row

    def migration_status(self) -> dict[str, Any]:
        row = self._progress_row()
        if row is None:
            return {
                "ok": True,
                "status": "未开始",
                "total": 0,
                "migrated": 0,
                "skipped": 0,
                "remaining": 0,
                "migrated_versions": [],
            }
        return {
            "ok": True,
            "status": row.get("status", "未开始"),
            "total": row.get("total", 0),
            "migrated": row.get("migrated", 0),
            "skipped": row.get("skipped", 0),
            "remaining": row.get("remaining", 0),
            "migrated_versions": list(row.get("migrated_versions", [])),
            "progress": row,
        }

    # ---------- 分类 ----------
    @staticmethod
    def _classify_rows() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """把 schema_version 行分为待迁移/已迁移/不可解析三类。"""
        legacy_rows: list[dict[str, Any]] = []
        typed_rows: list[dict[str, Any]] = []
        skipped_rows: list[dict[str, Any]] = []
        for row in store.rows("schema_version"):
            # 已带形态标记的以标记为准，避免重复处理
            if row.get(COL_FORMAT) == TYPED_FORMAT or row.get(COL_MIGRATION_STATUS) == MIGRATION_DONE:
                typed_rows.append(row)
                continue
            _, fmt = decode_field_defs(row.get("字段定义"))
            if fmt == LEGACY_FORMAT:
                legacy_rows.append(row)
            elif fmt == TYPED_FORMAT:
                typed_rows.append(row)
            else:
                skipped_rows.append(row)
        return legacy_rows, typed_rows, skipped_rows

    # ---------- 迁移（可中断、可恢复） ----------
    def migrate(self, limit: int | None = None) -> dict[str, Any]:
        """把历史名称数组逐行迁移为带类型的版本描述。

        ``limit`` 模拟部署切换中断：本次最多迁移多少行；再次调用时已迁移行
        自动跳过，从断点继续。每个版本行的备份与改写在同一步完成，保证
        中断不会留下半迁移行。
        """
        legacy_rows, typed_rows, skipped_rows = self._classify_rows()

        progress = self._progress_row()
        migrated_versions: list[str] = (
            list(progress.get("migrated_versions", [])) if progress else []
        )
        already_migrated = len(typed_rows)

        processed_this_run = 0
        interrupted = False
        for row in legacy_rows:
            if limit is not None and processed_this_run >= limit:
                interrupted = True
                break
            descriptors, _ = decode_field_defs(row.get("字段定义"))
            if descriptors is None:
                continue
            # 逐行原子：先备份旧形态，再改写为带类型描述，最后落迁移标记
            row[COL_LEGACY_BACKUP] = row.get("字段定义")
            row["字段定义"] = encode_descriptors(descriptors)
            row[COL_FORMAT] = TYPED_FORMAT
            row[COL_MIGRATION_STATUS] = MIGRATION_DONE
            row["field_count"] = len(descriptors)
            migrated_versions.append(str(row.get("版本号")))
            processed_this_run += 1

        legacy_left, typed_after, skipped_after = self._classify_rows()
        total = len(legacy_left) + len(typed_after) + len(skipped_after)
        status = "已中断" if interrupted or (legacy_left and limit is not None) else (
            "已完成" if not legacy_left else "进行中"
        )
        saved = self._save_progress(
            status=status,
            total=total,
            migrated=len(typed_after),
            skipped=len(skipped_after),
            remaining=len(legacy_left),
            migrated_versions=migrated_versions,
            last_run_migrated=processed_this_run,
            already_migrated_before_run=already_migrated,
        )
        return {
            "ok": True,
            "message": "Schema 字段定义迁移" + ("中断，进度已保存" if status == "已中断" else "完成"),
            "migrated_this_run": processed_this_run,
            "already_migrated": already_migrated,
            "remaining": len(legacy_left),
            "skipped": len(skipped_after),
            "status": status,
            "progress": saved,
        }

    # ---------- 回滚 ----------
    def rollback(self) -> dict[str, Any]:
        """恢复旧格式字段定义；旧解析入口重新可用。

        已产出的标准化明细不在回滚范围内——它们自身记录了当时采用的版本号
        与字段类型快照，类型依据不会丢失。
        """
        restored: list[str] = []
        for row in store.rows("schema_version"):
            backup = row.get(COL_LEGACY_BACKUP)
            descriptors, _ = decode_field_defs(row.get("字段定义"))
            if backup:
                # 迁移来的版本：直接还原迁移前的名称数组
                row["字段定义"] = backup
            elif descriptors and row.get(COL_FORMAT) == TYPED_FORMAT:
                # 直接按带类型形态发布的新版本：从描述符降级出名称数组，
                # 让旧格式解析入口在回滚后仍能读取同一批字段
                backup = json.dumps(
                    [d["name"] for d in descriptors], ensure_ascii=False
                )
                row[COL_LEGACY_BACKUP] = row.get("字段定义")
                row["字段定义"] = backup
            else:
                continue
            row[COL_FORMAT] = LEGACY_FORMAT
            row[COL_MIGRATION_STATUS] = MIGRATION_ROLLED_BACK
            restored.append(str(row.get("版本号")))

        saved = self._save_progress(
            status="已回滚",
            restored_versions=restored,
            restored_count=len(restored),
        )
        return {
            "ok": True,
            "message": f"Schema 字段定义已回滚 {len(restored)} 个版本，旧格式解析入口可用",
            "restored_versions": restored,
            "restored_count": len(restored),
            "progress": saved,
        }


schema_evolution = SchemaEvolutionService()
