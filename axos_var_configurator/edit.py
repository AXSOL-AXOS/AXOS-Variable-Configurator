from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from axos_var_configurator.csvio import detect_csv_kind, read_csv_rows, write_csv_rows


@dataclass(frozen=True)
class PlannedChange:
    csv_path: Path
    action: str
    row_id: str
    field: str
    before: str
    after: str


def _atomic_write(csv_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    csv_path = csv_path.resolve()
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=csv_path.stem + "_", suffix=".tmp", dir=str(csv_path.parent))
    os.close(tmp_fd)

    tmp_p = Path(tmp_path)
    try:
        write_csv_rows(tmp_p, fieldnames=fieldnames, rows=rows)
        tmp_p.replace(csv_path)
    finally:
        if tmp_p.exists():
            tmp_p.unlink(missing_ok=True)


def _id_column_for(csv_path: Path) -> str:
    kind = detect_csv_kind(csv_path)
    if kind == "device":
        return "Topic"
    if kind == "axsol_abstraction":
        return "AXSOL_Name_Short"
    raise ValueError(f"Unsupported CSV kind for edit: {csv_path}")


def apply_set_field(
    csv_path: Path,
    row_id: str,
    field: str,
    value: str,
    apply: bool,
) -> list[PlannedChange]:
    fieldnames, rows = read_csv_rows(csv_path)
    if not fieldnames:
        raise ValueError(f"No header found: {csv_path}")

    id_col = _id_column_for(csv_path)
    id_col_real = None
    field_real = None

    for fn in fieldnames:
        if fn.strip().lower() == id_col.strip().lower():
            id_col_real = fn
        if fn.strip().lower() == field.strip().lower():
            field_real = fn

    if id_col_real is None:
        raise ValueError(f"Missing identifier column {id_col} in {csv_path.name}")
    if field_real is None:
        raise ValueError(f"Unknown column {field} in {csv_path.name}")

    planned: list[PlannedChange] = []
    matched = 0

    for r in rows:
        rid = (r.get(id_col_real) or "").strip()
        if rid == row_id:
            before = r.get(field_real, "")
            after = value
            if before != after:
                planned.append(
                    PlannedChange(
                        csv_path=csv_path,
                        action="set",
                        row_id=row_id,
                        field=field_real,
                        before=before,
                        after=after,
                    )
                )
                r[field_real] = after
            matched += 1

    if matched == 0:
        raise ValueError(f"Row not found: {row_id}")

    if apply and planned:
        _atomic_write(csv_path, fieldnames=fieldnames, rows=rows)

    return planned


def apply_add_row(csv_path: Path, row: dict[str, Any], apply: bool) -> list[PlannedChange]:
    fieldnames, rows = read_csv_rows(csv_path)
    if not fieldnames:
        raise ValueError(f"No header found: {csv_path}")

    id_col = _id_column_for(csv_path)

    id_col_real = None
    for fn in fieldnames:
        if fn.strip().lower() == id_col.strip().lower():
            id_col_real = fn
            break

    if id_col_real is None:
        raise ValueError(f"Missing identifier column {id_col} in {csv_path.name}")

    id_val = None
    for k, v in row.items():
        if str(k).strip().lower() == id_col.strip().lower():
            id_val = str(v)
            break

    if not id_val:
        raise ValueError(f"Row must include identifier column {id_col}")

    existing = set((r.get(id_col_real) or "").strip() for r in rows)
    if id_val in existing:
        raise ValueError(f"Row already exists: {id_val}")

    new_row = {fn: "" for fn in fieldnames}
    for k, v in row.items():
        for fn in fieldnames:
            if fn.strip().lower() == str(k).strip().lower():
                new_row[fn] = "" if v is None else str(v)

    rows.append(new_row)

    planned = [
        PlannedChange(
            csv_path=csv_path,
            action="add",
            row_id=id_val,
            field="*",
            before="",
            after="(row added)",
        )
    ]

    if apply:
        _atomic_write(csv_path, fieldnames=fieldnames, rows=rows)

    return planned


def format_planned_changes(changes: list[PlannedChange]) -> str:
    if not changes:
        return "No changes"

    lines: list[str] = []
    for ch in changes:
        if ch.action == "add":
            lines.append(f"ADD {ch.csv_path.name} row_id={ch.row_id}")
        else:
            lines.append(
                f"SET {ch.csv_path.name} row_id={ch.row_id} field={ch.field} before={ch.before!r} after={ch.after!r}"
            )
    return "\n".join(lines)
