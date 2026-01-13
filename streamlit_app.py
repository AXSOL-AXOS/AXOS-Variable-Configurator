from __future__ import annotations

from pathlib import Path
from typing import Optional
from datetime import datetime
import shutil

import pandas as pd
import streamlit as st

from axos_var_configurator.baseline import apply_baseline, plan_baseline
from axos_var_configurator.csvio import detect_csv_kind, load_axsol_abstractions_by_prefix, read_csv_rows
from axos_var_configurator.exporter import export_device_json
from axos_var_configurator.csvio import scan_database


DEFAULT_DB_PATH = Path(
    r"C:\Users\SimonFeuerbacher\AXSOL GmbH\AXSOL - 05_Entwicklung\20_Software und Steuerung\99_DataTransfer\DataBase_Devices"
)
DEFAULT_BASELINE_EXCLUDES = ["AXSOL_DataBase_Collection_AD_temp_.csv"]
DEFAULT_UI_BACKUP_DIR = (Path.cwd() / "ui_backups").resolve()


def _resolve_db_path(db: Optional[str]) -> Path:
    if db and db.strip():
        return Path(db).expanduser().resolve()
    return DEFAULT_DB_PATH.expanduser().resolve()


def _id_column_for_kind(kind: str) -> str:
    if kind == "device":
        return "Topic"
    if kind == "axsol_abstraction":
        return "AXSOL_Name_Short"
    return ""


def _sanitize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        df[c] = df[c].astype("string")
        df[c] = df[c].fillna("")
    return df


def _sanitize_folder(name: str) -> str:
    name = name.strip()
    if not name:
        return "_"
    import re

    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def _planned_changes(old_df: pd.DataFrame, new_df: pd.DataFrame, id_col: str) -> dict[str, object]:
    old_df = _sanitize_df(old_df)
    new_df = _sanitize_df(new_df)

    old_ids = set(old_df[id_col].astype(str).str.strip()) if id_col in old_df.columns else set()
    new_ids = set(new_df[id_col].astype(str).str.strip()) if id_col in new_df.columns else set()

    added = sorted([i for i in new_ids if i and i not in old_ids])
    removed = sorted([i for i in old_ids if i and i not in new_ids])

    old_by_id = {str(r[id_col]).strip(): r for r in old_df.to_dict(orient="records") if str(r.get(id_col, "")).strip()}
    new_by_id = {str(r[id_col]).strip(): r for r in new_df.to_dict(orient="records") if str(r.get(id_col, "")).strip()}

    changed: list[dict[str, str]] = []
    for rid in sorted(old_ids.intersection(new_ids)):
        o = old_by_id.get(rid)
        n = new_by_id.get(rid)
        if o is None or n is None:
            continue
        for col in new_df.columns:
            ov = "" if o.get(col) is None else str(o.get(col))
            nv = "" if n.get(col) is None else str(n.get(col))
            if ov != nv:
                changed.append({"row": rid, "field": col, "before": ov, "after": nv})

    return {"added": added, "removed": removed, "changed": changed}


def _write_csv_from_df(csv_path: Path, original_fieldnames: list[str], df: pd.DataFrame) -> None:
    df = _sanitize_df(df)

    out_rows: list[dict[str, str]] = []
    records = df.to_dict(orient="records")
    for r in records:
        out_rows.append({fn: ("" if r.get(fn) is None else str(r.get(fn))) for fn in original_fieldnames})

    from axos_var_configurator.edit import _atomic_write

    _atomic_write(csv_path, fieldnames=original_fieldnames, rows=out_rows)


def _backup_csv(csv_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = DEFAULT_UI_BACKUP_DIR / ts
    out_dir.mkdir(parents=True, exist_ok=True)
    dst = out_dir / csv_path.name
    shutil.copy2(csv_path, dst)
    return dst


def main() -> None:
    st.set_page_config(page_title="AXOS Variable Configurator", layout="wide")

    st.title("AXOS Variable Configurator")

    db_input = st.text_input("Database folder", value=str(DEFAULT_DB_PATH))
    db_path = _resolve_db_path(db_input)

    if not db_path.exists():
        st.error("Database folder does not exist")
        return

    tab_edit, tab_export, tab_baseline = st.tabs(["Edit CSV", "Export JSON", "Baselines"])

    with tab_edit:
        st.subheader("Edit CSV")
        scan = scan_database(db_path)
        csvs = sorted([*scan.axsol_abstraction_files, *scan.device_files, *scan.unknown_files], key=lambda p: p.name.lower())

        selected = st.selectbox("CSV file", options=csvs, format_func=lambda p: p.name)
        kind = detect_csv_kind(selected)
        id_col = _id_column_for_kind(kind)

        fieldnames, rows = read_csv_rows(selected)
        if not fieldnames:
            st.warning("Selected CSV has no header")
            return

        df = pd.DataFrame(rows, columns=fieldnames)
        df = _sanitize_df(df)

        st.caption(f"Detected kind: {kind}")
        if id_col:
            st.caption(f"Identifier column: {id_col}")
        else:
            st.warning("This CSV kind is not editable (unknown schema).")
            return

        if "_original_df" not in st.session_state or st.session_state.get("_original_path") != str(selected):
            st.session_state["_original_df"] = df.copy()
            st.session_state["_original_path"] = str(selected)

        edited_df = st.data_editor(
            df,
            num_rows="dynamic",
            use_container_width=True,
            key="editor",
        )
        edited_df = _sanitize_df(edited_df)

        st.markdown("### Planned changes")
        plan = _planned_changes(st.session_state["_original_df"], edited_df, id_col=id_col)

        st.write({"added": len(plan["added"]), "removed": len(plan["removed"]), "changed": len(plan["changed"])})

        if plan["changed"]:
            st.dataframe(pd.DataFrame(plan["changed"]), use_container_width=True)
        if plan["added"]:
            st.info(f"Added rows: {plan['added']}")
        if plan["removed"]:
            st.warning(f"Removed rows: {plan['removed']}")

        st.markdown("### Safeguard")
        confirm = st.checkbox("I understand this will write changes to the CSV file.")
        typed = st.text_input("Type APPLY to enable saving", value="")

        can_apply = confirm and typed.strip() == "APPLY"
        if st.button("Apply changes", type="primary", disabled=not can_apply):
            if plan["removed"]:
                st.error("Row deletions are currently blocked in UI for safety. Please undo deletions (or use CLI).")
            else:
                backup_path = _backup_csv(selected)
                _write_csv_from_df(selected, original_fieldnames=fieldnames, df=edited_df)
                st.success(f"Saved. Backup created at: {backup_path}")
                st.session_state["_original_df"] = edited_df.copy()

    with tab_export:
        st.subheader("Export JSON")

        scan = scan_database(db_path)
        device_files = scan.device_files

        export_all = st.checkbox("Export all devices", value=True)
        mode = st.selectbox("Mode", options=["both", "original", "axsol"], index=0)
        out_dir = st.text_input("Output folder", value=str((Path.cwd() / "out_all").resolve()))

        if export_all:
            selection = device_files
        else:
            selection = st.multiselect("Select device CSVs", options=device_files, format_func=lambda p: p.name)

        if st.button("Run export", type="primary"):
            abstractions = load_axsol_abstractions_by_prefix(db_path)
            out = Path(out_dir).expanduser().resolve()
            out.mkdir(parents=True, exist_ok=True)

            modes = [mode] if mode != "both" else ["original", "axsol"]
            for device_csv in selection:
                device_folder = out / _sanitize_folder(device_csv.stem)
                for m in modes:
                    export_device_json(device_csv=device_csv, out_dir=device_folder / m, mode=m, abstractions=abstractions)
            st.success(f"Exported {len(selection)} device(s) to {out}")

    with tab_baseline:
        st.subheader("Baselines")

        name = st.text_input("Baseline name", value="baseline_001")
        repo_root = Path.cwd().resolve()

        plan = plan_baseline(
            name=name,
            db_path=db_path,
            repo_root=repo_root,
            exclude_names=DEFAULT_BASELINE_EXCLUDES,
        )

        st.write(
            {
                "output": str(plan.baseline_dir),
                "files": len(plan.include_files),
                "excluded": plan.exclude_names,
            }
        )

        confirm_baseline = st.checkbox("I understand this will create a baseline snapshot folder in this repo.")
        if st.button("Create baseline", type="primary", disabled=not confirm_baseline):
            out = apply_baseline(plan)
            st.success(f"Created baseline at {out}")


if __name__ == "__main__":
    main()
