from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from axos_var_configurator.csvio import (
    DatabaseScanResult,
    detect_csv_kind,
    load_axsol_abstractions_by_prefix,
    scan_database,
)
from axos_var_configurator.baseline import apply_baseline, plan_baseline
from axos_var_configurator.edit import (
    apply_add_row,
    apply_set_field,
    format_planned_changes,
)
from axos_var_configurator.exporter import export_device_json

app = typer.Typer(add_completion=False)
console = Console()

DEFAULT_DB_PATH = Path(
    r"C:\Users\SimonFeuerbacher\AXSOL GmbH\AXSOL - 05_Entwicklung\20_Software und Steuerung\99_DataTransfer\DataBase_Devices"
)

DEFAULT_BASELINE_EXCLUDES = ["AXSOL_DataBase_Collection_AD_temp_.csv"]


def _resolve_db_path(db: Optional[Path]) -> Path:
    return (db or DEFAULT_DB_PATH).expanduser().resolve()


def _sanitize_folder(name: str) -> str:
    name = name.strip()
    if not name:
        return "_"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


@app.command("scan")
def scan_cmd(
    db: Optional[Path] = typer.Option(None, "--db", help="Folder containing the CSV database"),
    details: bool = typer.Option(False, "--details", help="List detected files per category"),
) -> None:
    db_path = _resolve_db_path(db)
    result = scan_database(db_path)

    console.print(f"DB: {db_path}")
    console.print(f"AXSOL abstraction CSVs: {len(result.axsol_abstraction_files)}")
    console.print(f"Device CSVs: {len(result.device_files)}")
    console.print(f"Other/Unknown CSVs: {len(result.unknown_files)}")

    if details:
        if result.axsol_abstraction_files:
            console.print("\nAXSOL abstraction:")
            for p in result.axsol_abstraction_files:
                console.print(f"- {p.name}")
        if result.device_files:
            console.print("\nDevices:")
            for p in result.device_files:
                console.print(f"- {p.name}")
        if result.unknown_files:
            console.print("\nUnknown:")
            for p in result.unknown_files:
                console.print(f"- {p.name}")


@app.command("validate")
def validate_cmd(
    db: Optional[Path] = typer.Option(None, "--db", help="Folder containing the CSV database"),
) -> None:
    db_path = _resolve_db_path(db)
    result = scan_database(db_path)

    problems: list[str] = []

    for p in result.axsol_abstraction_files:
        kind = detect_csv_kind(p)
        if kind != "axsol_abstraction":
            problems.append(f"Expected AXSOL abstraction but detected {kind}: {p.name}")

    for p in result.device_files:
        kind = detect_csv_kind(p)
        if kind != "device":
            problems.append(f"Expected device but detected {kind}: {p.name}")

    if problems:
        console.print("Validation problems:")
        for pr in problems:
            console.print(f"- {pr}")
        raise typer.Exit(code=1)

    console.print("OK")


@app.command("export-json")
def export_json_cmd(
    device_csv: Path = typer.Argument(..., help="Path to a device CSV file"),
    out: Path = typer.Option(..., "--out", help="Output folder"),
    mode: str = typer.Option("original", "--mode", help="original|axsol"),
    db: Optional[Path] = typer.Option(None, "--db", help="Folder containing the CSV database (for abstraction join)"),
) -> None:
    if mode not in {"original", "axsol"}:
        raise typer.BadParameter("mode must be 'original' or 'axsol'")

    db_path = _resolve_db_path(db)
    abstractions = load_axsol_abstractions_by_prefix(db_path)
    export_device_json(device_csv=device_csv, out_dir=out, mode=mode, abstractions=abstractions)


@app.command("export-json-all")
def export_json_all_cmd(
    out: Path = typer.Option(..., "--out", help="Output folder"),
    mode: str = typer.Option("both", "--mode", help="original|axsol|both"),
    db: Optional[Path] = typer.Option(None, "--db", help="Folder containing the CSV database"),
) -> None:
    if mode not in {"original", "axsol", "both"}:
        raise typer.BadParameter("mode must be 'original', 'axsol', or 'both'")

    db_path = _resolve_db_path(db)
    scan = scan_database(db_path)
    abstractions = load_axsol_abstractions_by_prefix(db_path)

    out = out.resolve()
    out.mkdir(parents=True, exist_ok=True)

    modes = [mode] if mode != "both" else ["original", "axsol"]
    console.print(f"Devices: {len(scan.device_files)}")
    console.print(f"Output: {out}")
    console.print(f"Modes: {', '.join(modes)}")

    for device_csv in scan.device_files:
        device_folder = out / _sanitize_folder(device_csv.stem)
        for m in modes:
            target = device_folder / m
            export_device_json(device_csv=device_csv, out_dir=target, mode=m, abstractions=abstractions)
        console.print(f"Exported: {device_csv.name}")


@app.command("baseline-plan")
def baseline_plan_cmd(
    name: str = typer.Argument(..., help="Baseline name"),
    db: Optional[Path] = typer.Option(None, "--db", help="Folder containing the CSV database"),
) -> None:
    db_path = _resolve_db_path(db)
    repo_root = Path.cwd().resolve()
    plan = plan_baseline(name=name, db_path=db_path, repo_root=repo_root, exclude_names=DEFAULT_BASELINE_EXCLUDES)
    console.print(f"Baseline: {plan.name}")
    console.print(f"Output: {plan.baseline_dir}")
    console.print(f"Files: {len(plan.include_files)}")
    for p in plan.include_files:
        console.print(f"- {p.name}")


@app.command("baseline-apply")
def baseline_apply_cmd(
    name: str = typer.Argument(..., help="Baseline name"),
    apply: bool = typer.Option(False, "--apply", help="Actually write baseline snapshot"),
    db: Optional[Path] = typer.Option(None, "--db", help="Folder containing the CSV database"),
) -> None:
    db_path = _resolve_db_path(db)
    repo_root = Path.cwd().resolve()
    plan = plan_baseline(name=name, db_path=db_path, repo_root=repo_root, exclude_names=DEFAULT_BASELINE_EXCLUDES)

    if not apply:
        console.print("Dry-run. Use --apply to create the baseline snapshot.")
        console.print(f"Would create: {plan.baseline_dir}")
        console.print(f"Files: {len(plan.include_files)}")
        raise typer.Exit(code=0)

    out_dir = apply_baseline(plan)
    console.print(f"Created baseline at: {out_dir}")


@app.command("plan-set")
def plan_set_cmd(
    csv_path: Path = typer.Argument(..., help="CSV file to edit"),
    row_id: str = typer.Argument(..., help="Row identifier (Topic for device CSVs, AXSOL_Name_Short for abstraction CSVs)"),
    field: str = typer.Argument(..., help="Column name to change"),
    value: str = typer.Argument(..., help="New value"),
) -> None:
    planned = apply_set_field(csv_path=csv_path, row_id=row_id, field=field, value=value, apply=False)
    console.print(format_planned_changes(planned))


@app.command("apply-set")
def apply_set_cmd(
    csv_path: Path = typer.Argument(..., help="CSV file to edit"),
    row_id: str = typer.Argument(..., help="Row identifier (Topic for device CSVs, AXSOL_Name_Short for abstraction CSVs)"),
    field: str = typer.Argument(..., help="Column name to change"),
    value: str = typer.Argument(..., help="New value"),
) -> None:
    planned = apply_set_field(csv_path=csv_path, row_id=row_id, field=field, value=value, apply=True)
    console.print(format_planned_changes(planned))


@app.command("plan-add")
def plan_add_cmd(
    csv_path: Path = typer.Argument(..., help="CSV file to edit"),
    json_row: str = typer.Argument(..., help="Row as JSON object (keys are column names)"),
) -> None:
    row = json.loads(json_row)
    if not isinstance(row, dict):
        raise typer.BadParameter("json_row must be a JSON object")

    planned = apply_add_row(csv_path=csv_path, row=row, apply=False)
    console.print(format_planned_changes(planned))


@app.command("apply-add")
def apply_add_cmd(
    csv_path: Path = typer.Argument(..., help="CSV file to edit"),
    json_row: str = typer.Argument(..., help="Row as JSON object (keys are column names)"),
) -> None:
    row = json.loads(json_row)
    if not isinstance(row, dict):
        raise typer.BadParameter("json_row must be a JSON object")

    planned = apply_add_row(csv_path=csv_path, row=row, apply=True)
    console.print(format_planned_changes(planned))
