from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional


@dataclass(frozen=True)
class DatabaseScanResult:
    axsol_abstraction_files: list[Path]
    device_files: list[Path]
    unknown_files: list[Path]


def iter_csv_files(db_path: Path) -> Iterable[Path]:
    if not db_path.exists() or not db_path.is_dir():
        return []
    return sorted(p for p in db_path.iterdir() if p.is_file() and p.suffix.lower() == ".csv")


def read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return []
    return [h.strip() for h in header]


def detect_csv_kind(path: Path) -> str:
    header = read_csv_header(path)

    if not header:
        return "unknown"

    header_l = [h.lower() for h in header]

    # Prefer device detection first: vendor CSVs can also contain "AXSOL Name" columns,
    # and some exports include "Unnamed:" columns.
    if "topic" in header_l and ("register address" in header_l or "register adress" in header_l):
        return "device"

    # AXSOL abstraction CSVs exist in multiple variants.
    # Variant A: "AXSOL_Name_Short" + "AXSOL Name"
    # Variant B: first column exported as "Unnamed: 0" + "AXSOL Name"
    if ("axsol_name_short" in header_l or any(h.startswith("unnamed:") for h in header_l)) and "axsol name" in header_l:
        return "axsol_abstraction"

    return "unknown"


def scan_database(db_path: Path) -> DatabaseScanResult:
    axsol: list[Path] = []
    device: list[Path] = []
    unknown: list[Path] = []

    for p in iter_csv_files(db_path):
        kind = detect_csv_kind(p)
        if kind == "axsol_abstraction":
            axsol.append(p)
        elif kind == "device":
            device.append(p)
        else:
            unknown.append(p)

    return DatabaseScanResult(axsol_abstraction_files=axsol, device_files=device, unknown_files=unknown)


def _normalize_header_name(name: str) -> str:
    return name.strip().lower().replace(" ", "_")


def _axsol_name_keys(name: str) -> list[str]:
    raw = str(name)
    base = raw.strip().strip(",")
    collapsed = " ".join(base.split())
    no_space = collapsed.replace(" ", "")

    keys: list[str] = []
    for k in [base, collapsed, no_space, base.lower(), collapsed.lower(), no_space.lower()]:
        k2 = k.strip()
        if k2 and k2 not in keys:
            keys.append(k2)
    return keys


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return ([], [])
        fieldnames = list(reader.fieldnames)

        rows: list[dict[str, str]] = []
        for r in reader:
            row: dict[str, str] = {}
            for k, v in r.items():
                if k is None:
                    continue
                row[k] = "" if v is None else str(v)
            rows.append(row)

    return (fieldnames, rows)


def write_csv_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow({fn: r.get(fn, "") for fn in fieldnames})


@dataclass(frozen=True)
class AxsOlAbstractionRow:
    short_name: str
    long_name: str
    unit: Optional[str]
    scaling: Optional[str]
    limit_down: Optional[str]
    limit_up: Optional[str]


def load_axsol_abstractions_by_prefix(db_path: Path) -> dict[str, dict[str, AxsOlAbstractionRow]]:
    """Returns: prefix (AX_Container) -> long_name -> abstraction row"""

    result: dict[str, dict[str, AxsOlAbstractionRow]] = {}
    scan = scan_database(db_path)

    for p in scan.axsol_abstraction_files:
        fieldnames, rows = read_csv_rows(p)
        if not fieldnames:
            continue

        hmap = {_normalize_header_name(h): h for h in fieldnames}

        short_h = hmap.get("axsol_name_short")
        if short_h is None:
            # Some files have the short name in an "Unnamed: 0" column.
            for hn, original in hmap.items():
                if hn.startswith("unnamed:"):
                    short_h = original
                    break
        axsol_name_h = hmap.get("axsol_name")

        if short_h is None or axsol_name_h is None:
            continue

        unit_h = hmap.get("ax_unit") or hmap.get("axsol_unit_&_resolution")
        scaling_h = hmap.get("ax_scaling")
        lim_down_h = hmap.get("ax_limitdown")
        lim_up_h = hmap.get("ax_limitup")

        for r in rows:
            long_val = (r.get(axsol_name_h) or "").strip()
            short_val = (r.get(short_h) or "").strip()
            if not long_val or not short_val:
                continue

            prefix = long_val.split()[0].strip()
            unit_val = (r.get(unit_h) if unit_h else None)
            scaling_val = (r.get(scaling_h) if scaling_h else None)
            lim_d = (r.get(lim_down_h) if lim_down_h else None)
            lim_u = (r.get(lim_up_h) if lim_up_h else None)

            abs_row = AxsOlAbstractionRow(
                short_name=short_val,
                long_name=long_val,
                unit=(unit_val.strip() if isinstance(unit_val, str) else None),
                scaling=(scaling_val.strip() if isinstance(scaling_val, str) else None),
                limit_down=(lim_d.strip() if isinstance(lim_d, str) else None),
                limit_up=(lim_u.strip() if isinstance(lim_u, str) else None),
            )

            if prefix not in result:
                result[prefix] = {}

            for key in _axsol_name_keys(long_val):
                result[prefix].setdefault(key, abs_row)

    return result
