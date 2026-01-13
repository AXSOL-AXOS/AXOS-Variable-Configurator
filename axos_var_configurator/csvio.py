from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from rich.console import Console

console = Console()


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
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            # Read the first line and split it into columns
            first_line = next(f).strip()
            # Clean up the header by removing any extra spaces and converting to lowercase
            header = [h.strip().lower() for h in first_line.split(",") if h.strip()]
            
            # Debug output
            print(f"Detected headers: {header}")
            
            # Check for device CSV
            has_topic = "topic" in header
            has_register = "register address" in header or "register adress" in header
            
            if has_topic and has_register:
                print("Detected device CSV")
                return "device"
                
            # Check for AXSOL abstraction CSV
            has_axsol_short = "axsol_name_short" in header or any(h.startswith("unnamed:") for h in header)
            has_axsol_name = "axsol name" in header
            
            if has_axsol_short and has_axsol_name:
                print("Detected AXSOL abstraction CSV")
                return "axsol_abstraction"
                
            # If we get here, we couldn't determine the type
            print("Could not determine CSV type")
            return "unknown"
            
    except Exception as e:
        print(f"Error detecting CSV kind: {e}")
        # Try to read the file to see what's in it
        try:
            with path.open("r", encoding="utf-8-sig") as f:
                print(f"First 100 chars of file: {f.read(100)}")
        except Exception as e2:
            print(f"Could not read file: {e2}")
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
        # Read all lines and clean them up
        lines = [line.strip() for line in f if line.strip()]
        if not lines:
            return ([], [])

        # Read the header
        header_line = lines[0]
        # Use csv.reader to properly handle quoted fields in the header
        header_reader = csv.reader([header_line])
        fieldnames = [h.strip() for h in next(header_reader) if h.strip()]

        # Create a CSV reader for the remaining lines
        reader = csv.DictReader(lines[1:], fieldnames=fieldnames)

        rows: list[dict[str, str]] = []
        for row in reader:
            # Clean up the row data
            cleaned_row = {}
            for k, v in row.items():
                if k is None:
                    continue
                clean_k = k.strip() if k else ""
                clean_v = v.strip() if v is not None else ""
                cleaned_row[clean_k] = clean_v
            rows.append(cleaned_row)

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

        # For device CSVs, the column names are different from abstraction CSVs
        # In device CSVs, we have direct column names like 'upperLimit' and 'lowerLimit'
        unit_h = "AXSOL Unit & Resolution"
        scaling_h = "Scaling"
        lim_down_h = "lowerLimit"
        lim_up_h = "upperLimit"
        
        # Debug output for column mapping
        console.print(f"[dim]Using direct column mapping - Unit: {unit_h}, Scaling: {scaling_h}, Lower: {lim_down_h}, Upper: {lim_up_h}")
        
        # For abstraction CSVs, we need to try different variations
        if not any(h in hmap for h in [unit_h, 'ax_unit', 'axsol_unit']):
            # This is likely an abstraction CSV, use the old logic
            unit_h = (
                hmap.get("ax_unit") or 
                hmap.get("axsol_unit_&_resolution") or
                hmap.get("axsol unit & resolution") or
                hmap.get("axsol_unit_and_resolution")
            )
            
            scaling_h = hmap.get("ax_scaling") or hmap.get("ax_scaling_factor")
            
            # Try different variations of limit column names
            lim_down_h = (
                hmap.get("ax_limitdown") or 
                hmap.get("ax_limit_down") or
                hmap.get("lowerLimit") or
                hmap.get("lower_limit") or
                hmap.get("lower limit") or
                "lowerLimit"  # Default fallback
            )
            
            lim_up_h = (
                hmap.get("ax_limitup") or 
                hmap.get("ax_limit_up") or
                hmap.get("upperLimit") or 
                hmap.get("upper_limit") or
                hmap.get("upper limit") or
                "upperLimit"  # Default fallback
            )
            
            console.print(f"[dim]Using abstraction mapping - Unit: {unit_h}, Scaling: {scaling_h}, Lower: {lim_down_h}, Upper: {lim_up_h}")

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
