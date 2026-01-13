from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Optional, Dict, Any

from axos_var_configurator.csvio import AxsOlAbstractionRow, read_csv_rows
from rich.console import Console

console = Console()
from axos_var_configurator.units import convert_unit_factor
from rich.console import Console

console = Console()


def _write_json_file(file_path: Path, data: Dict[str, Any], force_overwrite: bool = False) -> None:
    """Write JSON data to a file with atomic writes.
    
    Args:
        file_path: Path to the output file
        data: Data to write as JSON
        force_overwrite: If True, existing files will be overwritten without warning
    """
    # Ensure parent directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to a temporary file first, then rename atomically
    temp_path = file_path.with_suffix('.tmp')
    try:
        temp_path.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )
        # On Windows, we need to handle the case where the destination exists
        if file_path.exists() and not force_overwrite:
            file_path.unlink()
        temp_path.rename(file_path)
        console.print(f"[green]Wrote {file_path}")
    except Exception as e:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        console.print(f"[red]Error writing {file_path}: {e}")
        raise


def _needs_expansion(topic: Optional[str], multiplier: Optional[str]) -> tuple[bool, int]:
    """Returns (needs_expand, multiplier).

    Expands if either:
    - Multiplier > 1, or
    - Topic contains U#_ or U_# (in which case use multiplier=1 if not set).
    """
    mult = _try_int(multiplier) or 1
    if mult < 1:
        mult = 1

    has_u_placeholder = topic and ("U#_" in topic or "U_#" in topic)
    if has_u_placeholder and mult == 1:
        mult = 1  # Keep mult=1 if U#_ is present but no explicit multiplier
        needs_expand = True
    else:
        needs_expand = mult > 1

    return needs_expand, mult


def _sanitize_filename(name: str) -> str:
    name = name.strip()
    if not name:
        return "_"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", name)


def _try_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(str(s).strip())
    except Exception:
        return None


def _try_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).strip())
    except Exception:
        return None


def _normalize_axsol_long_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    s = str(name).strip()
    if not s:
        return None
    # Some device CSVs include trailing commas (e.g. copied from Excel)
    s = s.strip().strip(",")
    s = " ".join(s.split())
    return s if s else None


def _axsol_lookup_keys(name: str) -> list[str]:
    base = _normalize_axsol_long_name(name) or ""
    no_space = base.replace(" ", "")
    keys: list[str] = []
    for k in [base, base.lower(), no_space, no_space.lower()]:
        k2 = k.strip()
        if k2 and k2 not in keys:
            keys.append(k2)
    return keys


def _expand_name_template(template: Optional[str], idx: int) -> Optional[str]:
    if template is None:
        return None
    s = str(template)
    return s.replace("#", str(idx))


def _expand_mqtt_name(template: Optional[str], idx: int) -> Optional[str]:
    if template is None:
        return None
    s = str(template)

    # AXSOL abstraction uses U#_... to mark unit values.
    s = s.replace("U#_", f"U{idx}_")
    s = s.replace("U_#_", f"U{idx}_")
    s = s.replace("U_#", f"U{idx}")

    # If someone used a generic # placeholder in mqttName, expand it too.
    s = s.replace("#", str(idx))
    return s


def _parse_unit_and_scaling_blob(blob: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Very tolerant parser for vendor CSVs that combine unit+scaling in one column.

    If parsing fails, returns (unit=blob, scaling=None).
    """

    if blob is None:
        return (None, None)
    s = str(blob).strip()
    if not s:
        return (None, None)

    # Common cases seen:
    # - "V"
    # - "0.1V/bit" (sometimes stored elsewhere, but we try)
    m = re.match(r"^([0-9]*\.?[0-9]+)\s*([A-Za-z°%]+.*)$", s)
    if m:
        return (m.group(2).strip(), m.group(1).strip())

    return (s, None)


def export_device_json(
    device_csv: Path,
    out_dir: Path,
    mode: str,
    abstractions: dict[str, dict[str, AxsOlAbstractionRow]],
    force_overwrite: bool = False,
) -> None:
    """Export device CSV to JSON files with optional overwrite confirmation.
    
    Args:
        device_csv: Path to the device CSV file
        out_dir: Directory to write JSON files to
        mode: Export mode ('axsol' or 'native')
        abstractions: AXSOL abstraction data
        force_overwrite: If True, existing files will be overwritten without confirmation
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    fieldnames, rows = read_csv_rows(device_csv)
    if not fieldnames:
        raise ValueError(f"No CSV header found: {device_csv}")
        
    # Check for existing files first if not in force mode
    if not force_overwrite:
        existing_files = []
        for row in rows:
            topic = row.get("Topic", "")
            if not topic:
                continue
                
            multiplier = row.get("Multiplier", "1")
            needs_expand, mult_i = _needs_expansion(topic, multiplier)
            
            if needs_expand:
                for idx in range(1, mult_i + 1):
                    expanded_name = _expand_name_template(topic, idx) or topic
                    out_file = out_dir / f"{_sanitize_filename(str(expanded_name))}.json"
                    if out_file.exists():
                        existing_files.append(out_file)
            else:
                out_file = out_dir / f"{_sanitize_filename(topic)}.json"
                if out_file.exists():
                    existing_files.append(out_file)
        
        # Ask for confirmation if there are existing files
        if existing_files:
            console.print(f"[yellow]Warning: {len(existing_files)} files already exist in the output directory.")
            for i, f in enumerate(existing_files[:5], 1):
                console.print(f"  {i}. {f.name}")
            if len(existing_files) > 5:
                console.print(f"  ... and {len(existing_files) - 5} more")
            
            response = console.input(
                "Overwrite all existing files? [y/N] "
            ).strip().lower()
            
            if response != 'y':
                console.print("[yellow]Export cancelled.")
                return
            
            # User confirmed, set force_overwrite to True for the rest of the export
            force_overwrite = True
    
    # Check for existing files first
    existing_files = []
    for row in rows:
        topic = row["Topic"]
        multiplier = row.get("Multiplier", "1")
        mqtt_name = row.get("MqttName", "")
        
        needs_expand, mult_i = _needs_expansion(topic, multiplier)
        if needs_expand:
            for idx in range(1, mult_i + 1):
                expanded_name = _expand_name_template(topic, idx) or topic
                out_file = out_dir / f"{_sanitize_filename(str(expanded_name))}.json"
                if out_file.exists():
                    existing_files.append(out_file)
        else:
            out_file = out_dir / f"{_sanitize_filename(topic)}.json"
            if out_file.exists():
                existing_files.append(out_file)
    
    # Ask for confirmation if there are existing files and not in force mode
    if existing_files and not force_overwrite:
        console.print(f"[yellow]Warning: {len(existing_files)} files already exist in the output directory.")
        for i, f in enumerate(existing_files[:5], 1):
            console.print(f"  {i}. {f.name}")
        if len(existing_files) > 5:
            console.print(f"  ... and {len(existing_files) - 5} more")
        
        response = console.input(
            "Overwrite all existing files? [y/N] "
        ).strip().lower()
        
        if response != 'y':
            console.print("[yellow]Export cancelled.")
            return
    
    def get(r: dict[str, str], *candidates: str) -> Optional[str]:
        for c in candidates:
            for fn in fieldnames:
                if fn.strip().lower() == c.strip().lower():
                    v = (r.get(fn) or "").strip()
                    return v if v != "" else None
        return None

    for r in rows:
        topic = get(r, "Topic")
        if not topic:
            continue

        reg = get(r, "Register Address", "Register Adress")
        unit = get(r, "Unit")
        scaling = get(r, "Scaling")
        offset = get(r, "Offset")

        if unit is None and scaling is None:
            blob = get(r, "Unit& Scaling", "Unit & Scaling")
            unit, scaling = _parse_unit_and_scaling_blob(blob)
        dtype = get(r, "type")
        axsol_long = _normalize_axsol_long_name(get(r, "AXSOL Name"))

        multiplier = get(r, "Multiplier")
        address_offset = get(r, "AddressOffset", "Address Offset")

        mqtt_name: Optional[str] = None
        ax_unit: Optional[str] = None
        ax_scaling: Optional[str] = None
        ax_lim_down: Optional[str] = None
        ax_lim_up: Optional[str] = None

        abs_row: Optional[AxsOlAbstractionRow] = None

        if axsol_long:
            prefix = axsol_long.split()[0].strip()
            row_map = abstractions.get(prefix, {})
            for key in _axsol_lookup_keys(axsol_long):
                abs_row = row_map.get(key)
                if abs_row:
                    break
            if abs_row:
                mqtt_name = abs_row.short_name
                ax_unit = abs_row.unit
                ax_scaling = abs_row.scaling
                ax_lim_down = abs_row.limit_down
                ax_lim_up = abs_row.limit_up

        out_unit = unit
        out_scaling = scaling
        out_offset = offset
        upper_limit = ax_lim_up if abs_row else None
        lower_limit = ax_lim_down if abs_row else None

        if mode == "axsol" and axsol_long:
            if ax_unit:
                factor = convert_unit_factor(unit, ax_unit)
                s_f = _try_float(scaling)
                o_f = _try_float(offset)
                if s_f is not None:
                    out_scaling = str(s_f * factor)
                if o_f is not None:
                    out_offset = str(o_f * factor)
                out_unit = ax_unit

            if ax_scaling:
                out_scaling = ax_scaling

        payload = {
            "mbRegister": reg,
            "unit": out_unit,
            "scaling": out_scaling,
            "offset": out_offset,
            "upperLimit": upper_limit,
            "lowerLimit": lower_limit,
            "description": axsol_long,
            "mqttName": mqtt_name,
            "type": dtype,
            "nativeName": topic,
        }

        needs_expand, mult_i = _needs_expansion(topic, multiplier)
        if needs_expand and _try_int(multiplier) is None and "#" not in str(topic):
            console.print(f"[yellow]Warning: Multiplier>1 but no '#' in topic name: {topic}")
        offset_i = _try_int(address_offset) or 0

        reg_int = _try_int(reg)

        # Expand multi-unit entries if:
        # - Topic includes '#' (e.g. MCU_#_Grid_Voltage_L1N), or
        # - mqttName includes U#_/U_# (e.g. U#_ME_V_L3N), or
        # - Multiplier > 1 (even without placeholders)
        if needs_expand:
            for idx in range(1, mult_i + 1):
                payload_i = dict(payload)
                payload_i["nativeName"] = _expand_name_template(topic, idx)
                payload_i["mqttName"] = _expand_mqtt_name(mqtt_name, idx)

                if reg_int is not None:
                    payload_i["mbRegister"] = str(reg_int + (idx - 1) * offset_i)

                # Use the expanded native name for the output filename
                out_name = payload_i["nativeName"] or topic
                out_file = out_dir / f"{_sanitize_filename(str(out_name))}.json"
                _write_json_file(out_file, payload_i, force_overwrite)
        else:
            # For non-expanded entries, use the original topic name
            out_file = out_dir / f"{_sanitize_filename(topic)}.json"
            _write_json_file(out_file, payload, force_overwrite)
