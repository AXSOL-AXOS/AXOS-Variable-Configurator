"""Process CSV into processed variables and per-variable JSON configs.

This module provides a small CLI utility that reads a semicolon-separated CSV
containing PLC variable definitions and converts it into:

- a processed CSV (optional) without expanding `#` placeholders; iteration is
  meant to happen in the PLC, so the base rows are preserved and enriched with
  `mbTypeSize`, `mbHandler`, `mbHandlerOffset`, and `mbIdx`.
- a `configs/` directory containing one JSON file per `plcVariableName` with
  all resulting columns serialized.
- a text summary `mb_handler_summary.txt` describing handlers per device
  (iteration index), including start address and length.

Usage example:
    python process_csv.py --input AXSOL_DataBase_Collection_WSTech_MCS-Series.csv --outdir .

Notes:
- Input CSV is expected to use `;` as separator.
- Rows that contain numeric `multiplier` are expanded only for internal
  calculations to determine handler offsets (no `#` replacement).
- `mbTypeSize` is inferred from `mbType` (e.g. `UINT16` -> 16). Unknown types
  default to 16.

"""

import argparse
import json
import os
import re
from typing import Dict

import pandas as pd


def infer_type_size(t):
    """Infer Modbus type size from an mbType string.

    Parameters
    - t: value from `mbType` column. Can be None/NaN or strings such as
      'UINT16', 'INT32', 'FLOAT', or other vendor-specific names that may end
      with a numeric bit-size.

    Returns
    - integer number of bits (16, 32, 64, ...). Defaults to 16 when unknown.
    """
    if pd.isna(t):
        return 32
    s = str(t).strip().upper()
    type_map = {
        "UINT16": 16,
        "INT16": 16,
        "UINT": 16,
        "UINT32": 32,
        "INT32": 32,
        "FLOAT": 32,
    }
    if s in type_map:
        return type_map[s]
    # If the type name ends with digits, assume those digits represent size
    m = re.search(r"(\d+)$", s)
    if m:
        return int(m.group(1))
    # Fallback default
    return 32


def expand_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Expand rows according to `multiplier`, without relying on `#`.

    Behavior:
    - If `multiplier` > 1 the row is duplicated `multiplier` times.
    - In each duplicate `mbRegister` is incremented by `addressOffset * iteration`.
    - Names are kept as-is (no `#` replacement).

    Returns a new DataFrame with expanded rows and reset index.
    """
    expanded = []
    for _, row in df.iterrows():
        plc = str(row.get("plcVariableName", ""))
        # defensive parsing of numeric-like fields
        try:
            multiplier = int(row.get("multiplier", 1)) if pd.notna(row.get("multiplier", 1)) else 1
        except Exception:
            multiplier = 1
        try:
            address_offset = (
                int(row.get("addressOffset", 0)) if pd.notna(row.get("addressOffset", 0)) else 0
            )
        except Exception:
            address_offset = 0
        base_register = int(row.get("mbRegister", 0)) if pd.notna(row.get("mbRegister", 0)) else 0

        if multiplier > 1:
            for i in range(multiplier):
                new = row.copy()
                new["__iteration"] = i
                # keep names as-is; only shift address by iteration
                new["plcVariableName"] = plc
                new["mbRegister"] = base_register + (i * address_offset)
                expanded.append(new)
        else:
            new = row.copy()
            new["__iteration"] = 0
            expanded.append(new)
    # return a fresh DataFrame (drop original indexing)
    return pd.DataFrame(expanded).reset_index(drop=True)


def assign_mb_handler(df: pd.DataFrame) -> pd.DataFrame:
    """Assign `mbHandler` values to the DataFrame rows.

    Rules implemented:
    - A handler can contain up to 124 variables.
    - A handler may only contain variables with the same `mbFunctionCode`.
    - Variables assigned to the same handler must have consecutive addresses
      (accounting for the data size). For example, a 32-bit variable occupies two
      16-bit addresses, therefore address increments are computed as `mbTypeSize/16`.

    The function walks the rows in ascending `mbRegister` order and starts a
    new handler whenever one of the rules would be violated.
    """
    # Initialize handler tracking
    df["mbHandler"] = 0
    current_handler = 0
    current_function = None
    current_count = 0
    last_address = None
    last_size = None

    for idx, row in df.iterrows():
        func = row.get("mbFunctionCode")
        addr = int(row.get("mbRegister"))
        size = int(row.get("mbTypeSize"))

        # Determine if the current row can continue the current handler
        continue_same = False
        if (
            current_function is not None
            and func == current_function
            and current_count + size // 16 < 124
            and last_address is not None
        ):
            # expected next address depends on last variable size
            expected = last_address + (last_size // 16)
            if addr == expected:
                continue_same = True

        if not continue_same:
            # start a new handler
            current_handler += 1
            current_count = 0
            current_function = func

        current_count += size // 16
        df.at[idx, "mbHandler"] = int(current_handler)
        last_address = addr
        last_size = size

    return df


def write_handler_summary(expanded_df: pd.DataFrame, outdir: str) -> str:
    """Write a flat list of all handlers (no grouping).

    The list includes the total number of handlers and, for each handler,
    its start address and length (in 16-bit registers).
    """
    if "__iteration" not in expanded_df.columns:
        return ""

    df = expanded_df.copy()
    if df.empty:
        return ""

    # Compute register length per row (number of 16-bit registers)
    df["__reg_len"] = df["mbTypeSize"].astype(int) // 16
    df["__reg_end"] = df["mbRegister"].astype(int) + df["__reg_len"] - 1

    handlers = []
    # Group by handler across all rows to create a flat list of all handlers
    for handler_id, h_rows in df.groupby("mbHandler"):
        start_addr = int(h_rows["mbRegister"].min())
        end_addr = int(h_rows["__reg_end"].max())
        length = int(end_addr - start_addr + 1)
        handlers.append((int(handler_id), start_addr, length))

    handlers.sort(key=lambda x: x[0])
    summary_lines = [f"Handlers: {len(handlers)}"]
    for handler_id, start_addr, length in handlers:
        summary_lines.append(f"Handler {handler_id}: start={start_addr}, length={length}")

    out_path = os.path.join(outdir, "mb_handler_summary.txt")
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(summary_lines).rstrip() + "\n")
    return out_path


def sanitize_filename(name: str) -> str:
    """Return a safe filename for the given variable name.

    Non-alphanumeric characters (except `_`, `-` and `.`) are replaced with
    underscore to avoid filesystem problems across platforms.
    """
    # remove problematic characters
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", name)


def normalize_mqtt_name(value: str) -> str:
    """Normalize mqttName by stripping and collapsing internal whitespace."""
    return " ".join(str(value).split())


def process(input_csv: str, outdir: str, save_processed: bool = True) -> pd.DataFrame:
    """Process the input CSV and generate outputs.

    Steps performed:
    1. Read the semicolon-separated CSV and normalize column names.
    2. Expand rows using `multiplier` via `expand_rows` for internal handler
       calculations only (names are not expanded).
    3. Infer `mbTypeSize` from `mbType` (fallback to 16 bits).
    4. Ensure `mbRegister` is an integer and sort the expanded table by it.
    5. Assign `mbHandler` on the expanded table.
    6. Map iteration-0 handler back to base rows and compute `mbHandlerOffset`
       between iteration 0 and 1.
    7. Optionally save `processed_variables.csv` to `outdir`.
    8. Emit one JSON file per variable under `outdir/configs/`.
    9. Write `mb_handler_summary.txt` with handler ranges.

    Parameters
    - input_csv: path to input CSV file
    - outdir: output directory where `processed_variables.csv` and `configs/` will be
      created (if saving enabled)
    - save_processed: whether to write `processed_variables.csv` to disk

    Returns the final DataFrame of processed variables.
    """
    # Read input CSV; detect delimiter and handle encoding
    with open(input_csv, "rb") as fh:
        header_bytes = fh.readline()
    try:
        header_line = header_bytes.decode("utf-8", errors="strict")
        encoding = "utf-8"
    except UnicodeDecodeError:
        header_line = header_bytes.decode("latin1")
        encoding = "latin1"
    if "\t" in header_line:
        sep = "\t"
    elif ";" in header_line:
        sep = ";"
    else:
        sep = ","
    try:
        df = pd.read_csv(input_csv, sep=sep, encoding=encoding)
    except UnicodeDecodeError:
        df = pd.read_csv(input_csv, sep=sep, encoding="latin1")
    df.columns = df.columns.str.strip()
    # Drop rows with mbUsed == 0/false if the column exists
    if "mbUsed" in df.columns:

        def _is_used(val):
            if pd.isna(val):
                return True
            if isinstance(val, (int, float)):
                return int(val) != 0
            s = str(val).strip().lower()
            return s not in ("0", "0.0", "false")

        df = df[df["mbUsed"].apply(_is_used)].reset_index(drop=True)
    df["__base_index"] = range(len(df))

    # Expand rows for internal calculations (names unchanged)
    expanded_df = expand_rows(df)

    # Infer the mbTypeSize (in bits) from mbType values
    if "mbType" in expanded_df.columns:
        expanded_df["mbTypeSize"] = expanded_df["mbType"].apply(infer_type_size)
    else:
        expanded_df["mbTypeSize"] = 16

    # Ensure mbRegister is integer (addresses are integral)
    expanded_df["mbRegister"] = expanded_df["mbRegister"].astype(int)

    # Sort by function code first, then register to keep handlers consistent
    expanded_df = expanded_df.sort_values(
        by=["mbFunctionCode", "mbRegister"],
    ).reset_index(drop=True)

    # Assign grouping/handler numbers
    expanded_df = assign_mb_handler(expanded_df)

    # Map handler and payload from iteration 0 back to the base rows
    iter0 = expanded_df[expanded_df["__iteration"] == 0].set_index("__base_index")
    iter1 = expanded_df[expanded_df["__iteration"] == 1].set_index("__base_index")
    df["mbHandler"] = df["__base_index"].map(iter0["mbHandler"])

    # Compute mbHandlerOffset for rows with multiplier/addressOffset
    offsets = (iter1["mbHandler"] - iter0["mbHandler"]).astype(float)
    df["mbHandlerOffset"] = df["__base_index"].map(offsets)
    df["mbHandlerOffset"] = df["mbHandlerOffset"].fillna(0).astype(int)

    # Assign per-handler index (1-based) for variables inside each handler
    df = df.reset_index().rename(columns={"index": "__row"})
    sorted_df = df.sort_values(by=["mbHandler", "mbRegister", "__row"]).copy()
    sorted_df["mbIdx"] = sorted_df.groupby("mbHandler").cumcount() + 1
    df = sorted_df.sort_values(by="__row").drop(columns=["__row"]).reset_index(drop=True)

    # Copy mbTypeSize to the base output for completeness
    if "mbType" in df.columns:
        df["mbTypeSize"] = df["mbType"].apply(infer_type_size)
    else:
        df["mbTypeSize"] = 16

    # Ensure mqttPayload does not appear in outputs
    if "mqttPayload" in df.columns:
        df = df.drop(columns=["mqttPayload"])

    # Drop helper column before writing outputs
    if "__base_index" in df.columns:
        df = df.drop(columns=["__base_index"])

    # Ensure outputs directory exists and save the processed CSV if requested
    os.makedirs(outdir, exist_ok=True)
    if save_processed:
        out_csv = os.path.join(outdir, "processed_variables.csv")
        df.to_csv(out_csv, sep=";", index=False)

    # Write handler summary text file
    write_handler_summary(expanded_df, outdir)

    # Create per-variable JSON configs
    configs_dir = os.path.join(outdir, "configs")
    os.makedirs(configs_dir, exist_ok=True)

    for _, row in df.iterrows():
        name = str(row.get("plcVariableName", "variable"))
        filename = sanitize_filename(name) + ".json"
        path = os.path.join(configs_dir, filename)
        # convert each pandas/np value to native python types for JSON
        record: Dict = {}
        float_fields = {
            "mqttLowerLimit",
            "mqttUpperLimit",
            "mbScaling",
            "mbOffset",
            "mqttScaling",
            "mqttOffset",
        }
        for col in df.columns:
            val = row[col]
            if pd.isna(val):
                continue
            if isinstance(val, str) and val.strip() == "":
                continue
            try:
                val = val.item() if hasattr(val, "item") else val
            except Exception:
                pass
            # Coerce mbUsed to boolean (unquoted true/false in JSON)
            if col == "mbUsed":
                if isinstance(val, str):
                    v = val.strip().lower()
                    if v in ("true", "false"):
                        record[col] = v == "true"
                        continue
                    try:
                        record[col] = bool(int(v))
                        continue
                    except Exception:
                        pass
                if isinstance(val, (int, float)):
                    record[col] = bool(int(val))
                    continue
            if col == "mqttName":
                record[col] = normalize_mqtt_name(val)
                continue
            # Coerce selected fields to float, all other numbers to int
            if isinstance(val, (int, float)):
                if col in float_fields:
                    record[col] = float(val)
                else:
                    record[col] = int(val)
            else:
                record[col] = val

        with open(path, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, ensure_ascii=False)

    return df


def main():
    """CLI entry point.

    Parses command line arguments and dispatches to `process`.
    """
    p = argparse.ArgumentParser(
        description="Process CSV into processed_variables and per-variable JSON configs"
    )
    p.add_argument("--input", "-i", default=None, help="Input CSV path")
    p.add_argument("--outdir", "-o", default=os.path.dirname(__file__), help="Output directory")
    p.add_argument(
        "--no-save-processed",
        dest="save_processed",
        action="store_false",
        help="Do not save processed_variables.csv",
    )
    args = p.parse_args()

    if not args.input:
        raise SystemExit(
            "Missing --input. Example: python process_csv.py --input path/to/input.csv --outdir ."
        )

    df = process(args.input, args.outdir, save_processed=args.save_processed)
    configs_dir = os.path.join(args.outdir, "configs")
    print(f"Processed {len(df)} variables. Configs written to: {configs_dir}")


if __name__ == "__main__":
    main()
