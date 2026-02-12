"""Validate generated output folders for CSV/config consistency."""

import argparse
import csv
import json
import os
from collections import defaultdict

REQUIRED_KEYS = {
    "mbHandler",
    "mbIdx",
    "mbRegister",
    "mbFunctionCode",
    "mbTypeSize",
}

FLOAT_FIELDS = {
    "mqttLowerLimit",
    "mqttUpperLimit",
    "mbScaling",
    "mbOffset",
    "mqttScaling",
    "mqttOffset",
}


def iter_run_outputs(base_dir: str):
    for name in os.listdir(base_dir):
        if name.startswith("run_output_"):
            path = os.path.join(base_dir, name)
            if os.path.isdir(path):
                yield path


def load_processed_csv(path: str):
    with open(path, "r", encoding="utf-8", newline="") as fh:
        sample = fh.read(2048)
        if ";" not in sample:
            raise ValueError("processed_variables.csv is not semicolon-separated")
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        rows = list(reader)
    return reader.fieldnames or [], rows


def is_number(val):
    return isinstance(val, (int, float)) and not isinstance(val, bool)


def validate_json(path: str):
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    errors = []

    missing = REQUIRED_KEYS - set(data.keys())
    if missing:
        errors.append(f"missing keys: {sorted(missing)}")

    for k, v in data.items():
        if isinstance(v, str) and v.strip() == "":
            errors.append(f"empty string field: {k}")
        if k == "mbUsed":
            if not isinstance(v, bool):
                errors.append("mbUsed is not boolean")
            continue
        if is_number(v):
            if k in FLOAT_FIELDS:
                if not isinstance(v, float):
                    errors.append(f"non-float number in field: {k}")
            else:
                if not isinstance(v, int):
                    errors.append(f"non-int number in field: {k}")

    return errors, data


def validate_mbidx(rows):
    errors = []
    by_handler = defaultdict(list)
    for r in rows:
        try:
            h = int(float(r.get("mbHandler", "0")))
            idx = int(float(r.get("mbIdx", "0")))
        except Exception:
            errors.append("mbHandler/mbIdx not numeric in processed CSV")
            return errors
        by_handler[h].append(idx)

    for h, idxs in by_handler.items():
        if not idxs:
            continue
        expected = list(range(1, len(idxs) + 1))
        if sorted(idxs) != expected:
            errors.append(f"mbIdx not contiguous for mbHandler {h}")
    return errors


def validate_handler_limit(rows, max_handler):
    try:
        handlers = [int(float(r.get("mbHandler", "0"))) for r in rows]
    except Exception:
        return ["mbHandler not numeric in processed CSV"]
    if not handlers:
        return []
    max_seen = max(handlers)
    if max_seen > max_handler:
        return [f"mbHandler exceeds limit: {max_seen} > {max_handler}"]
    return []


def main():
    p = argparse.ArgumentParser(description="Validate run_output_* folders")
    p.add_argument(
        "--base-dir",
        default=os.path.dirname(__file__),
        help="Base directory containing run_output_* folders",
    )
    p.add_argument(
        "--max-mbhandler",
        type=int,
        default=30,
        help="Maximum allowed mbHandler value",
    )
    args = p.parse_args()

    failures = 0
    for outdir in iter_run_outputs(args.base_dir):
        local_failures = 0
        processed = os.path.join(outdir, "processed_variables.csv")
        configs_dir = os.path.join(outdir, "configs")
        if not os.path.exists(processed):
            print(f"[FAIL] {outdir}: missing processed_variables.csv")
            failures += 1
            local_failures += 1
            continue
        if not os.path.isdir(configs_dir):
            print(f"[FAIL] {outdir}: missing configs/ directory")
            failures += 1
            local_failures += 1
            continue

        try:
            fields, rows = load_processed_csv(processed)
        except Exception as exc:
            print(f"[FAIL] {outdir}: {exc}")
            failures += 1
            local_failures += 1
            continue

        if "mbIdx" not in fields:
            print(f"[FAIL] {outdir}: processed_variables.csv missing mbIdx column")
            failures += 1
            local_failures += 1
            continue

        mbidx_errors = validate_mbidx(rows)
        for e in mbidx_errors:
            print(f"[FAIL] {outdir}: {e}")
        failures += len(mbidx_errors)
        local_failures += len(mbidx_errors)

        hlimit_errors = validate_handler_limit(rows, args.max_mbhandler)
        for e in hlimit_errors:
            print(f"[FAIL] {outdir}: {e}")
        failures += len(hlimit_errors)
        local_failures += len(hlimit_errors)

        for name in os.listdir(configs_dir):
            if not name.endswith(".json"):
                continue
            path = os.path.join(configs_dir, name)
            errors, _ = validate_json(path)
            for e in errors:
                print(f"[FAIL] {outdir}/{name}: {e}")
            failures += len(errors)
            local_failures += len(errors)

        if local_failures == 0:
            print(f"[OK] {outdir}")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
