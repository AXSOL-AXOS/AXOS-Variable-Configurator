# AXOS Variable Configurator

Scripts in this repo:
- `process_csv.py`: transforms an input PLC CSV into `processed_variables.csv`, `configs/*.json`, and `mb_handler_summary.txt`.
- `validate_outputs.py`: validates generated `run_output_*` folders.

## Prerequisites

- Python `3.10+`
- `pip`

## Setup

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r dev-requirements.txt
```

## Run

```bash
python process_csv.py --input path/to/input.csv --outdir run_output_001
```

Arguments:
- `--input, -i`: required path to input CSV.
- `--outdir, -o`: output directory (defaults to repository root).
- `--no-save-processed`: skip writing `processed_variables.csv`.

## Validate Generated Outputs

```bash
python validate_outputs.py --base-dir . --max-mbhandler 30
```

## Maintain The Repository

Quality checks:

```bash
ruff check .
ruff format --check .
pytest -q
```

Enable local pre-commit hooks:

```bash
pre-commit install
pre-commit run --all-files
```

CI:
- GitHub Actions runs linting and tests on every push and pull request via `.github/workflows/ci.yml`.

## Input Expectations

- Delimiter is auto-detected: tab, semicolon, or comma.
- At minimum, include `plcVariableName` and `mbRegister`.
- Optional columns include `multiplier`, `addressOffset`, `mbType`, `mbFunctionCode`, `mbUsed`, etc.
- Rows with `multiplier > 1` are expanded for internal handler calculations only (variable names are not expanded).

## Output Details

- `processed_variables.csv`: semicolon-separated with computed columns (`mbTypeSize`, `mbHandler`, `mbHandlerOffset`, `mbIdx`).
- `configs/`: one JSON file per `plcVariableName`.
- `mb_handler_summary.txt`: handler start/length overview.
