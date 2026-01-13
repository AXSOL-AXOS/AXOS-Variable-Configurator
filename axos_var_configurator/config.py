from pathlib import Path

# Default path to the AXOS_Configs repository (now a submodule)
DEFAULT_CONFIGS_PATH = Path(__file__).parent.parent / "configs"

# Path to the database devices directory (CSV files)
DATABASE_DEVICES_PATH = DEFAULT_CONFIGS_PATH / "TRLY2501" / "csvs"

# Ensure the directory exists
DATABASE_DEVICES_PATH.mkdir(parents=True, exist_ok=True)
