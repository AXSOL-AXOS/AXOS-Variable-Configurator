from pathlib import Path

# Default path to the AXOS_Configs repository (now a submodule)
DEFAULT_CONFIGS_PATH = Path(__file__).parent.parent / "configs"

# Path to the database devices directory (CSV files)
DATABASE_DEVICES_PATH = DEFAULT_CONFIGS_PATH / "TRLY2501" / "csvs"

# Default output directory for generated files
DEFAULT_OUTPUT_DIR = DEFAULT_CONFIGS_PATH / "TRLY2501" / "configs"

# Ensure the directories exist
DATABASE_DEVICES_PATH.mkdir(parents=True, exist_ok=True)
DEFAULT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Available projects
AVAILABLE_PROJECTS = ["TRLY2501"]  # Add more projects as needed
