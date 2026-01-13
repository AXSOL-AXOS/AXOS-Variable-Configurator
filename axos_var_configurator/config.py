from pathlib import Path

# Default path to the AXOS_Configs repository
# Update this path to point to your local clone of AXSOL-AXOS/AXOS_Configs
DEFAULT_CONFIGS_PATH = Path.home() / "AXSOL_Configs"

# Path to the database devices directory
DATABASE_DEVICES_PATH = DEFAULT_CONFIGS_PATH / "TRLY2501" / "configs"

# Ensure the directory exists
DATABASE_DEVICES_PATH.mkdir(parents=True, exist_ok=True)
