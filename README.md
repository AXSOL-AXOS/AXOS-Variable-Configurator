# AXOS Variable Configurator

A powerful tool for managing and exporting device configurations in the AXSOL ecosystem. This project provides both a command-line interface and a Streamlit-based web interface for working with device variable configurations.

## Features

- **CSV to JSON Conversion**: Convert device configuration CSVs to standardized JSON format
- **AXSOL Integration**: Map device-specific parameters to AXSOL's standardized format
- **Unit Variable Expansion**: Automatically expand unit variables with configurable multipliers and address offsets
- **Streamlit UI**: User-friendly web interface for browsing and editing configurations
- **Batch Processing**: Process multiple device configurations in one go
- **Safe File Operations**: Atomic writes and backup functionality to prevent data loss

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/axos-var-configurator.git
   cd axos-var-configurator
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command Line Interface

```bash
# Export device configuration to JSON
python -m axos_var_configurator.cli export /path/to/device.csv --output /output/directory

# Export in AXSOL format
python -m axos_var_configurator.cli export /path/to/device.csv --format axsol --output /output/directory

# Batch export all devices from a directory
python -m axos_var_configurator.cli batch-export /path/to/devices/ --output /output/directory
```

### Web Interface

```bash
# Start the Streamlit web interface
streamlit run streamlit_app.py
```

Then open your browser to `http://localhost:8501`

## Features in Detail

### Unit Variable Expansion

The configurator supports automatic expansion of unit variables with patterns like `U#_` or `U_#` in MQTT names. For example:

- `U#_MEAS_VOLTAGE` with multiplier 3 becomes:
  - `U1_MEAS_VOLTAGE`
  - `U2_MEAS_VOLTAGE`
  - `U3_MEAS_VOLTAGE`

### Safe File Operations

- Atomic writes to prevent partial file corruption
- Automatic backups before overwriting files
- Confirmation prompts for destructive operations

## Project Structure

```
axos_var_configurator/
├── __init__.py
├── cli.py           # Command-line interface
├── csvio.py         # CSV I/O operations
├── exporter.py      # JSON export logic
├── units.py         # Unit conversion utilities
└── baseline.py      # Configuration versioning
```

## Dependencies

- Python 3.8+
- typer
- rich
- streamlit
- pandas

## License

This project is proprietary software. All rights reserved.

## Contributing

For internal contributions only. Please contact the maintainers for access.

## Support

For support, please contact the development team at [your-email@example.com](mailto:your-email@example.com).
