import json

from process_csv import process


def test_process_generates_expected_outputs(tmp_path):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text(
        "plcVariableName;mbRegister;mbFunctionCode;mbType;multiplier;addressOffset;mbUsed\n"
        "Temp_#;100;3;UINT16;2;2;1\n"
        "Pressure_#;200;4;UINT32;1;0;true\n",
        encoding="utf-8",
    )

    outdir = tmp_path / "run_output_test"
    result = process(str(input_csv), str(outdir), save_processed=True)

    assert len(result) == 2
    assert (outdir / "processed_variables.csv").exists()
    assert (outdir / "mb_handler_summary.txt").exists()
    assert (outdir / "configs").is_dir()

    temp_json = outdir / "configs" / "Temp__.json"
    assert temp_json.exists()
    with open(temp_json, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    assert isinstance(data["mbHandler"], int)
    assert isinstance(data["mbIdx"], int)
    assert isinstance(data["mbUsed"], bool)
