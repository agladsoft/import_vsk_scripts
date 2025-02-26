import pytest
import os
import json
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from src.scripts.flat_import_vsk import ImportVSK


@pytest.fixture
def sample_dataframe():
    data = {
        "Год": [2023],
        "Месяц": ["Январь"],
        "Дата отгрузки": ["2024-08-16"],
        "ИНН": [None],
        "Направление": ["импорт"],
    }
    return pd.DataFrame(data)


@pytest.fixture
def temp_excel_file(tmp_path, sample_dataframe):
    file_path = tmp_path / "test.xlsx"
    sample_dataframe.to_excel(file_path, index=False)
    return file_path


@pytest.fixture
def temp_output_folder(tmp_path):
    return tmp_path / "output"


@pytest.fixture
def import_vsk(temp_excel_file, temp_output_folder):
    temp_output_folder.mkdir()
    return ImportVSK(str(temp_excel_file), str(temp_output_folder))


def test_read_file(import_vsk):
    df = import_vsk.read_file()
    assert not df.empty
    assert "Год" in df.columns
    assert "Дата отгрузки" in df.columns


def test_transformation_df(import_vsk, sample_dataframe):
    df_transformed = import_vsk.transformation_df(sample_dataframe)
    assert "year" in df_transformed.columns
    assert "shipment_date" in df_transformed.columns


def test_add_new_columns(import_vsk, sample_dataframe):
    import_vsk.add_new_columns(sample_dataframe)
    assert "original_file_name" in sample_dataframe.columns
    assert "original_file_parsed_on" in sample_dataframe.columns


def test_change_type_and_values(sample_dataframe):
    ImportVSK.change_type_and_values(sample_dataframe)
    sample_dataframe = ImportVSK.transformation_df(sample_dataframe)
    assert sample_dataframe["shipment_date"].dtype == object


def test_write_to_json(import_vsk, sample_dataframe, temp_output_folder):
    sample_dataframe = import_vsk.transformation_df(sample_dataframe)
    parsed_data = sample_dataframe.to_dict("records")
    import_vsk.write_to_json(parsed_data)
    output_file = temp_output_folder / "test.xlsx.json"
    assert output_file.exists()
    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        assert len(data) == 1
        assert data[0]["year"] == 2023


def test_get_port(import_vsk, sample_dataframe, monkeypatch):
    monkeypatch.setattr("src.scripts.parsed.unified_list_line_name", lambda: {"ARKAS": ["ARKAS"]})
    sample_dataframe = import_vsk.transformation_df(sample_dataframe)
    df = import_vsk.get_port(sample_dataframe)
    assert df["direction"].iloc[0] == "import"


def test_main(import_vsk,monkeypatch):
    monkeypatch.setattr("src.scripts.parsed.unified_list_line_name", lambda: {"ARKAS": ["ARKAS"]})
    import_vsk.main()
    output_file = os.path.join(import_vsk.output_folder, os.path.basename(import_vsk.input_file_path) + ".json")
    assert os.path.exists(output_file)
