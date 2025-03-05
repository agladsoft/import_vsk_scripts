import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from src.scripts.parsed import ParsedDf


@pytest.fixture
def sample_df():
    data = {
        'consignment': ['CSYTVS134948', 'МАГВОС24290002'],
        'goods_name': ['АВТОЗАПЧАСТИ (ТОРМОЗНЫЕ КОЛОДКИ)', 'КОНТЕЙНЕРЫ УНИВЕРСАЛЬНЫЕ НИЕ СОБСТВЕННЫЕ'],
        'line': ['ARKAS', 'ARKAS'],
        'direction': ['import', 'cabotage'],
        'tracking_seaport': [None, None]
    }
    return pd.DataFrame(data)


@pytest.fixture()
def parsed_df(sample_df):
    return ParsedDf(sample_df)


@pytest.mark.parametrize('row,answer', [
    ({"line": "ARKAS"}, False),
    ({"line": "ARKA"}, True)
])
def test_check_lines(row: dict, answer: str, parsed_df: ParsedDf, monkeypatch):
    monkeypatch.setattr("src.scripts.parsed.get_line_tracking_empty", lambda: ["ARKAS"])
    assert parsed_df.check_lines(row) == answer


@pytest.mark.parametrize("direction,answer", [
    ("import", "import"),
    ("export", "export"),
    ("cabotage", "cabotage")])
def test_get_direction(direction: str, answer: str, parsed_df):
    assert parsed_df.get_direction(direction) == answer


@pytest.mark.parametrize("consignment,answer", [
    ("VX61CT24000372,VX61CT24000372", "VX61CT24000372"),
    ("VX61CT24000372", "VX61CT24000372")
])
def test_get_number_consignment(consignment: str, answer: str, parsed_df: ParsedDf):
    assert parsed_df.get_number_consignment(consignment) == answer


def mock_get_line_unified(item, line_name: str):
    return line_name


@pytest.mark.parametrize("row,consignment,answer", [
    ({"line": "ARKAS", "consignment": "ARKAS123456", "direction": "export", "some": "Some"},
     "consignment", {"line": "ARKAS", "consignment": "ARKAS123456", "direction": "export"}),
    ({"line": "ARKAS", "consignment": "ARKAS123456", "some": "Some"},
     "consignment", {"line": "ARKAS", "consignment": "ARKAS123456", "direction": "import"})
])
def test_body(row: str, consignment: str, answer: dict, parsed_df: ParsedDf, monkeypatch):
    monkeypatch.setattr('src.scripts.parsed.get_line_unified', mock_get_line_unified)
    monkeypatch.setattr('src.scripts.parsed.unified_list_line_name', lambda: {"ARKAS": ["ARKAS"]})
    assert parsed_df.body(row, consignment) == answer


@pytest.mark.parametrize("body,row,answer",
                         [
                             ({"line": "ARKAS", "consignment": "ARKAS123456", "direction": "import"},
                              {"consignment": "ARKAS123456"}, {"PORT"})
                         ])
@patch('requests.post')
def test_get_vuxx_response(mock_post, body, row, answer, parsed_df):
    mock_response = MagicMock()
    mock_response.json.return_value = answer
    mock_post.return_value = mock_response

    port = parsed_df.get_vuxx_response(body, row)
    assert port == answer
    mock_post.assert_called()


@pytest.mark.parametrize("row,consignment,answer", [
    ({"line": "ARKAS", "consignment": "ARKAS123456", "direction": "import"}, "consignment", {"PORT"}),
])
@patch('requests.post')
def test_get_port_with_recursion(mock_post, row: dict, consignment: str, answer, parsed_df):
    mock_response = MagicMock()
    mock_response.json.return_value = answer
    mock_post.return_value = mock_response
    with patch.object(ParsedDf, "body", return_value=row):
        port = parsed_df.get_port_with_recursion(1, row, consignment)

    assert port == answer
    mock_post.assert_called()


@pytest.mark.parametrize("row, answer", [
    ({"consignment": "ARKAS123456"}, "consignment")
])
def test_get_consignment(row, answer, parsed_df):
    assert parsed_df.get_consignment(row) == answer


@pytest.mark.parametrize("index, port,answer", [
    (0, "Port", "Port")
])
def test_write_port(index, port, answer, parsed_df):
    parsed_df.write_port(index, port)
    assert "is_auto_tracking" in parsed_df.df.iloc[index]
    assert "is_auto_tracking_ok" in parsed_df.df.iloc[index]
    if port:
        assert "tracking_seaport" in parsed_df.df.iloc[index]


@pytest.mark.parametrize("line,answer", [
    ("ARKA", True),
    ("ARKAS", False)
])
def test_check_line(line, answer, parsed_df, monkeypatch):
    monkeypatch.setattr("src.scripts.parsed.unified_list_line_name", lambda: ["ARKAS"])

    assert parsed_df.check_line(line) == answer


def test_add_new_columns(parsed_df):
    parsed_df.add_new_columns()
    assert "is_auto_tracking" in parsed_df.df


@pytest.mark.parametrize("ports", [("Port")])
def test_get_port(ports, parsed_df, monkeypatch):
    monkeypatch.setattr("src.scripts.parsed.unified_list_line_name", lambda: {"ARKAS": ["ARKAS"]})
    monkeypatch.setattr("src.scripts.parsed.get_line_tracking_empty", lambda: ["ARKAS"])
    with patch.object(ParsedDf, "get_port_with_recursion", return_value=ports):
        parsed_df.get_port()
        for index, row in parsed_df.df.iterrows():
            assert "tracking_seaport" in row
            assert "is_auto_tracking" in row
            assert "is_auto_tracking_ok" in row
            assert row['tracking_seaport'] == ports
