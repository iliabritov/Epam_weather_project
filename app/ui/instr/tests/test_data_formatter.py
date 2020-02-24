import csv
import pytest
from datetime import datetime, timedelta
from pathlib import Path
import ui.instr.data_formatter as dt_formatter


@pytest.mark.parametrize(
    "date, expecter_result",
    [
        ("10.10.2020", datetime(2020, 10, 10)),
        ("1.10.2010", datetime(2010, 10, 1)),
        ("15.3.2009", datetime(2009, 3, 15)),
        ("05.10.2020", datetime(2020, 10, 5)),
        ("4.04.2015", datetime(2015, 4, 4)),
    ],
)
def test_str_to_datetime(date, expecter_result):
    test_result = dt_formatter.str_to_datetime(date)
    assert test_result == expecter_result


@pytest.mark.parametrize(
    "date", ["10/10/2020", "10.23.2020", "50.10.2020", "10.00.2020", "2020.10.10"]
)
def test_str_to_datetime_unexpected_results(date):
    with pytest.raises(ValueError):
        dt_formatter.str_to_datetime(date)


@pytest.mark.parametrize(
    "datetime_obj, expecter_result",
    [
        (datetime(2020, 10, 10), "10.10.2020"),
        (datetime(2010, 10, 1), "01.10.2010"),
        (datetime(2009, 3, 15), "15.03.2009"),
        (datetime(2020, 10, 5), "05.10.2020"),
        (datetime(2015, 4, 4), "04.04.2015"),
    ],
)
def test_datetime_to_str(datetime_obj, expecter_result):
    test_result = dt_formatter.datetime_to_str(datetime_obj)
    assert test_result == expecter_result


@pytest.fixture(scope="function")
def responce_data():
    input_data = [
        {"start_date": "10.10.2000", "end_date": "15.03.2009", "city_name": "Samara"},
        {"start_date": "15.04.2010", "end_date": "24.12.2019", "city_name": "Berlin"},
        None,
    ]
    exp_result = [
        [datetime(2000, 10, 10), datetime(2009, 3, 15), "Samara"],
        [datetime(2010, 4, 15), datetime(2019, 12, 24), "Berlin"],
        [datetime.now().date() - timedelta(days=7), datetime.now().date(), "Irkutsk"],
    ]
    return zip(input_data, exp_result)


def test_form_query_data(responce_data):
    for raw_responce, expected_result in responce_data:
        result = dt_formatter.form_query_data(raw_responce)
        assert result == expected_result


@pytest.fixture(scope="function")
def csv_test_file():
    class CsvTestFile:
        def __init__(self):
            self.file_name = Path("instr/tests/test_data/data_for_reformator.csv")
            self.city_name = "Samara"
            self.raw_line = self.get_test_line(8)
            self.expected_data = (
                "rp5",
                "Samara",
                datetime(2020, 2, 20).date(),
                -1.0,
                " ",
                0,
                0,
                "Ветер, дующий с запада",
            )

        def get_test_line(self, num_line):
            with open(self.file_name, "r", encoding="utf-8") as csv_f:
                raw_lines = csv.reader(csv_f, delimiter=";")
                n = 1
                for line in raw_lines:
                    if n == num_line:
                        return line
                    n += 1

    return CsvTestFile()


@pytest.fixture(scope="function")
def csv_reformator(csv_test_file):
    return dt_formatter.CsvReformator(csv_test_file.file_name, csv_test_file.city_name)


def test_csv_reformator_correct_init_attrs(csv_reformator, csv_test_file):
    assert csv_reformator.path == str(csv_test_file.file_name)
    assert csv_reformator.city == csv_test_file.city_name


@pytest.mark.parametrize(
    "num, supposed_type",
    [
        (0, str),
        (1, str),
        (2, type(datetime(20, 1, 1).date())),
        (3, float),
        (4, str),
        (5, int),
        (6, int),
        (7, str),
    ],
)
def test_reformat_data_correct_types(csv_reformator, csv_test_file, num, supposed_type):
    result = csv_reformator.reformat_data(csv_test_file.raw_line)
    assert type(result[num]) == supposed_type


def test_reformat_data_correct_result(csv_reformator, csv_test_file):
    result = csv_reformator.reformat_data(csv_test_file.raw_line)
    assert csv_test_file.expected_data == result


@pytest.fixture(scope="function")
def clean_lines_dataset(csv_reformator):
    return [line for line in csv_reformator.clean_lines()]


def test_clean_lines_check_correct_(clean_lines_dataset):
    assert len(clean_lines_dataset) == 56


def test_clean_lines(clean_lines_dataset, csv_test_file):
    assert csv_test_file.expected_data == clean_lines_dataset[0]
