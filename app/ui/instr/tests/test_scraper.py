import json
import gzip
import pytest
import shutil
import requests
from pathlib import Path
from datetime import datetime
from configparser import ConfigParser
from ui.instr.scraper import LoaderRp5


class MockSession:
    def __init__(self, *args, **kwargs):
        self.text = f"href=http://address/../our_file.gz>Download/23333"


class MockGet:
    def __init__(self, *args, **kwargs):
        self.content = None


class MockGzip:
    def __init__(self, *args, **kwargs):
        pass

    def read(self, *args, **kwargs):
        return b"Some data"


def delete_folder_if_exists(path_to_folder):
    if path_to_folder.exists():
        shutil.rmtree(path_to_folder)


@pytest.fixture(scope="function")
def cfg_content():
    cfg_path = Path("instr/tests/test_data/test_download_config.ini")
    json_parts = {
        "cities": {"Samara": "28902", "Kazan": "27595"},
        "headers": {"DNT": "1", "Host": "rp5.ru"},
    }
    jsons_parts = {attr: json.dumps(json_parts[attr]) for attr in json_parts}
    str_parts = {
        "cookie_url": "https://rp5.ru/Weather_archive_in_Kurumoch_(airport)",
        "source_url": "https://rp5.ru/responses/reFileSynop.php",
        "out_folder": "./tpm",
        "data_folder": "test_data",
    }
    return cfg_path, jsons_parts, str_parts


@pytest.fixture(scope="function")
def config_file(cfg_content):
    cfg_path = cfg_content[0]
    test_dwn_cfg = ConfigParser()
    test_dwn_cfg["download"] = {**cfg_content[1], **cfg_content[2]}

    with cfg_path.open("w") as cfg:
        test_dwn_cfg.write(cfg)

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2020, 1, 3)

    yield cfg_path, start_date, end_date

    if cfg_path.exists():
        cfg_path.unlink()

    test_data_folder = Path(test_dwn_cfg["download"]["out_folder"])

    delete_folder_if_exists(test_data_folder)


@pytest.fixture(scope="function")
def cfg_content_obj_preparation(cfg_content):
    json_parts = {attr: json.loads(cfg_content[1][attr]) for attr in cfg_content[1]}
    paths = {
        attr: Path(cfg_content[2][attr]) for attr in cfg_content[2] if "folder" in attr
    }
    return {**json_parts, **cfg_content[2], **paths}


@pytest.fixture(scope="function")
def csv_loader(config_file):
    return LoaderRp5(*config_file)


def test_correct_input_arrts(config_file, csv_loader):
    assert csv_loader.config_path == config_file[0]
    assert csv_loader.start_date == config_file[1]
    assert csv_loader.end_date == config_file[2]


def test_correct_config_file(cfg_content, csv_loader):
    config = {**cfg_content[1], **cfg_content[2]}
    assert csv_loader.config == config


def test_correct_obj_attrs(cfg_content_obj_preparation, csv_loader):
    for attr in cfg_content_obj_preparation:
        assert getattr(csv_loader, attr) == cfg_content_obj_preparation[attr]


@pytest.fixture(scope="function")
def mock_requests(monkeypatch):
    def mock_get(*args, **kwargs):
        return f"Some data"

    monkeypatch.setattr(requests, "get", mock_get)

    return


@pytest.fixture(scope="function")
def session_obj(mock_requests, csv_loader):
    return csv_loader.make_session()


def test_make_session_out_session_obj(session_obj):
    assert type(session_obj) == type(requests.session())


@pytest.fixture(scope="function")
def cookies(mock_requests, session_obj):
    return dict(session_obj.cookies.iteritems())


def test_make_session_right_cookies_quantity(cookies):
    return len(cookies) == 2


@pytest.mark.parametrize("exp_name", ["PHPSESSID", "lang"])
def test_make_session_attrs_cookies(cookies, exp_name):
    assert exp_name in cookies


@pytest.fixture(scope="function")
def form_data_example(csv_loader):
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2020, 1, 3)
    return csv_loader.get_form_data("Samara", start_date, end_date)


def test_get_form_data_correct_return_type(form_data_example):
    assert type(form_data_example) == dict


@pytest.mark.parametrize(
    "form_key, exp_type, exp_value",
    [
        ("wmo_id", str, "28902"),
        ("a_date1", str, "01.01.2020"),
        ("a_date2", str, "03.01.2020"),
        ("f_ed3", int, 2),
        ("f_ed4", int, 2),
        ("f_ed5", int, 17),
        ("f_pe", int, 1),
        ("f_pe1", int, 2),
        ("lng_id", int, 1),
    ],
)
def test_get_form_data_basic_result(form_data_example, form_key, exp_type, exp_value):
    assert form_key in form_data_example
    assert type(form_data_example[form_key] == exp_type)
    assert form_data_example[form_key] == exp_value


def test_get_form_data_city_not_exists(csv_loader):
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2020, 1, 3)
    with pytest.raises(KeyError):
        csv_loader.get_form_data("NotCity", start_date, end_date)


def test__make_folder_if_not_exists(csv_loader):
    csv_loader._make_folder_if_not_exists(csv_loader.out_folder)


@pytest.fixture(scope="function")
def downloaded_csv_file(monkeypatch, form_data_example, csv_loader):
    monkeypatch.setattr(csv_loader._session, "post", MockSession)
    monkeypatch.setattr(gzip, "GzipFile", MockGzip)
    monkeypatch.setattr(requests, "get", MockGet)

    out_data_folder = csv_loader.data_folder
    delete_folder_if_exists(out_data_folder)
    out_data_folder.mkdir()

    file_path = out_data_folder / Path("new_file.csv")
    csv_loader.download_dataset(form_data_example, file_path)
    yield file_path

    delete_folder_if_exists(out_data_folder)


def test_download_dataset_file_exist(downloaded_csv_file):
    assert downloaded_csv_file.exists()


def test_download_dataset_file_content(downloaded_csv_file):
    with downloaded_csv_file.open() as csv_file:
        content = csv_file.read()
    assert content == "Some data"


@pytest.fixture(scope="function")
def loaded_dataset(monkeypatch, csv_loader):
    monkeypatch.setattr(csv_loader._session, "post", MockSession)
    monkeypatch.setattr(gzip, "GzipFile", MockGzip)
    monkeypatch.setattr(requests, "get", MockGet)

    delete_folder_if_exists(csv_loader.out_folder)

    csv_loader.load_weather_datasets()

    city_folder_names = [name for name in csv_loader.cities]
    file_names = [
        f"{year}.csv"
        for year in range(csv_loader.start_date.year, csv_loader.end_date.year + 1)
    ]
    folder_paths = {
        "out_folder": csv_loader.out_folder,
        "data_folder": csv_loader.out_folder / csv_loader.data_folder,
        "city_folders": city_folder_names,
        "file_names": file_names,
    }

    yield folder_paths

    delete_folder_if_exists(csv_loader.out_folder)


def test_load_weather_datasets_exists_out_folder(loaded_dataset):
    assert loaded_dataset["out_folder"].exists()


def test_load_weather_datasets_exists_datasets_directory(loaded_dataset):
    assert loaded_dataset["data_folder"].exists()


def test_load_weather_datasets_correct_folders_list(loaded_dataset):
    folders_list = [folder.name for folder in loaded_dataset["data_folder"].iterdir()]
    assert folders_list == loaded_dataset["city_folders"]


def test_load_weather_datasets_exists_file_right_name(loaded_dataset):
    city_name = loaded_dataset["city_folders"][0]
    data_folder = loaded_dataset["data_folder"] / city_name
    file_names = [csv_f.name for csv_f in data_folder.iterdir()]
    assert file_names == loaded_dataset["file_names"]


def test_load_weather_datasets_correct_data_in_csv(loaded_dataset):
    downloaded_csv_file = (
        loaded_dataset["data_folder"]
        / loaded_dataset["city_folders"][0]
        / loaded_dataset["file_names"][0]
    )
    with downloaded_csv_file.open() as csv_file:
        content = csv_file.read()
    assert content == "Some data"
