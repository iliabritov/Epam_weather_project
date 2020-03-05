import pytest
import sqlalchemy
from datetime import datetime
from configparser import ConfigParser
from pathlib import Path
from ui.instr.db_driver import DbDriver, WeatherDate


class MockDataClass:
    def __init__(self, info):
        self.info = info


class Query:
    def __init__(self):
        self.query = []

    def all(self):
        return self.query


class MockDatabaseSession:
    def __init__(self):
        self.database = Query()

    def add(self, new_obj):
        self.database.query.append(new_obj)

    def commit(self):
        pass

    def delete(self, obj):
        self.database.query.remove(obj)

    def query(self, obj_type):
        return self.database.query


@pytest.fixture(scope="function")
def weather_date_obj():
	weather_test_data = {
		"data_base": "test_weather_database",
		"city": "Test_city",
		"date_day": datetime.now().date(),
		"temp": 10,
		"prec_desc": "Here is test description",
		"prec_mm": 100,
		"wind_speed": 20,
		"wind_direc": "TestWindSpeed"
	}
	return WeatherDate(**weather_test_data)


@pytest.mark.parametrize(
	"attr, exp_value",
	[
		("__tablename__", "weather"),
		("data_base", "test_weather_database"),
		("city", "Test_city"),
		("date_day", datetime.now().date()),
		("temp", 10),
		("prec_desc", "Here is test description"),
		("prec_mm", 100),
		("wind_speed", 20),
		("wind_direc", "TestWindSpeed")
	]
)
def test_WeatherDate_obj_correct_attrs(weather_date_obj, attr, exp_value):
	assert getattr(weather_date_obj, attr) == exp_value


@pytest.fixture(scope="function")
def config_file():
	test_cfg = ConfigParser()
	cfg_name = "weather_db"
	test_cfg[cfg_name] = {
		"database": "dbweather",
		"user": "postgres",
		"password": "postgres",
		"host": "weather_db"
	}
	ini_file_path = Path("instr/tests/test_data/test_db_cfg.ini")
	with ini_file_path.open("w") as ini_file:
		test_cfg.write(ini_file)

	yield ini_file_path, cfg_name, test_cfg

	if ini_file_path.exists():
		ini_file_path.unlink()


@pytest.fixture(scope="function")
def db_driver(monkeypatch, config_file):
    def mock_engine(*args, **kwargs):
        pass

    def mock_session(*args, **kwargs):
        return MockDatabaseSession()

    monkeypatch.setattr(sqlalchemy, "create_engine", mock_engine)

    test_db_driver = DbDriver(config_file[0], config_file[1])

    monkeypatch.setattr(test_db_driver, "_session", mock_session())
    return test_db_driver


def test_get_config(config_file, db_driver):
	assert config_file[2][config_file[1]] == db_driver._config


@pytest.fixture(scope="function")
def data_for_test():
    return [MockDataClass(f"object num {i}") for i in range(5)]


@pytest.fixture(scope="function")
def db_driver_with_query(db_driver, data_for_test):
    db_driver._session.database.query.extend(data_for_test)
    db_driver.make_query(MockDataClass)
    return db_driver


def test_make_query(db_driver_with_query, data_for_test):
	assert db_driver_with_query.query == data_for_test


def test_add(db_driver_with_query, data_for_test):
    added_data = MockDataClass("add this data in test_add")
    db_driver_with_query.add(added_data)
    db_driver_with_query.make_query(MockDataClass)
    assert len(db_driver_with_query.query) == 6
    assert db_driver_with_query.query[:-1] == data_for_test
    assert db_driver_with_query.query[-1] == added_data


def test_delete(db_driver_with_query, data_for_test):
    deleted_data = db_driver_with_query.query[0]
    db_driver_with_query.delete(deleted_data)
    assert len(db_driver_with_query.query) == 4
    assert db_driver_with_query.query == data_for_test[1:]
    assert deleted_data == data_for_test[0]
