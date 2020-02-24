import pytest
from ui.app import app


@pytest.fixture()
def client():
    return app.test_client()


@pytest.mark.parametrize("route, expected_result", [("/", 500), ("home", 500)])
def test_correct_status_code(client, route, expected_result):
    assert client.get(route).status_code == expected_result


@pytest.mark.parametrize("route", ["/", "home"])
def test_correct_content_type(client, route):
    assert client.get(route).content_type == "text/html"
