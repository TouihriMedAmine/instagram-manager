import pytest
from app import app


@pytest.fixture
def client():
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


def test_index_returns_200(client):
    response = client.get("/")
    assert response.status_code == 200


def test_api_state_returns_json(client):
    response = client.get("/api/state")
    assert response.status_code == 200
    assert response.content_type == "application/json"


def test_login_requires_post(client):
    response = client.get("/api/login")
    assert response.status_code == 405
