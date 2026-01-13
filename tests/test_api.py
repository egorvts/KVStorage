import tempfile
import pytest
import base64
from unittest.mock import patch
from fastapi.testclient import TestClient


@pytest.fixture
def temp_storage_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def client(temp_storage_path):
    """Create a test client with a temporary storage"""
    from src.kvstorage.storage import KVStorage

    test_storage = KVStorage(temp_storage_path)

    with patch("src.server.main.storage", test_storage):
        from src.server.main import app

        with TestClient(app) as test_client:
            yield test_client, test_storage


def get_auth_header(username: str, password: str) -> dict:
    """Create HTTP Basic Auth header"""
    credentials = f"{username}:{password}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


class TestRootEndpoint:
    """Test root endpoint"""

    def test_root_returns_welcome_message(self, client):
        test_client, _ = client
        response = test_client.get("/")
        assert response.status_code == 200
        assert response.json() == {"message": "Welcome to the KVStorage API"}


class TestUserRegistration:
    """Test user registration endpoint"""

    def test_register_new_user(self, client):
        test_client, _ = client
        response = test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        assert response.status_code == 201
        data = response.json()
        assert data["username"] == "testuser"
        assert data["success"] is True
        assert "registered successfully" in data["message"]

    def test_register_duplicate_user(self, client):
        test_client, _ = client
        test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        response = test_client.post(
            "/auth/register", json={"username": "testuser", "password": "different"}
        )
        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]

    def test_register_empty_username(self, client):
        test_client, _ = client
        response = test_client.post(
            "/auth/register", json={"username": "", "password": "testpass"}
        )
        assert response.status_code == 400
        assert "empty" in response.json()["detail"]

    def test_register_short_password(self, client):
        test_client, _ = client
        response = test_client.post(
            "/auth/register", json={"username": "testuser", "password": "abc"}
        )
        assert response.status_code == 400
        assert "at least 4 characters" in response.json()["detail"]


class TestUserLogin:
    """Test user login endpoint"""

    def test_login_valid_credentials(self, client):
        test_client, _ = client
        test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        response = test_client.post(
            "/auth/login", json={"username": "testuser", "password": "testpass"}
        )
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "testuser"
        assert data["success"] is True
        assert "successful" in data["message"]

    def test_login_invalid_username(self, client):
        test_client, _ = client
        response = test_client.post(
            "/auth/login", json={"username": "nonexistent", "password": "testpass"}
        )
        assert response.status_code == 401

    def test_login_invalid_password(self, client):
        test_client, _ = client
        test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        response = test_client.post(
            "/auth/login", json={"username": "testuser", "password": "wrongpass"}
        )
        assert response.status_code == 401


class TestGetCurrentUser:
    """Test get current user endpoint"""

    def test_get_me_authenticated(self, client):
        test_client, _ = client
        test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        response = test_client.get(
            "/auth/me", headers=get_auth_header("testuser", "testpass")
        )
        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "testuser"
        assert data["success"] is True

    def test_get_me_unauthenticated(self, client):
        test_client, _ = client
        response = test_client.get("/auth/me")
        assert response.status_code == 401

    def test_get_me_invalid_credentials(self, client):
        test_client, _ = client
        response = test_client.get(
            "/auth/me", headers=get_auth_header("fake", "credentials")
        )
        assert response.status_code == 401


class TestKeyValueEndpoints:
    """Test key-value CRUD endpoints"""

    @pytest.fixture
    def auth_client(self, client):
        """Create a client with authenticated user"""
        test_client, storage = client
        test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        headers = get_auth_header("testuser", "testpass")
        return test_client, storage, headers

    def test_create_key_value(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.post(
            "/keys", json={"key": "name", "value": "Egor"}, headers=headers
        )
        assert response.status_code == 201
        data = response.json()
        assert data["key"] == "name"
        assert data["value"] == "Egor"
        assert data["success"] is True

    def test_create_key_value_unauthenticated(self, client):
        test_client, _ = client
        response = test_client.post("/keys", json={"key": "name", "value": "Egor"})
        assert response.status_code == 401

    def test_create_duplicate_key(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys", json={"key": "name", "value": "Egor"}, headers=headers
        )
        response = test_client.post(
            "/keys", json={"key": "name", "value": "John"}, headers=headers
        )
        assert response.status_code == 409
        assert "already exists" in response.json()["detail"]

    def test_get_key_value(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys", json={"key": "name", "value": "Egor"}, headers=headers
        )
        response = test_client.get("/keys/name", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "name"
        assert data["value"] == "Egor"

    def test_get_nonexistent_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.get("/keys/nonexistent", headers=headers)
        assert response.status_code == 404

    def test_get_all_keys(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys", json={"key": "key1", "value": "value1"}, headers=headers
        )
        test_client.post(
            "/keys", json={"key": "key2", "value": "value2"}, headers=headers
        )

        response = test_client.get("/keys", headers=headers)
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        keys = {item["key"] for item in data}
        assert keys == {"key1", "key2"}

    def test_get_all_keys_filters_internal(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys",
            json={"key": "public_key", "value": "public_value"},
            headers=headers,
        )

        response = test_client.get("/keys", headers=headers)
        assert response.status_code == 200
        data = response.json()

        for item in data:
            assert not item["key"].startswith("__users__:")

    def test_update_key_value(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys", json={"key": "name", "value": "Egor"}, headers=headers
        )

        response = test_client.put(
            "/keys/name", json={"key": "name", "value": "John"}, headers=headers
        )
        assert response.status_code == 200
        data = response.json()
        assert data["key"] == "name"
        assert data["value"] == "John"

    def test_update_nonexistent_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.put(
            "/keys/nonexistent",
            json={"key": "nonexistent", "value": "value"},
            headers=headers,
        )
        assert response.status_code == 404

    def test_update_key_mismatch(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys", json={"key": "name", "value": "Egor"}, headers=headers
        )

        response = test_client.put(
            "/keys/name", json={"key": "different", "value": "value"}, headers=headers
        )
        assert response.status_code == 400
        assert "must match" in response.json()["detail"]

    def test_delete_key(self, auth_client):
        test_client, _, headers = auth_client
        test_client.post(
            "/keys", json={"key": "name", "value": "Egor"}, headers=headers
        )

        response = test_client.delete("/keys/name", headers=headers)
        assert response.status_code == 204

        response = test_client.get("/keys/name", headers=headers)
        assert response.status_code == 404

    def test_delete_nonexistent_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.delete("/keys/nonexistent", headers=headers)
        assert response.status_code == 404


class TestInternalKeyProtection:
    """Test that internal keys are protected"""

    @pytest.fixture
    def auth_client(self, client):
        test_client, storage = client
        test_client.post(
            "/auth/register", json={"username": "testuser", "password": "testpass"}
        )
        headers = get_auth_header("testuser", "testpass")
        return test_client, storage, headers

    def test_cannot_create_internal_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.post(
            "/keys", json={"key": "__users__:hacker", "value": "evil"}, headers=headers
        )
        assert response.status_code == 403

    def test_cannot_get_internal_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.get("/keys/__users__:testuser", headers=headers)
        assert response.status_code == 403

    def test_cannot_update_internal_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.put(
            "/keys/__users__:testuser",
            json={"key": "__users__:testuser", "value": "hacked"},
            headers=headers,
        )
        assert response.status_code == 403

    def test_cannot_delete_internal_key(self, auth_client):
        test_client, _, headers = auth_client
        response = test_client.delete("/keys/__users__:testuser", headers=headers)
        assert response.status_code == 403
