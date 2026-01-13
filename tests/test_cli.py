import tempfile
import pytest
from argparse import Namespace
from src.kvstorage.storage import KVStorage, KeyNotFoundError
from src.kvstorage.requests.set_request import SetRequest
from src.kvstorage.requests.get_request import GetRequest
from src.kvstorage.requests.delete_request import DeleteRequest


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield KVStorage(tmpdir)


class TestSetRequest:
    """Test SetRequest class"""

    def test_set_single_pair(self, temp_storage):
        args = Namespace(items=["name=Egor"], verbose=False)
        request = SetRequest(args, temp_storage)
        request.execute()

        assert temp_storage.get("name") == "Egor"

    def test_set_multiple_pairs(self, temp_storage):
        args = Namespace(items=["name=Egor", "age=19", "city=Moscow"], verbose=False)
        request = SetRequest(args, temp_storage)
        request.execute()

        assert temp_storage.get("name") == "Egor"
        assert temp_storage.get("age") == "19"
        assert temp_storage.get("city") == "Moscow"

    def test_set_verbose_output(self, temp_storage, capsys):
        args = Namespace(items=["name=Egor"], verbose=True)
        request = SetRequest(args, temp_storage)
        request.execute()

        captured = capsys.readouterr()
        assert "Set name = Egor" in captured.out

    def test_set_value_with_equals_sign(self, temp_storage):
        args = Namespace(items=["equation=a=b+c"], verbose=False)
        request = SetRequest(args, temp_storage)
        request.execute()

        assert temp_storage.get("equation") == "a=b+c"

    def test_set_empty_value(self, temp_storage):
        args = Namespace(items=["key="], verbose=False)
        request = SetRequest(args, temp_storage)
        request.execute()

        assert temp_storage.get("key") == ""

    def test_set_invalid_no_equals(self, temp_storage):
        args = Namespace(items=["invalid_item"], verbose=False)
        with pytest.raises(ValueError, match="Invalid arguments"):
            SetRequest(args, temp_storage)

    def test_set_invalid_empty_items(self, temp_storage):
        args = Namespace(items=[], verbose=False)
        with pytest.raises(ValueError, match="Invalid arguments"):
            SetRequest(args, temp_storage)

    def test_set_invalid_mixed_items(self, temp_storage):
        args = Namespace(items=["valid=item", "invalid"], verbose=False)
        with pytest.raises(ValueError, match="Invalid arguments"):
            SetRequest(args, temp_storage)


class TestGetRequest:
    """Test GetRequest class"""

    def test_get_existing_key(self, temp_storage, capsys):
        temp_storage.set("name", "Egor")

        args = Namespace(items=["name"], verbose=True)
        request = GetRequest(args, temp_storage)
        request.execute()

        captured = capsys.readouterr()
        assert "name = Egor" in captured.out

    def test_get_nonexistent_key(self, temp_storage, capsys):
        args = Namespace(items=["nonexistent"], verbose=True)
        request = GetRequest(args, temp_storage)
        request.execute()

        captured = capsys.readouterr()
        assert "'nonexistent' not found in storage" in captured.out

    def test_get_no_output_without_verbose(self, temp_storage, capsys):
        temp_storage.set("name", "Egor")

        args = Namespace(items=["name"], verbose=False)
        request = GetRequest(args, temp_storage)
        request.execute()

        captured = capsys.readouterr()
        assert captured.out == ""

    def test_get_invalid_multiple_keys(self, temp_storage):
        args = Namespace(items=["key1", "key2"], verbose=False)
        with pytest.raises(ValueError, match="exactly one key is required"):
            GetRequest(args, temp_storage)

    def test_get_invalid_no_keys(self, temp_storage):
        args = Namespace(items=[], verbose=False)
        with pytest.raises(ValueError, match="exactly one key is required"):
            GetRequest(args, temp_storage)


class TestDeleteRequest:
    """Test DeleteRequest class"""

    def test_delete_single_key(self, temp_storage):
        temp_storage.set("name", "Egor")

        args = Namespace(items=["name"], verbose=False)
        request = DeleteRequest(args, temp_storage)
        request.execute()

        with pytest.raises(KeyNotFoundError):
            temp_storage.get("name")

    def test_delete_multiple_keys(self, temp_storage):
        temp_storage.set("key1", "value1")
        temp_storage.set("key2", "value2")
        temp_storage.set("key3", "value3")

        args = Namespace(items=["key1", "key2"], verbose=False)
        request = DeleteRequest(args, temp_storage)
        request.execute()

        with pytest.raises(KeyNotFoundError):
            temp_storage.get("key1")
        with pytest.raises(KeyNotFoundError):
            temp_storage.get("key2")
        assert temp_storage.get("key3") == "value3"

    def test_delete_verbose_output(self, temp_storage, capsys):
        temp_storage.set("name", "Egor")

        args = Namespace(items=["name"], verbose=True)
        request = DeleteRequest(args, temp_storage)
        request.execute()

        captured = capsys.readouterr()
        assert "'name' was deleted" in captured.out

    def test_delete_nonexistent_key(self, temp_storage, capsys):
        args = Namespace(items=["nonexistent"], verbose=True)
        request = DeleteRequest(args, temp_storage)
        request.execute()

        captured = capsys.readouterr()
        assert "'nonexistent' not found in storage" in captured.out

    def test_delete_invalid_no_keys(self, temp_storage):
        args = Namespace(items=[], verbose=False)
        with pytest.raises(ValueError, match="at least one key is required"):
            DeleteRequest(args, temp_storage)
