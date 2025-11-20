import tempfile
import pytest
from kvstorage.storage import KVStorage, KeyNotFoundError


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_set(temp_storage):
    storage = KVStorage(temp_storage)
    storage.set("name", "Egor")

    assert storage.get("name") == "Egor"


def test_delete(temp_storage):
    storage = KVStorage(temp_storage)
    storage.set("name", "Egor")
    assert storage.get("name") == "Egor"

    storage.delete("name")

    with pytest.raises(KeyNotFoundError):
        storage.get("key®")
