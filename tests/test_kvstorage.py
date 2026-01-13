import tempfile
import os
import pytest
from src.kvstorage.storage import KVStorage, KeyNotFoundError


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def storage(temp_storage):
    return KVStorage(temp_storage)


class TestKVStorageInit:
    """Test KVStorage initialization"""

    def test_init_creates_directory(self, temp_storage):
        storage_path = os.path.join(temp_storage, "new_storage")
        storage = KVStorage(storage_path)
        assert os.path.isdir(storage_path)

    def test_init_invalid_storage_name_empty(self, temp_storage):
        with pytest.raises(ValueError, match="Invalid storage name"):
            KVStorage("")

    def test_init_invalid_storage_name_whitespace(self, temp_storage):
        with pytest.raises(ValueError, match="Invalid storage name"):
            KVStorage("   ")

    def test_init_invalid_buckets_count_zero(self, temp_storage):
        with pytest.raises(ValueError, match="Invalid buckets count"):
            KVStorage(temp_storage, buckets_count=0)

    def test_init_invalid_buckets_count_negative(self, temp_storage):
        with pytest.raises(ValueError, match="Invalid buckets count"):
            KVStorage(temp_storage, buckets_count=-1)

    def test_init_invalid_max_cached_buckets(self, temp_storage):
        with pytest.raises(ValueError, match="Invalid max cached buckets count"):
            KVStorage(temp_storage, max_cached_buckets=0)

    def test_init_existing_file_not_directory(self, temp_storage):
        file_path = os.path.join(temp_storage, "not_a_directory")
        with open(file_path, "w") as f:
            f.write("test")
        with pytest.raises(FileExistsError):
            KVStorage(file_path)


class TestKVStorageBasicOperations:
    """Test basic get/set/delete operations"""

    def test_set_and_get(self, storage):
        storage.set("name", "Egor")
        assert storage.get("name") == "Egor"

    def test_set_multiple_values(self, storage):
        storage.set("key1", "value1")
        storage.set("key2", "value2")
        storage.set("key3", "value3")

        assert storage.get("key1") == "value1"
        assert storage.get("key2") == "value2"
        assert storage.get("key3") == "value3"

    def test_set_overwrite(self, storage):
        storage.set("name", "Egor")
        storage.set("name", "John")
        assert storage.get("name") == "John"

    def test_get_nonexistent_key(self, storage):
        with pytest.raises(KeyNotFoundError):
            storage.get("nonexistent")

    def test_delete_existing_key(self, storage):
        storage.set("name", "Egor")
        assert storage.get("name") == "Egor"

        value = storage.delete("name")
        assert value == "Egor"

        with pytest.raises(KeyNotFoundError):
            storage.get("name")

    def test_delete_nonexistent_key(self, storage):
        value = storage.delete("nonexistent")
        assert value is None

    def test_set_invalid_key_empty(self, storage):
        with pytest.raises(ValueError, match="Invalid key"):
            storage.set("", "value")

    def test_set_invalid_key_whitespace(self, storage):
        with pytest.raises(ValueError, match="Invalid key"):
            storage.set("   ", "value")


class TestKVStorageDataTypes:
    """Test storage with different data types"""

    def test_store_string(self, storage):
        storage.set("key", "string value")
        assert storage.get("key") == "string value"

    def test_store_integer(self, storage):
        storage.set("key", 42)
        assert storage.get("key") == 42

    def test_store_float(self, storage):
        storage.set("key", 3.14159)
        assert storage.get("key") == 3.14159

    def test_store_boolean(self, storage):
        storage.set("key_true", True)
        storage.set("key_false", False)
        assert storage.get("key_true") is True
        assert storage.get("key_false") is False

    def test_store_none(self, storage):
        storage.set("key", None)
        assert storage.get("key") is None

    def test_store_list(self, storage):
        storage.set("key", [1, 2, 3, "four"])
        assert storage.get("key") == [1, 2, 3, "four"]

    def test_store_dict(self, storage):
        data = {"nested": {"key": "value"}, "list": [1, 2, 3]}
        storage.set("key", data)
        assert storage.get("key") == data

    def test_store_unicode(self, storage):
        storage.set("emoji", "🔑📦")
        storage.set("russian", "Привет")
        assert storage.get("emoji") == "🔑📦"
        assert storage.get("russian") == "Привет"


class TestKVStorageKeysMethods:
    """Test keys(), items(), exists() methods"""

    def test_keys_empty_storage(self, storage):
        assert storage.keys() == []

    def test_keys_with_data(self, storage):
        storage.set("key1", "value1")
        storage.set("key2", "value2")
        storage.set("key3", "value3")

        keys = storage.keys()
        assert len(keys) == 3
        assert set(keys) == {"key1", "key2", "key3"}

    def test_items_empty_storage(self, storage):
        assert storage.items() == []

    def test_items_with_data(self, storage):
        storage.set("key1", "value1")
        storage.set("key2", "value2")

        items = storage.items()
        assert len(items) == 2
        assert ("key1", "value1") in items
        assert ("key2", "value2") in items

    def test_exists_true(self, storage):
        storage.set("key", "value")
        assert storage.exists("key") is True

    def test_exists_false(self, storage):
        assert storage.exists("nonexistent") is False

    def test_exists_after_delete(self, storage):
        storage.set("key", "value")
        assert storage.exists("key") is True
        storage.delete("key")
        assert storage.exists("key") is False


class TestKVStoragePersistence:
    """Test data persistence across storage instances"""

    def test_persistence_across_instances(self, temp_storage):
        storage1 = KVStorage(temp_storage)
        storage1.set("persistent_key", "persistent_value")
        storage1.flush()

        storage2 = KVStorage(temp_storage)
        assert storage2.get("persistent_key") == "persistent_value"

    def test_flush_saves_all_cached(self, temp_storage):
        storage = KVStorage(temp_storage, max_cached_buckets=1)
        storage.set("key1", "value1")
        storage.set("key2", "value2")
        storage.flush()

        storage2 = KVStorage(temp_storage)
        assert storage2.get("key1") == "value1"
        assert storage2.get("key2") == "value2"


class TestKVStorageCaching:
    """Test bucket caching behavior"""

    def test_cache_eviction(self, temp_storage):
        storage = KVStorage(temp_storage, buckets_count=16, max_cached_buckets=2)

        for i in range(20):
            storage.set(f"key_{i}", f"value_{i}")

        for i in range(20):
            assert storage.get(f"key_{i}") == f"value_{i}"


class TestKVStorageRebalance:
    """Test bucket rebalancing"""

    def test_rebalance_increase_buckets(self, temp_storage):
        storage = KVStorage(temp_storage, buckets_count=4)

        for i in range(10):
            storage.set(f"key_{i}", f"value_{i}")

        count = storage.rebalance(16)
        assert count == 10
        assert storage.buckets_count == 16

        for i in range(10):
            assert storage.get(f"key_{i}") == f"value_{i}"

    def test_rebalance_decrease_buckets(self, temp_storage):
        storage = KVStorage(temp_storage, buckets_count=16)

        for i in range(10):
            storage.set(f"key_{i}", f"value_{i}")

        count = storage.rebalance(4)
        assert count == 10
        assert storage.buckets_count == 4

        for i in range(10):
            assert storage.get(f"key_{i}") == f"value_{i}"

    def test_rebalance_empty_storage(self, temp_storage):
        storage = KVStorage(temp_storage, buckets_count=4)
        count = storage.rebalance(8)
        assert count == 0
        assert storage.buckets_count == 8

    def test_rebalance_invalid_count(self, temp_storage):
        storage = KVStorage(temp_storage)
        with pytest.raises(ValueError, match="Invalid buckets count"):
            storage.rebalance(0)
        with pytest.raises(ValueError, match="Invalid buckets count"):
            storage.rebalance(-1)
