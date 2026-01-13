import hashlib
import json
import os

from collections import OrderedDict
from typing import Any


class KeyNotFoundError(Exception):
    """Raised when a key is not found in storage"""

    pass


class KVStorage:
    """Key-value storage class

    Stores key-value pairs in a directory using multiple bucket files. Each bucket is a separate JSON file. Keys are hashed to determine their bucket
    """

    def __init__(
        self, storage_name="kvstorage", buckets_count=16, max_cached_buckets=2
    ):
        """Initialize the key-value storage

        Args:
            storage_name (str): The name of the storage directory (default: "kvstorage")
            buckets_count (int): The number of buckets to use for storage (default: 16)
            max_cached_buckets (int): The maximum number of cached buckets (default: 2)

        Raises:
            ValueError: If storage_name is empty/whitespace, buckets_count <= 0, or max_cached_buckets <= 0
            FileExistsError: If storage_name exists and is not a directory
        """

        if not storage_name or not storage_name.strip():
            raise ValueError("Invalid storage name")

        if buckets_count <= 0:
            raise ValueError("Invalid buckets count")

        if max_cached_buckets <= 0:
            raise ValueError("Invalid max cached buckets count")

        self.storage_name = storage_name
        self.buckets_count = buckets_count
        self.max_cached_buckets = max_cached_buckets

        if os.path.exists(self.storage_name) and not os.path.isdir(self.storage_name):
            raise FileExistsError(f"{self.storage_name} exists and is not a directory")
        os.makedirs(self.storage_name, exist_ok=True)

        self.bucket_cache = OrderedDict()

    def _bucket_path(self, bucket_id: int) -> str:
        """Get the file path for a specific bucket

        Args:
            bucket_id (int): The ID of the bucket

        Returns:
            str: The file path for the bucket
        """

        return os.path.join(self.storage_name, f"bucket_{bucket_id}.kvs")

    def _get_bucket_id(self, key: str) -> int:
        """Get the bucket ID for a specific key

        Args:
            key (str): The key to get the bucket ID for

        Returns:
            int: The ID of the bucket
        """

        hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return int(hash, 16) % self.buckets_count

    def _load_bucket(self, bucket_id: int) -> dict[str, Any]:
        """Load a bucket from file or cache

        Args:
            bucket_id (int): The ID of the bucket to load

        Returns:
            dict[str, any]: The contents of the bucket

        Note:
            If the bucket file has invalid JSON, returns an empty dictionary
        """

        if bucket_id in self.bucket_cache:
            # Mark as recently used
            self.bucket_cache.move_to_end(bucket_id)

            return self.bucket_cache[bucket_id]

        if len(self.bucket_cache) >= self.max_cached_buckets:
            self._save_bucket(*self.bucket_cache.popitem(last=False))

        cache = {}
        path = self._bucket_path(bucket_id)

        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    cache = json.load(f)
                except json.JSONDecodeError as e:
                    print(f"Error loading bucket {bucket_id}: {e}")
                    cache = {}

        self.bucket_cache[bucket_id] = cache
        return cache

    def _save_bucket(
        self, bucket_id: int, bucket_content: dict[str, Any] = None
    ) -> None:
        """Save a bucket to file system

        Args:
            bucket_id (int): The ID of the bucket to save
            bucket_content (dict[str, any], optional): The contents of the bucket to save (if None, the cached content will be used, defaults to None)
        """

        cache = bucket_content or self.bucket_cache.get(bucket_id, {})
        path = self._bucket_path(bucket_id)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    def set(self, key: str, value: Any) -> None:
        """Set a key-value pair in the storage

        Args:
            key (str): The key to set
            value (any): The value to set

        Raises:
            ValueError: If the key is empty, whitespace, or not a string
        """

        if not key or not key.strip():
            raise ValueError("Invalid key")

        if not isinstance(key, str):
            raise ValueError("Key must be a string")

        bucket_id = self._get_bucket_id(key)
        bucket = self._load_bucket(bucket_id)
        bucket[key] = value
        self._save_bucket(bucket_id)

    def get(self, key: str) -> Any:
        """Get a value from the storage

        Args:
            key (str): The key to get

        Returns:
            any: Value associated with the key (None if not found)

        Note:
            No key validation
        """

        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)

        if key not in bucket:
            raise KeyNotFoundError("Key not found")
        return bucket[key]

    def delete(self, key: str) -> Any:
        """Delete a key-value pair from the storage

        Args:
            key (str): The key to delete

        Returns:
            any: Value associated with the key (None if not found)
        """

        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        value = bucket.pop(key, None)
        self._save_bucket(id)
        return value

    def keys(self) -> list[str]:
        """Get all keys in the storage

        Returns:
            list[str]: List of all keys in the storage
        """
        all_keys = []
        for bucket_id in range(self.buckets_count):
            bucket = self._load_bucket(bucket_id)
            all_keys.extend(bucket.keys())
        return all_keys

    def items(self) -> list[tuple[str, Any]]:
        """Get all key-value pairs in the storage

        Returns:
            list[tuple[str, Any]]: List of all key-value pairs
        """
        all_items = []
        for bucket_id in range(self.buckets_count):
            bucket = self._load_bucket(bucket_id)
            all_items.extend(bucket.items())
        return all_items

    def exists(self, key: str) -> bool:
        """Check if a key exists in the storage

        Args:
            key (str): The key to check

        Returns:
            bool: True if key exists, False otherwise
        """
        bucket_id = self._get_bucket_id(key)
        bucket = self._load_bucket(bucket_id)
        return key in bucket

    def flush(self) -> None:
        """Flush all cached buckets to disk"""
        for bucket_id, bucket_content in list(self.bucket_cache.items()):
            self._save_bucket(bucket_id, bucket_content)
        self.bucket_cache.clear()

    def rebalance(self, new_buckets_count: int) -> int:
        """Rebalance storage with a new number of buckets

        Args:
            new_buckets_count (int): The new number of buckets

        Returns:
            int: Number of items rebalanced

        Raises:
            ValueError: If new_buckets_count <= 0
        """
        if new_buckets_count <= 0:
            raise ValueError("Invalid buckets count")

        all_items = self.items()
        self.flush()

        for bucket_id in range(self.buckets_count):
            path = self._bucket_path(bucket_id)
            if os.path.exists(path):
                os.remove(path)

        self.buckets_count = new_buckets_count

        for key, value in all_items:
            self.set(key, value)

        self.flush()
        return len(all_items)
