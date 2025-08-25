import hashlib
import json
import os


class KVStorage:
    """Key-value storage class

    Stores key-value pairs in a directory using multiple bucket files. Each bucket is a separate JSON file. Keys are hashed to determine their bucket
    """

    def __init__(self, storage_name="kvstorage", buckets_count=16):
        """Initialize the key-value storage

        Args:
            storage_name (str): The name of the storage directory (default: "kvstorage")
            buckets_count (int): The number of buckets to use for storage (default: 16)
        """

        if not storage_name or not storage_name.strip():
            raise ValueError("Invalid storage name")

        if buckets_count <= 0:
            raise ValueError("Invalid buckets count")

        self.storage_name = storage_name
        self.buckets_count = buckets_count

        if os.path.exists(self.storage_name) and not os.path.isdir(self.storage_name):
            raise FileExistsError(f"{self.storage_name} exists and is not a directory")
        os.makedirs(self.storage_name, exist_ok=True)

        self.bucket_cache = {}

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

    def _load_bucket(self, bucket_id: int) -> dict[str, any]:
        """Load a bucket from file or cache

        Args:
            bucket_id (int): The ID of the bucket to load

        Returns:
            dict[str, any]: The contents of the bucket
        """

        if bucket_id in self.bucket_cache:
            return self.bucket_cache[bucket_id]

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

    def _save_bucket(self, bucket_id: int) -> None:
        """Save a bucket to file system

        Args:
            bucket_id (int): The ID of the bucket to save
        """

        cache = self.bucket_cache.get(bucket_id, {})
        path = self._bucket_path(bucket_id)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    def set(self, key: str, value: any) -> None:
        """Set a key-value pair in the storage

        Args:
            key (str): The key to set
            value (any): The value to set
        """

        if not key or not key.strip():
            raise ValueError("Invalid key")

        if not isinstance(key, str):
            raise ValueError("Key must be a string")

        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        bucket[key] = value
        self._save_bucket(id)

    def get(self, key: str) -> any:
        """Get a value from the storage

        Args:
            key (str): The key to get
        """

        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        return bucket.get(key)

    def delete(self, key: str) -> any:
        """Delete a key-value pair from the storage

        Args:
            key (str): The key to delete
        """

        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        value = bucket.pop(key, None)
        self._save_bucket(id)
        return value
