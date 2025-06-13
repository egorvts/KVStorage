import hashlib
import json
import os


class KVStorage:
    def __init__(self, storage_name="kvstorage", buckets_count=16):
        self.storage_name = storage_name
        self.buckets_count = buckets_count

        if os.path.exists(self.storage_name) and not os.path.isdir(self.storage_name):
            raise FileExistsError(
                f"{self.storage_name} exists and is not a directory")
        os.makedirs(self.storage_name, exist_ok=True)

        self.bucket_cache = {}

    def _bucket_path(self, bucket_id):
        return os.path.join(self.storage_name, f"bucket_{bucket_id}.kvs")

    def _get_bucket_id(self, key):
        hash = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return int(hash, 16) % self.buckets_count

    def _load_bucket(self, bucket_id):
        if bucket_id in self.bucket_cache:
            return self.bucket_cache[bucket_id]

        cache = {}
        path = self._bucket_path(bucket_id)

        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                try:
                    cache = json.load(f)
                except json.JSONDecodeError:
                    cache = {}

        self.bucket_cache[bucket_id] = cache
        return cache

    def _save_bucket(self, bucket_id):
        cache = self.bucket_cache.get(bucket_id, {})
        path = self._bucket_path(bucket_id)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)

    def set(self, key, value):
        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        bucket[key] = value
        self._save_bucket(id)

    def get(self, key):
        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        return bucket.get(key)

    def delete(self, key):
        id = self._get_bucket_id(key)
        bucket = self._load_bucket(id)
        value = bucket.pop(key, None)
        self._save_bucket(id)
        return value
