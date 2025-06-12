class KVStorage:
    def __init__(self, storage_name="storage.kvs", index=None):
        self.storage_name = storage_name
        self.index = {}
        self._load()

    def _load(self):
        try:
            with open(self.storage_name, "r", encoding="utf-8") as f:
                for line in f:
                    key, value = line.strip().split("=", 1)
                    self.index[key] = value

        except FileNotFoundError:
            return

    def set(self, key, value):
        self.index[key] = value
        with open(self.storage_name, "a", encoding="utf-8") as f:
            f.write(f"{key}={value}\n")

    def get(self, key):
        return self.index.get(key, None)
    
    def delete(self, key):
        value = self.index.pop(key, None)
        if value is not None:
            with open(self.storage_name, "w", encoding="utf-8") as f:
                for k, v in self.index.items():
                    f.write(f"{k}={v}\n")
        return value
