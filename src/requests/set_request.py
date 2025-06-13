from .base_request import BaseRequest


class SetRequest(BaseRequest):
    def validate_args(self):
        items = self.args.items

        if len(items) != 1 or "=" not in items[0]:
            raise ValueError("Usage: <storage> set <key>=<value>")

        self.key, self.value = items[0].split("=", 1)

    def execute(self):
        self.storage.set(self.key, self.value)
        print(f"Set {self.key} = {self.value}")
