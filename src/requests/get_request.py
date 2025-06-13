from .base_request import BaseRequest


class GetRequest(BaseRequest):
    def validate_args(self):
        items = self.args.items

        if len(items) != 1:
            raise ValueError("Usage: <storage> get <key>")

        self.key = items[0]

    def execute(self):
        value = self.storage.get(self.key)

        if value is not None:
            print(f"{self.key} = {value}")
        else:
            print(f"'{self.key}' not found in storage")
