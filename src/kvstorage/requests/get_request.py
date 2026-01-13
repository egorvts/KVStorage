from .base_request import BaseRequest
from ..storage import KeyNotFoundError


class GetRequest(BaseRequest):
    def validate_args(self):
        items = self.args.items

        if len(items) != 1:
            raise ValueError("Invalid arguments: exactly one key is required")

        self.key = items[0]

    def execute(self):
        try:
            value = self.storage.get(self.key)
            if self.args.verbose:
                print(f"{self.key} = {value}")
        except KeyNotFoundError:
            print(f"'{self.key}' not found in storage")
