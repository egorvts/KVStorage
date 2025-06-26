from .base_request import BaseRequest


class SetRequest(BaseRequest):
    def validate_args(self):
        items = self.args.items

        if len(items) < 1 or not all("=" in item for item in items):
            raise ValueError(
                "Invalid arguments: at least one key=value pair is required")

        self.pairs = [item.split("=", 1) for item in items]

    def execute(self):
        for key, value in self.pairs:
            self.storage.set(key, value)
            if self.args.verbose:
                print(f"Set {key} = {value}")
