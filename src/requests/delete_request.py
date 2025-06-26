from .base_request import BaseRequest


class DeleteRequest(BaseRequest):
    def validate_args(self):
        items = self.args.items

        if len(items) < 1:
            raise ValueError("Invalid arguments: at least one key is required")

        self.keys = items

    def execute(self):
        for key in self.keys:
            value = self.storage.delete(key)

            if value is not None:
                if self.args.verbose:
                    print(f"'{key}' was deleted")
            else:
                print(f"'{key}' not found in storage")
