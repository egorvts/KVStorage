from .base import BaseRequest


class DeleteRequest(BaseRequest):
    def validate_args(self):
        items = self.args.items

        if len(items) != 1:
            raise ValueError("Usage: python storage.py <storage> delete <key>")

        self.key = items[0]

    def execute(self):
        value = self.storage.delete(self.key)

        if value is not None:
            print(f"'{self.key}' was deleted")
        else:
            print(f"'{self.key}' not found in storage")
