import argparse
from storage import KVStorage
from requests.set_request import SetRequest
from requests.get_request import GetRequest
from requests.delete_request import DeleteRequest


def main():
    parser = argparse.ArgumentParser(
        prog="KV-Storage", description="Tool to create storages of key-value pairs", epilog="© 2025 Egor Vetoshkin")

    parser.add_argument("storage", help="Name of the storage to use")
    parser.add_argument("command", choices=[
                        "get", "set", "delete"], help="Command to execute on the storage")
    parser.add_argument("items", nargs="+",
                        help="List of keys or key=value pairs depending on command")
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Print detailed output (default: no output)"
    )

    args = parser.parse_args()

    storage = KVStorage(storage_name=args.storage)
    command = args.command

    command_map = {
        "set": SetRequest,
        "get": GetRequest,
        "delete": DeleteRequest,
    }

    request_class = command_map[command]
    request = request_class(args, storage)
    request.execute()


if __name__ == "__main__":
    main()
