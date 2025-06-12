import sys
import argparse
from storage import KVStorage


def main():
    parser = argparse.ArgumentParser(
        prog="KV-Storage", description="Tool to create storages of key-value pairs", epilog="© 2025 Egor Vetoshkin")

    parser.add_argument("storage", help="Name of the storage to use")
    parser.add_argument("command", choices=[
                        "get", "set", "delete"], help="Command to execute on the storage")
    parser.add_argument("pairs_list", nargs="+",
                        help="List of key-value pairs in the format <key>=<value>")

    args = parser.parse_args()

    storage = KVStorage(storage_name=args.storage)
    command = args.command
    pairs_list = args.pairs_list

    if command == "set":
        set(pairs_list, storage)
    elif command == "get":
        get(pairs_list, storage)
    elif command == "delete":
        delete(pairs_list, storage)


def set(args, storage):
    if len(args) != 1:
        print("Usage: python storage.py <storage> set <key>=<value>")
        return

    key_value = args[0].split("=", 1)

    if len(key_value) != 2:
        raise argparse.ArgumentTypeError(
            f"Invalid format, expected key=value")

    key, value = key_value
    storage.set(key, value)
    print(f"Set {key} = {value}")


def get(args, storage):
    if len(args) != 1:
        print("Usage: python storage.py <storage> get <key>")
        return

    key = args[0]
    value = storage.get(key)
    if value is not None:
        print(f"{key} = {value}")
    else:
        print(f"'{key}' not found in storage")


def delete(args, storage):
    if len(args) != 1:
        print("Usage: python storage.py <storage> delete <key>")
        return

    key = args[0]
    value = storage.delete(key)
    if value is not None:
        print(f"'{key}' was deleted")
    else:
        print(f"'{key}' not found in storage")


if __name__ == "__main__":
    main()
