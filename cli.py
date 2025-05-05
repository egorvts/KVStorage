import sys
from storage import KVStorage


def main():
    args = sys.argv[1:]
    if len(args) < 1:
        print("Usage: python storage.py <storage> <command> [<options>]")
        return

    command = args[1]
    storage = KVStorage(args[0])

    if command == "set":
        set(args, storage)
    elif command == "get":
        get(args, storage)
    else:
        print("Invalid command. Available commands: get, set")
        return


def set(args, storage):
    if len(args) != 3:
        print("Usage: python storage.py <storage> set <key>=<value>")
        return

    key_value = args[2].split("=", 1)

    if len(key_value) != 2:
        print("Invalid key-value pair. Use format <key>=<value>")
        return

    key, value = key_value
    storage.set(key, value)
    print(f"Set {key} = {value}")


def get(args, storage):
    if len(args) != 3:
        print("Usage: python storage.py <storage> get <key>")
        return

    key = args[2]
    value = storage.get(key)
    if value is not None:
        print(f"{key} = {value}")
    else:
        print(f"'{key}' not found in storage.")


if __name__ == "__main__":
    main()
