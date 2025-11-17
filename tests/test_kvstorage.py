import os
import tempfile
import pytest
import subprocess
from kvstorage.storage import KVStorage, KeyNotFoundError


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_set(temp_storage):
    os.system(f"python3 kvstorage/cli.py {temp_storage} set name=Egor")

    storage = KVStorage(temp_storage)
    assert storage.get("name") == "Egor"


def test_set_multiple(temp_storage):
    os.system(
        f"python3 kvstorage/cli.py {temp_storage} set name=Egor age=19 city=Yekaterinburg"
    )

    storage = KVStorage(temp_storage)
    assert storage.get("name") == "Egor"
    assert storage.get("age") == "19"
    assert storage.get("city") == "Yekaterinburg"


def test_get_verbose(temp_storage):
    os.system(f"python3 kvstorage/cli.py {temp_storage} set name=Egor")

    result = (
        os.popen(f"python3 kvstorage/cli.py -v {temp_storage} get name").read().strip()
    )
    assert result == "name = Egor"


def test_delete(temp_storage):
    os.system(f"python3 kvstorage/cli.py {temp_storage} set name=Egor")
    os.system(f"python3 kvstorage/cli.py {temp_storage} delete name")

    storage = KVStorage(temp_storage)
    with pytest.raises(KeyNotFoundError):
        storage.get("name")


def test_special_characters(temp_storage):
    subprocess.run(
        ["python3", "kvstorage/cli.py", temp_storage, "set", "key®=!@#$%^&*()"],
        check=True,
    )

    storage = KVStorage(temp_storage)
    assert storage.get("key®") == "!@#$%^&*()"

    subprocess.run(
        ["python3", "kvstorage/cli.py", temp_storage, "delete", "key®"], check=True
    )

    storage = KVStorage(temp_storage)
    with pytest.raises(KeyNotFoundError):
        storage.get("key®")
