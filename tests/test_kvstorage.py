import os
import tempfile
import pytest
import subprocess
from src.storage import KVStorage


@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


def test_set(temp_storage):
    os.system(f'python3 src/cli.py {temp_storage} set name=Egor')

    store = KVStorage(temp_storage)
    assert store.get("name") == "Egor"


def test_set_multiple(temp_storage):
    os.system(
        f'python3 src/cli.py {temp_storage} set name=Egor age=18 city=Yekaterinburg')

    store = KVStorage(temp_storage)
    assert store.get("name") == "Egor"
    assert store.get("age") == "18"
    assert store.get("city") == "Yekaterinburg"


def test_get_verbose(temp_storage):
    os.system(f'python3 src/cli.py {temp_storage} set name=Egor')

    result = os.popen(
        f'python3 src/cli.py -v {temp_storage} get name').read().strip()
    assert result == "name = Egor"


def test_delete(temp_storage):
    os.system(f'python3 src/cli.py {temp_storage} set name=Egor')
    os.system(f'python3 src/cli.py {temp_storage} delete name')

    result = os.popen(
        f'python3 src/cli.py {temp_storage} get name').read().strip()
    assert result == "'name' not found in storage"


def test_special_characters(temp_storage):
    subprocess.run([
        "python3", "src/cli.py", temp_storage, "set", "key®=!@#$%^&*()"
    ], check=True)

    store = KVStorage(temp_storage)
    assert store.get("key®") == "!@#$%^&*()"

    subprocess.run([
        "python3", "src/cli.py", temp_storage, "delete", "key®"
    ], check=True)

    store = KVStorage(temp_storage)
    assert store.get("key®") is None
