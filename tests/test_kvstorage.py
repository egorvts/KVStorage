import os
import tempfile
import pytest
from storage import KVStorage


@pytest.fixture
def temp_storage():
    with tempfile.NamedTemporaryFile(delete=True) as tmpfile:
        yield tmpfile.name


def test_set(temp_storage):
    os.system(f'python3 cli.py {temp_storage} set name=Egor')

    store = KVStorage(temp_storage)
    assert store.get("name") == "Egor"


def test_get(temp_storage):
    os.system(f'python3 cli.py {temp_storage} set name=Egor')

    result = os.popen(f'python3 cli.py {temp_storage} get name').read().strip()
    assert result == "name = Egor"

def test_delete(temp_storage):
    os.system(f'python3 cli.py {temp_storage} set name=Egor')
    os.system(f'python3 cli.py {temp_storage} delete name')

    result = os.popen(f'python3 cli.py {temp_storage} get name').read().strip()
    assert result == "'name' not found in storage."