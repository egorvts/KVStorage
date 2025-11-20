import tempfile
import pytest
from argparse import Namespace
from kvstorage.storage import KVStorage, KeyNotFoundError
from kvstorage.requests.set_request import SetRequest
from kvstorage.requests.get_request import GetRequest
from kvstorage.requests.delete_request import DeleteRequest

@pytest.fixture
def temp_storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield KVStorage(tmpdir)

def test_set_request(temp_storage):
    args = Namespace(
        items=['name=Egor', 'age=19'],
        verbose=False
    )
    request = SetRequest(args, temp_storage)
    request.execute()
    
    assert temp_storage.get("name") == "Egor"
    assert temp_storage.get("age") == "19"

def test_get_request(temp_storage, capsys):
    temp_storage.set("name", "Egor")
    
    args = Namespace(items=['name'], verbose=True)
    request = GetRequest(args, temp_storage)
    request.execute()
    
    captured = capsys.readouterr()
    assert "name = Egor" in captured.out

def test_delete_request(temp_storage):
    temp_storage.set("name", "Egor")
    
    args = Namespace(items=['name'], verbose=False)
    request = DeleteRequest(args, temp_storage)
    request.execute()
    
    with pytest.raises(KeyNotFoundError):
        temp_storage.get("name")