from fastapi import FastAPI, HTTPException, status
import uvicorn
from kvstorage.storage import KVStorage, KeyNotFoundError
from server.models import KVEntry

storage = KVStorage()

app = FastAPI(
    title="KV-Storage", description="Tool to create storages of key-value pairs"
)


@app.get("/")
async def root():
    """Root endpoint that returns a welcome message

    Returns:
        dict: Welcome message
    """
    return {"message": "Welcome to the KVStorage API"}


@app.get("/keys")
async def get_all():
    """List all keys in the storage

    Returns:
        list[KVEntry]: list of all key-value pairs

    Raises:
        HTTPException: 501 Not Implemented - This endpoint is not yet implemented
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="List keys is not implemented yet",
    )


@app.get("/keys/{key}")
async def get(key: str):
    """Retrieve a value by its key

    Args:
        key: The key to retrieve

    Returns:
        KVEntry: Key-value pair entry

    Raises:
        HTTPException: 404 Not Found - Key does not exist
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    try:
        value = storage.get(key)
        return KVEntry(key, value)
    except KeyNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.post("/keys", status_code=status.HTTP_201_CREATED)
async def create(entry: KVEntry):
    """Create a new key-value pair

    Args:
        entry: KVEntry containing the key and value to create

    Returns:
        KVEntry: The created key-value pair

    Raises:
        HTTPException: 409 Conflict - Key already exists
        HTTPException: 400 Bad Request - Invalid input data
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    try:
        try:
            storage.get(entry.key)
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Key '{entry.key}' already exists",
            )
        except KeyNotFoundError:
            pass

        storage.set(entry.key, entry.value)
        return entry
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.put("/keys/{key}")
async def update(key: str, entry: KVEntry):
    """Update an existing key-value pair

    Args:
        key: The key to update (must match the key in the request body)
        entry: KVEntry containing the key and new value

    Returns:
        KVEntry: The updated key-value pair

    Raises:
        HTTPException: 400 Bad Request - Key in URL doesn't match request body or invalid data
        HTTPException: 404 Not Found - Key does not exist
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    try:
        if key != entry.key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Key in URL must match key in request body",
            )

        try:
            storage.get(key)
        except KeyNotFoundError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Key '{key}' not found"
            )

        storage.set(key, entry.value)
        return entry
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.delete("/keys/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(key: str):
    """Delete a key-value pair by key

    Args:
        key: The key to delete

    Returns:
        KVEntry: The deleted key-value pair

    Raises:
        HTTPException: 404 Not Found - Key does not exist
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    try:
        value = storage.get(key)
        storage.delete(key)
        return KVEntry(key, value)
    except KeyNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


if __name__ == "__main__":
    uvicorn.run("server.main:app", host="0.0.0.0", port=2310, reload=True)
