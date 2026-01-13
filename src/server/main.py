from fastapi import FastAPI, HTTPException, status, Depends
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import uvicorn
from kvstorage.storage import KVStorage
from kvstorage.storage import KeyNotFoundError
from server.models import KVEntry, KVResponse, UserCreate, UserLogin, UserResponse, User
from typing import List, Optional

storage = KVStorage()
security = HTTPBasic()

app = FastAPI(
    title="KV-Storage", description="Tool to create storages of key-value pairs"
)


def get_current_user(credentials: HTTPBasicCredentials = Depends(security)) -> User:
    """Validate user credentials and return user object

    Args:
        credentials: HTTP Basic auth credentials

    Returns:
        User: Authenticated user object

    Raises:
        HTTPException: 401 Unauthorized if credentials are invalid
    """
    try:
        user_data = storage.get(User.storage_key(credentials.username))
        user = User.from_dict(user_data)

        if not user.verify_password(credentials.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return user
    except Exception as e:
        if "Key not found" in str(e) or isinstance(e, KeyNotFoundError):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        raise


def optional_auth(
    credentials: Optional[HTTPBasicCredentials] = Depends(security),
) -> Optional[User]:
    """Optional authentication - returns user if valid credentials, None otherwise"""
    try:
        return get_current_user(credentials)
    except HTTPException:
        return None


@app.get("/")
async def root():
    """Root endpoint that returns a welcome message

    Returns:
        dict: Welcome message
    """
    return {"message": "Welcome to the KVStorage API"}


@app.get("/keys", response_model=List[KVResponse])
async def get_all(user: User = Depends(get_current_user)):
    """List all keys in the storage

    Returns:
        list[KVResponse]: list of all key-value pairs

    Requires:
        Authentication via HTTP Basic Auth
    """
    try:
        items = storage.items()
        filtered_items = [
            (k, v) for k, v in items if not k.startswith(User.USERS_PREFIX)
        ]
        return [KVResponse(key=k, value=v, success=True) for k, v in filtered_items]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.get("/keys/{key}", response_model=KVResponse)
async def get(key: str, user: User = Depends(get_current_user)):
    """Retrieve a value by its key

    Args:
        key: The key to retrieve

    Returns:
        KVResponse: Key-value pair entry

    Requires:
        Authentication via HTTP Basic Auth

    Raises:
        HTTPException: 404 Not Found - Key does not exist
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    # Prevent access to user keys
    if key.startswith(User.USERS_PREFIX):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access to internal keys is forbidden",
        )
    try:
        value = storage.get(key)
        entry = KVEntry(key=key, value=value)
        return KVResponse.from_entry(entry)
    except Exception as e:
        if "Key not found" in str(e) or isinstance(e, KeyNotFoundError):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Key not found"
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.post("/keys", status_code=status.HTTP_201_CREATED, response_model=KVResponse)
async def create(entry: KVEntry, user: User = Depends(get_current_user)):
    """Create a new key-value pair

    Args:
        entry: KVEntry containing the key and value to create

    Returns:
        KVResponse: The created key-value pair

    Requires:
        Authentication via HTTP Basic Auth

    Raises:
        HTTPException: 403 Forbidden - Attempt to create internal key
        HTTPException: 409 Conflict - Key already exists
        HTTPException: 400 Bad Request - Invalid input data
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    if entry.key.startswith(User.USERS_PREFIX):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Creating internal keys is forbidden",
        )
    try:
        if storage.exists(entry.key):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Key '{entry.key}' already exists",
            )

        storage.set(entry.key, entry.value)
        return KVResponse.from_entry(entry)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.put("/keys/{key}", response_model=KVResponse)
async def update(key: str, entry: KVEntry, user: User = Depends(get_current_user)):
    """Update an existing key-value pair

    Args:
        key: The key to update (must match the key in the request body)
        entry: KVEntry containing the key and new value

    Returns:
        KVResponse: The updated key-value pair

    Requires:
        Authentication via HTTP Basic Auth

    Raises:
        HTTPException: 403 Forbidden - Attempt to update internal key
        HTTPException: 400 Bad Request - Key in URL doesn't match request body or invalid data
        HTTPException: 404 Not Found - Key does not exist
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    # Prevent updating internal user keys
    if key.startswith(User.USERS_PREFIX):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Updating internal keys is forbidden",
        )
    try:
        if key != entry.key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Key in URL must match key in request body",
            )

        if not storage.exists(key):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Key '{key}' not found"
            )

        storage.set(key, entry.value)
        return KVResponse.from_entry(entry)
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.delete("/keys/{key}", status_code=status.HTTP_204_NO_CONTENT)
async def delete(key: str, user: User = Depends(get_current_user)):
    """Delete a key-value pair by key

    Args:
        key: The key to delete

    Returns:
        None: Returns 204 No Content on success

    Requires:
        Authentication via HTTP Basic Auth

    Raises:
        HTTPException: 403 Forbidden - Attempt to delete internal key
        HTTPException: 404 Not Found - Key does not exist
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    if key.startswith(User.USERS_PREFIX):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Deleting internal keys is forbidden",
        )
    try:
        if not storage.exists(key):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Key '{key}' not found"
            )
        storage.delete(key)
        return None
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.post(
    "/auth/register", status_code=status.HTTP_201_CREATED, response_model=UserResponse
)
async def register(user_data: UserCreate):
    """Register a new user

    Args:
        user_data: UserCreate containing username and password

    Returns:
        UserResponse: Registration success response

    Raises:
        HTTPException: 409 Conflict - Username already exists
        HTTPException: 400 Bad Request - Invalid input data
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    try:
        if not user_data.username or not user_data.username.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username cannot be empty",
            )

        if len(user_data.password) < 4:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must be at least 4 characters long",
            )

        user_key = User.storage_key(user_data.username)

        if storage.exists(user_key):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"User '{user_data.username}' already exists",
            )

        user = User(
            username=user_data.username,
            password_hash=User.hash_password(user_data.password),
        )

        storage.set(user_key, user.to_dict())

        return UserResponse(
            username=user_data.username,
            success=True,
            message="User registered successfully",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.post("/auth/login", response_model=UserResponse)
async def login(user_data: UserLogin):
    """Login with username and password

    Args:
        user_data: UserLogin containing username and password

    Returns:
        UserResponse: Login success response

    Raises:
        HTTPException: 401 Unauthorized - Invalid credentials
        HTTPException: 500 Internal Server Error - Server error occurred
    """
    try:
        user_key = User.storage_key(user_data.username)

        try:
            stored_user_data = storage.get(user_key)
            user = User.from_dict(stored_user_data)
        except Exception as e:
            if "Key not found" in str(e) or isinstance(e, KeyNotFoundError):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid username or password",
                )
            raise

        if not user.verify_password(user_data.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password",
            )

        return UserResponse(
            username=user_data.username, success=True, message="Login successful"
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.get("/auth/me", response_model=UserResponse)
async def get_current_user_info(user: User = Depends(get_current_user)):
    """Get current authenticated user information

    Returns:
        UserResponse: Current user information

    Requires:
        Authentication via HTTP Basic Auth
    """
    return UserResponse(username=user.username, success=True, message="Authenticated")


if __name__ == "__main__":
    uvicorn.run("server.main:app", host="localhost", port=2310, reload=True)
