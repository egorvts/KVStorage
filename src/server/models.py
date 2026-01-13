from pydantic import BaseModel
from typing import Any, Optional
import hashlib


class KVEntry(BaseModel):
    """Model for key-value entries"""

    key: str
    value: Any

    model_config = {"extra": "forbid"}


class KVResponse(BaseModel):
    """Response model for key-value operations"""

    key: str
    value: Any
    success: bool = True

    model_config = {"extra": "forbid"}

    @classmethod
    def from_entry(cls, entry: KVEntry, is_successful: bool = True) -> "KVResponse":
        return cls(key=entry.key, value=entry.value, success=is_successful)


class UserCreate(BaseModel):
    """Model for user registration"""

    username: str
    password: str

    model_config = {"extra": "forbid"}


class UserLogin(BaseModel):
    """Model for user login"""

    username: str
    password: str

    model_config = {"extra": "forbid"}


class UserResponse(BaseModel):
    """Response model for user operations"""

    username: str
    success: bool = True
    message: Optional[str] = None

    model_config = {"extra": "forbid"}


class User:
    """User entity for storage"""

    USERS_PREFIX = "__users__:"

    def __init__(self, username: str, password_hash: str):
        self.username = username
        self.password_hash = password_hash

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash password using SHA-256"""
        return hashlib.sha256(password.encode("utf-8")).hexdigest()

    def verify_password(self, password: str) -> bool:
        """Verify password against stored hash"""
        return self.password_hash == self.hash_password(password)

    def to_dict(self) -> dict:
        """Convert user to dictionary for storage"""
        return {"username": self.username, "password_hash": self.password_hash}

    @classmethod
    def from_dict(cls, data: dict) -> "User":
        """Create user from dictionary"""
        return cls(username=data["username"], password_hash=data["password_hash"])

    @classmethod
    def storage_key(cls, username: str) -> str:
        """Get storage key for user"""
        return f"{cls.USERS_PREFIX}{username}"
