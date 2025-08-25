from abc import ABC, abstractmethod


class BaseRequest(ABC):
    """Base class for all requests"""

    def __init__(self, args, storage):
        """Initialize the request

        Args:
            args (dict): Arguments for the request
            storage (KVStorage): Key-value storage instance
        """
        self.args = args
        self.storage = storage
        self.validate_args()

    @abstractmethod
    def validate_args(self):
        """Validate arguments for the request"""
        pass

    @abstractmethod
    def execute(self):
        """Execute the request"""
        pass
