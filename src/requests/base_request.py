from abc import ABC, abstractmethod


class BaseRequest(ABC):
    def __init__(self, args, storage):
        self.args = args
        self.storage = storage
        self.validate_args()

    @abstractmethod
    def validate_args(self):
        pass

    @abstractmethod
    def execute(self):
        pass
