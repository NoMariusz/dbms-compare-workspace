from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from constants import DBMSType


class BaseConnector(ABC):
    def __init__(self, dbms_type: DBMSType, name: str) -> None:
        self.dbms_type = dbms_type
        self.name = name
        self.client: Any = None

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    # restoring db with given sizes, needed for testing with different data sizes
    def restore_data(self, size_label: str) -> None:
        pass
