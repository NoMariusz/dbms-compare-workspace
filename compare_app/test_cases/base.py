from __future__ import annotations

import time
from abc import ABC, abstractmethod

from connectors import (BaseConnector, CouchConnector, MongoConnector,
                        PostgresConnector)
from constants import DBMSType


class BaseTestCase(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    def run(self, connector: BaseConnector) -> float:
        start = time.perf_counter()
        if type(connector) == PostgresConnector:
            self.run_for_postgresql(connector)
        elif type(connector) == MongoConnector:
            self.run_for_mongodb(connector)
        elif type(connector) == CouchConnector:
            self.run_for_couchdb(connector)
        else:
            raise ValueError(f"Unsupported DBMS type: {connector.dbms_type}")
        return time.perf_counter() - start

    @abstractmethod
    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        pass

    @abstractmethod
    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    @abstractmethod
    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
