from __future__ import annotations

import time
from abc import ABC, abstractmethod

from connectors import (BaseConnector, CouchConnector, MongoConnector,
                        PostgresConnector)


class BaseTestCase(ABC):
    def __init__(self, name: str) -> None:
        self.name = name

    '''Runs the test case for the given connector and returns the duration in milliseconds.
        The duration is measured just for the execution of the test case, not including any setup or teardown operations.
    '''
    def run(self, connector: BaseConnector) -> float:
        start = time.perf_counter_ns()
        if type(connector) == PostgresConnector:
            self.run_for_postgresql(connector)
        elif type(connector) == MongoConnector:
            self.run_for_mongodb(connector)
        elif type(connector) == CouchConnector:
            self.run_for_couchdb(connector)
        else:
            raise ValueError(f"Unsupported DBMS type: {connector.dbms_type}")
        # dividing by one million to return duration in milliseconds
        return (time.perf_counter_ns() - start ) / 1_000_000

    @abstractmethod
    def run_for_postgresql(self, connector: PostgresConnector) -> None:
        pass

    @abstractmethod
    def run_for_mongodb(self, connector: MongoConnector) -> None:
        pass

    @abstractmethod
    def run_for_couchdb(self, connector: CouchConnector) -> None:
        pass
