from __future__ import annotations

from connectors.base import BaseConnector
from constants import DBDataSize
from data_manager import DataManager
from test_cases.base import BaseTestCase


class BenchmarkRunner:
    def __init__(self, connectors: list[BaseConnector], test_cases: list[BaseTestCase], data_manager: DataManager) -> None:
        self.connectors = connectors
        self.test_cases = test_cases
        self.data_manager = data_manager

    def run(self, TESTED_SIZES: list[DBDataSize]) -> None:
        for connector in self.connectors:
            connector.connect()

        try:
            for size in TESTED_SIZES:
                size_label = size.label
                for connector in self.connectors:
                    connector.restore_data(size_label)
                    for test_case in self.test_cases:
                        duration = test_case.run(connector)
                        self.data_manager.store_result(test_case.name, connector.name, size_label, duration)
        finally:
            for connector in self.connectors:
                connector.close()
