from __future__ import annotations

from config import NUMER_OF_TEST_RUNS
from connectors.base import BaseConnector
from constants import DBDataSize
from data_manager import DataManager
from test_cases.base import BaseTestCase

import math

def _is_timeout_exception(exc: Exception) -> bool:
    text = str(exc)
    timeout_markers = (
        '"error":"timeout"',
        "The request could not be processed in a reasonable amount of time",
        "timed out",
    )
    return any(marker in text for marker in timeout_markers)

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
                    try:
                        connector.restore_data(size_label)
                    except RuntimeError as exc:
                        if _is_timeout_exception(exc):
                            print(
                                f"WARNING: Restore timeout for {connector.name} / size={size_label}. "
                                f"Saving inf for all remaining tests and continuing."
                            )
                            for test_case in self.test_cases:
                                self.data_manager.store_result(
                                    test_case.name,
                                    connector.name,
                                    size_label,
                                    math.inf,
                                )
                            continue
                        raise

                    for test_case in self.test_cases:
                        average_duration = math.inf

                        try:
                            # warm-up run not included in final score
                            test_case.prepare(connector)
                            test_case.run(connector)

                            full_duration = 0.0
                            for _ in range(NUMER_OF_TEST_RUNS):
                                test_case.prepare(connector)
                                duration = test_case.run(connector)
                                full_duration += duration

                            average_duration = full_duration / NUMER_OF_TEST_RUNS

                        except RuntimeError as exc:
                            if _is_timeout_exception(exc):
                                print(
                                    f"WARNING: Timeout in {test_case.name} "
                                    f"for {connector.name} / size={size_label}. "
                                    f"Saving inf and continuing."
                                )
                            else:
                                raise

                        self.data_manager.store_result(
                            test_case.name,
                            connector.name,
                            size_label,
                            average_duration,
                        )
        finally:
            for connector in self.connectors:
                connector.close()
