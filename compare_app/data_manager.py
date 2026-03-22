from __future__ import annotations

import csv
import os
from collections import defaultdict
from typing import Any


class DataManager:
    def __init__(self) -> None:
        self._matrix: dict[str, dict[str, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))

    def store_result(self, test_name: str, connector_name: str, size_label: str, duration: float) -> None:
        self._matrix[test_name][size_label][connector_name] = duration

    def save_to_csv(self, output_file_path: str) -> None:
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        tests = sorted(self._matrix.keys())
        size_order = sorted({size for size_map in self._matrix.values() for size in size_map.keys()})
        connector_order = sorted(
            {
                connector
                for size_map in self._matrix.values()
                for connector_map in size_map.values()
                for connector in connector_map.keys()
            }
        )

        columns: list[tuple[str, str]] = []
        for size in size_order:
            for connector in connector_order:
                columns.append((size, connector))

        header = ["TestCase"] + [f"{connector}_{size}" for size, connector in columns]

        with open(output_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(header)
            for test_name in tests:
                row: list[Any] = [test_name]
                for size, connector in columns:
                    row.append(self._matrix[test_name].get(size, {}).get(connector))
                writer.writerow(row)
