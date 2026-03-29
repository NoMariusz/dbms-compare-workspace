from __future__ import annotations

import os
from typing import Callable

import config
from connectors.base import BaseConnector
from connectors.couchdb import CouchConnector
from connectors.mongodb import MongoConnector
from connectors.postgres import PostgresConnector
from constants import DBMSType
from data_manager import DataManager
from runner import BenchmarkRunner
from test_cases.c1_insert_user import C1InsertUserTestCase
from test_cases.c2_insert_manufacturer_model import \
    C2InsertManufacturerModelTestCase
from test_cases.c3_insert_product_with_specification import \
    C3InsertProductWithSpecificationTestCase
from test_cases.c4_insert_product_variant import C4InsertProductVariantTestCase
from test_cases.c5_create_order_with_one_item import \
    C5CreateOrderWithOneItemTestCase
from test_cases.c6_bulk_add_order_items import C6BulkAddOrderItemsTestCase
from test_cases.r1_read_user import R1ReadUserByEmailTestCase
from test_cases.r2_list_products_by_type import R2ListProductsByTypeTestCase
from test_cases.r3_search_products_by_manufacturer import \
    R3SearchProductsByManufacturerTestCase
from test_cases.r4_product_details_card import R4ProductDetailsCardTestCase
from test_cases.r5_user_order_history import R5UserOrderHistoryTestCase
from test_cases.r6_stale_inventory_report import R6StaleInventoryReportTestCase
from test_cases.u1_update_user_contact import U1UpdateUserContactTestCase
from test_cases.u2_update_variant_price import U2UpdateVariantPriceTestCase
from test_cases.u3_reduce_variant_stock_after_sale import \
    U3ReduceVariantStockAfterSaleTestCase
from test_cases.u4_update_order_status import U4UpdateOrderStatusTestCase
from test_cases.u5_update_product_description import \
    U5UpdateProductDescriptionTestCase
from test_cases.u6_update_shipping_address import \
    U6UpdateShippingAddressTestCase


def _required_env(name: str, default: str | None = None) -> str:
	value = os.getenv(name, default)
	if value is None:
		raise ValueError(f"Missing required environment variable: {name}")
	return value


def _load_env_file(path: str) -> None:
	if not os.path.exists(path):
		return

	with open(path, mode="r", encoding="utf-8") as env_file:
		for line in env_file:
			line = line.strip()
			if not line or line.startswith("#") or "=" not in line:
				continue
			key, value = line.split("=", 1)
			os.environ.setdefault(key.strip(), value.strip())


def _build_postgres_lts() -> BaseConnector:
	return PostgresConnector(
		dbms_type=DBMSType.PostgreSQL_LTS,
		host=_required_env("POSTGRES_LTS_HOST", "localhost"),
		port=int(_required_env("POSTGRES_LTS_PORT")),
		user=_required_env("POSTGRES_LTS_USER"),
		password=_required_env("POSTGRES_LTS_PASSWORD"),
	)


def _build_postgres_11() -> BaseConnector:
	return PostgresConnector(
		dbms_type=DBMSType.PostgreSQL_11,
		host=_required_env("POSTGRES_11_HOST", "localhost"),
		port=int(_required_env("POSTGRES_11_PORT")),
		user=_required_env("POSTGRES_11_USER"),
		password=_required_env("POSTGRES_11_PASSWORD"),
	)


def _build_mongodb() -> BaseConnector:
	return MongoConnector(
		host=_required_env("MONGO_HOST", "localhost"),
		port=int(_required_env("MONGO_PORT")),
		user=_required_env("MONGO_USERNAME"),
		password=_required_env("MONGO_PASSWORD"),
	)


def _build_couchdb() -> BaseConnector:
	return CouchConnector(
		host=_required_env("COUCHDB_HOST", "localhost"),
		port=int(_required_env("COUCHDB_PORT")),
		user=_required_env("COUCHDB_USERNAME"),
		password=_required_env("COUCHDB_PASSWORD"),
	)


def build_connectors() -> list[BaseConnector]:
	connector_builders: dict[DBMSType, Callable[[], BaseConnector]] = {
		DBMSType.PostgreSQL_LTS: _build_postgres_lts,
		DBMSType.PostgreSQL_11: _build_postgres_11,
		DBMSType.MongoDB: _build_mongodb,
		DBMSType.CouchDB: _build_couchdb,
	}

	connectors: list[BaseConnector] = []
	for dbms in config.TESTED_DBMS:
		builder = connector_builders.get(dbms)
		if builder is None:
			raise ValueError(f"Unsupported DBMS in config.TESTED_DBMS: {dbms}")
		connectors.append(builder())

	return connectors


def build_test_cases() -> list:
	return [
		R1ReadUserByEmailTestCase(),
		R2ListProductsByTypeTestCase(),
		R3SearchProductsByManufacturerTestCase(),
		R4ProductDetailsCardTestCase(),
		R5UserOrderHistoryTestCase(),
		R6StaleInventoryReportTestCase(),
		C1InsertUserTestCase(),
		C2InsertManufacturerModelTestCase(),
		C3InsertProductWithSpecificationTestCase(),
		C4InsertProductVariantTestCase(),
		C5CreateOrderWithOneItemTestCase(),
		C6BulkAddOrderItemsTestCase(),
		U1UpdateUserContactTestCase(),
		U2UpdateVariantPriceTestCase(),
		U3ReduceVariantStockAfterSaleTestCase(),
		U4UpdateOrderStatusTestCase(),
		U5UpdateProductDescriptionTestCase(),
		U6UpdateShippingAddressTestCase(),
	]


def main() -> None:
	_load_env_file(path=os.path.join(os.path.dirname(__file__), ".env"))

	data_manager = DataManager()
	runner = BenchmarkRunner(
		connectors=build_connectors(),
		test_cases=build_test_cases(),
		data_manager=data_manager,
	)
	runner.run(config.TESTED_SIZES)
	data_manager.save_to_csv(config.OUTPUT_FILE_PATH)


if __name__ == "__main__":
	main()
