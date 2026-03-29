from __future__ import annotations

import argparse
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


def _load_env(path: Path) -> None:
	if not path.exists():
		return

	with path.open("r", encoding="utf-8") as env_file:
		for line in env_file:
			line = line.strip()
			if not line or line.startswith("#") or "=" not in line:
				continue
			key, value = line.split("=", 1)
			os.environ.setdefault(key.strip(), value.strip())


def _env(name: str, default: str | None = None) -> str:
	value = os.getenv(name, default)
	if value is None:
		raise ValueError(f"Missing required environment variable: {name}")
	return value


def _connect(target: str, db: str) -> psycopg2.extensions.connection:
	if target == "lts":
		return psycopg2.connect(
			host=_env("POSTGRES_LTS_HOST", "localhost"),
			port=int(_env("POSTGRES_LTS_PORT", "5432")),
			user=_env("POSTGRES_LTS_USER", "admin"),
			password=_env("POSTGRES_LTS_PASSWORD", "password123"),
			dbname=_env("POSTGRES_LTS_DB", db),
		)

	if target == "11":
		return psycopg2.connect(
			host=_env("POSTGRES_11_HOST", "localhost"),
			port=int(_env("POSTGRES_11_PORT", "5433")),
			user=_env("POSTGRES_11_USER", "admin"),
			password=_env("POSTGRES_11_PASSWORD", "password123"),
			dbname=_env("POSTGRES_11_DB", db),
		)

	raise ValueError(f"Unsupported target: {target}")


def _chunks(total: int, batch_size: int):
	start = 0
	while start < total:
		end = min(start + batch_size, total)
		yield start, end
		start = end


def _ensure_reference_data(cursor) -> None:
	execute_values(
		cursor,
		"INSERT INTO user_roles (id_role, role_name) VALUES %s ON CONFLICT (id_role) DO NOTHING",
		[(1, "Admin"), (2, "User")],
	)
	execute_values(
		cursor,
		"INSERT INTO order_status (id_status, status_name) VALUES %s ON CONFLICT (id_status) DO NOTHING",
		[
			(1, "Draft"),
			(2, "Waiting for a payment"),
			(3, "Shipment in progress"),
			(4, "Completed"),
			(5, "Cancelled"),
		],
	)
	execute_values(
		cursor,
		"INSERT INTO product_types (type_name) VALUES %s ON CONFLICT DO NOTHING",
		[
			("Man",),
			("Woman",),
			("Unisex",),
			("In-line Skates",),
			("Accessories",),
			("Ice Skates",),
		],
	)


def _truncate_generated_tables(cursor) -> None:
	# reset mutable tables while keeping reference dictionaries
	cursor.execute(
		"""
		TRUNCATE TABLE
			models_to_product_types,
			order_items,
			orders,
			product,
			models,
			gear_specifications,
			manufacturers,
			users
		RESTART IDENTITY CASCADE
		"""
	)


def _fetch_ids(cursor, table: str, id_column: str) -> list[int]:
	cursor.execute(f"SELECT {id_column} FROM {table}")
	return [row[0] for row in cursor.fetchall()]


def _split_entity_sizes(size: int) -> dict[str, int]:
	if size <= 0:
		raise ValueError("size must be greater than zero")

	entities = [
		"manufacturers",
		"models",
		"mappings",
		"specs",
		"products",
		"users",
		"orders",
		"order_items",
	]

	if size < len(entities):
		# fallback for very small runs to keep FK chain simple
		return {
			"manufacturers": 0,
			"models": 0,
			"mappings": 0,
			"specs": 0,
			"products": 0,
			"users": size,
			"orders": 0,
			"order_items": 0,
		}

	weights = {
		"manufacturers": 0.05,
		"models": 0.10,
		"mappings": 0.05,
		"specs": 0.10,
		"products": 0.20,
		"users": 0.20,
		"orders": 0.15,
		"order_items": 0.15,
	}

    # populate each entity with at least 1 row to ensure FK constraints can be satisfied, then distribute the remaining size according to weights
	counts = {entity: 1 for entity in entities}
	remaining = size - len(entities)

	raw = {entity: remaining * weights[entity] for entity in entities}
	base = {entity: int(raw[entity]) for entity in entities}
	for entity in entities:
		counts[entity] += base[entity]

	left = remaining - sum(base.values())
	if left > 0:
		fractions = sorted(
			entities,
			key=lambda entity: raw[entity] - int(raw[entity]),
			reverse=True,
		)
		for index in range(left):
			counts[fractions[index % len(fractions)]] += 1

	return counts


def populate_database(size: int, target: str, batch_size: int, reset: bool, db: str) -> None:
	conn = _connect(target, db)
	conn.autocommit = False

	run_tag = datetime.now().strftime("%Y%m%d%H%M%S")
	sizes = _split_entity_sizes(size)

	manufacturers_count = sizes["manufacturers"]
	models_count = sizes["models"]
	mappings_count = sizes["mappings"]
	specs_count = sizes["specs"]
	products_count = sizes["products"]
	users_count = sizes["users"]
	orders_count = sizes["orders"]
	order_items_count = sizes["order_items"]

	try:
		with conn.cursor() as cursor:
			if reset:
				_truncate_generated_tables(cursor)

			_ensure_reference_data(cursor)

			for start, end in _chunks(manufacturers_count, batch_size):
				rows = [(f"Manufacturer {run_tag}-{i}",) for i in range(start, end)]
				execute_values(cursor, "INSERT INTO manufacturers (name) VALUES %s", rows)

			manufacturer_ids = _fetch_ids(cursor, "manufacturers", "id_manufacturer")

			for start, end in _chunks(models_count, batch_size):
				rows = []
				for i in range(start, end):
					release_date = date.today() - timedelta(days=random.randint(0, 4000))
					rows.append(
						(
							random.choice(manufacturer_ids),
							f"Model {run_tag}-{i}",
							f"Generated model description {i}",
							release_date,
						)
					)
				execute_values(
					cursor,
					"""
					INSERT INTO models (id_manufacturer, model_name, description, release_date)
					VALUES %s
					""",
					rows,
				)

			cursor.execute(
				"SELECT id_model FROM models WHERE model_name LIKE %s",
				(f"Model {run_tag}-%",),
			)
			model_ids = [row[0] for row in cursor.fetchall()]
			product_type_ids = _fetch_ids(cursor, "product_types", "id_type")

			mapping_rows: list[tuple[int, int]] = []
			if model_ids and product_type_ids and mappings_count > 0:
				max_pairs = len(model_ids) * len(product_type_ids)
				target_pairs = min(mappings_count, max_pairs)
				seen_pairs: set[tuple[int, int]] = set()
				while len(mapping_rows) < target_pairs:
					pair = (random.choice(model_ids), random.choice(product_type_ids))
					if pair in seen_pairs:
						continue
					seen_pairs.add(pair)
					mapping_rows.append(pair)
			if mapping_rows:
				execute_values(
					cursor,
					"""
					INSERT INTO models_to_product_types (id_model, id_type)
					VALUES %s
					ON CONFLICT (id_model, id_type) DO NOTHING
					""",
					mapping_rows,
					page_size=batch_size,
				)

			for start, end in _chunks(specs_count, batch_size):
				rows = []
				for _ in range(start, end):
					rows.append(
						(
							random.choice([72, 76, 80, 84, 90, 100]),
							random.choice([3, 4, 5]),
							random.choice(["steel", "aluminium", "carbon"]),
							random.choice(["plastic", "carbon", "composite"]),
							random.choice(["ABEC-5", "ABEC-7", "ILQ-9"]),
						)
					)
				execute_values(
					cursor,
					"""
					INSERT INTO gear_specifications (
						wheel_size,
						number_of_wheels,
						blade_material,
						boot_material,
						bearing_type
					) VALUES %s
					""",
					rows,
				)

			spec_ids = _fetch_ids(cursor, "gear_specifications", "id_specification")

			for start, end in _chunks(products_count, batch_size):
				rows = []
				for i in range(start, end):
					rows.append(
						(
							random.choice(model_ids),
							random.choice(spec_ids),
							random.choice(["black", "white", "red", "blue", "green"]),
							random.choice(["36", "38", "40", "42", "44", "46"]),
							random.randint(0, 250),
							round(random.uniform(149.0, 999.0), 2),
							f"Generated product {run_tag}-{i}",
						)
					)
				execute_values(
					cursor,
					"""
					INSERT INTO product (
						id_model,
						id_specification,
						color_name,
						size_value,
						stock_quantity,
						price,
						description
					) VALUES %s
					""",
					rows,
				)

			for start, end in _chunks(users_count, batch_size):
				rows = []
				for i in range(start, end):
					rows.append(
						(
							f"user_{run_tag}_{i}",
							f"{run_tag}-u-{i}@bench.local",
							"hashed_password",
							f"+48{random.randint(500000000, 999999999)}",
							random.choice([1, 2]),
						)
					)
				execute_values(
					cursor,
					"""
					INSERT INTO users (username, email, password, phone, id_role)
					VALUES %s
					ON CONFLICT (email) DO NOTHING
					""",
					rows,
				)

			cursor.execute(
				"SELECT id_user FROM users WHERE email LIKE %s",
				(f"{run_tag}-u-%@bench.local",),
			)
			inserted_user_ids = [row[0] for row in cursor.fetchall()]

			for start, end in _chunks(orders_count, batch_size):
				rows = []
				for i in range(start, end):
					rows.append(
						(
							random.choice(inserted_user_ids),
							random.randint(1, 5),
							round(random.uniform(50.0, 2500.0), 2),
							f"{run_tag}-addr-{i}",
						)
					)
				execute_values(
					cursor,
					"""
					INSERT INTO orders (id_user, id_status, total_amount, shipping_address)
					VALUES %s
					""",
					rows,
				)

			cursor.execute(
				"SELECT id_order FROM orders WHERE shipping_address LIKE %s",
				(f"{run_tag}-addr-%",),
			)
			inserted_order_ids = [row[0] for row in cursor.fetchall()]
			product_ids = _fetch_ids(cursor, "product", "id_product")

			for start, end in _chunks(order_items_count, batch_size):
				rows = []
				for _ in range(start, end):
					quantity = random.randint(1, 4)
					unit_price = round(random.uniform(40.0, 1000.0), 2)
					rows.append(
						(
							random.choice(inserted_order_ids),
							random.choice(product_ids),
							quantity,
							unit_price,
						)
					)
				execute_values(
					cursor,
					"""
					INSERT INTO order_items (id_order, id_product, quantity, unit_price)
					VALUES %s
					""",
					rows,
				)

		conn.commit()
	except Exception:
		conn.rollback()
		raise
	finally:
		conn.close()


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Generate random PostgreSQL data for benchmark tests")
	parser.add_argument("--size", type=int, required=True, help="total generated row budget split across entities")
	parser.add_argument("--target", choices=["lts", "11"], default="lts", help="postgres target instance")
	parser.add_argument("--db", choices=["main_db", "indexed_db"], default="main_db", help="postgres database used for generation")
	parser.add_argument("--batch-size", type=int, default=1000, help="batch size for insert operations")
	parser.add_argument("--seed", type=int, default=None, help="optional random seed")
	parser.add_argument("--reset", action="store_true", help="truncate generated tables before inserts")
	return parser.parse_args()

# Populate db: py util_scripts\generate_random_data.py --size 500000 --target lts --batch-size 5000 --reset --db main_db
# Rr other possibilities:
# py util_scripts\generate_random_data.py --size 500000 --target lts --batch-size 5000 --reset --db indexed_db
# py util_scripts\generate_random_data.py --size 500000 --target 11 --batch-size 5000 --reset --db main_db
# py util_scripts\generate_random_data.py --size 500000 --target 11 --batch-size 5000 --reset --db indexed_db
# py util_scripts\generate_random_data.py --size 1000000 --target lts --batch-size 5000 --reset --db main_db
# ...
"""
Create backup:

Create dump inside postgres_lts container:
docker exec -e PGPASSWORD=password123 postgres_lts pg_dump -U admin -d main_db -Fc -f /tmp/database.backup
Or other possibilities:
docker exec -e PGPASSWORD=password123 postgres_lts pg_dump -U admin -d indexed_db -Fc -f /tmp/database.backup
docker exec -e PGPASSWORD=password123 postgres_11_22 pg_dump -U admin -d main_db -Fc -f /tmp/database.backup
docker exec -e PGPASSWORD=password123 postgres_11_22 pg_dump -U admin -d indexed_db -Fc -f /tmp/database.backup

Copy file to host:
docker cp postgres_lts:/tmp/database.backup ./tmp.backup
Or other possibilities:
docker cp postgres_11_22:/tmp/database.backup ./tmp.backup
"""

def main() -> None:
	args = parse_args()
	if args.size <= 0:
		raise ValueError("--size must be greater than zero")
	if args.batch_size <= 0:
		raise ValueError("--batch-size must be greater than zero")

	if args.seed is not None:
		random.seed(args.seed)

	env_path = Path(__file__).resolve().parents[1] / ".env"
	_load_env(env_path)

	populate_database(
		size=args.size,
		target=args.target,
		batch_size=args.batch_size,
		reset=args.reset,
		db=args.db,
	)
	print(f"Data generation finished for target={args.target}, size={args.size}")


if __name__ == "__main__":
	main()
