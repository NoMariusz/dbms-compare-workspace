from __future__ import annotations

import subprocess
from pathlib import Path

# DB_NAMES = ["skates_shop", "skates_shop_without_indexes", "skates_shop_roles", "skates_shop_encrypted"]
DB_NAMES = ["skates_shop_roles"]
# SIZES = [500_000, 1_000_000, 10_000_000]
SIZES = [1_000_000]
# SIZE_NAMES = ["500k", "1m", "10m"]
SIZE_NAMES = [ "1m"]

BATCH_SIZE = 5000
MONGO_CONTAINER = "mongodb_lts"
MONGO_URI_TEMPLATE = "mongodb://admin:password123@localhost:27017/{db_name}?authSource=admin"


def _run_command(command: list[str], step_name: str) -> None:
    print(step_name)
    print("Command:")
    print("  " + " ".join(command))
    subprocess.run(command, check=True)


def _generate_mongodb_backup(db_name: str, size: int, size_name: str, db_dir: Path) -> None:
    generator_command = [
        "py",
        "util_scripts/generate_random_data_mongodb.py",
        "--db",
        db_name,
        "--size",
        str(size),
        "--batch-size",
        str(BATCH_SIZE),
        "--reset",
    ]
    _run_command(generator_command, "Mongo Step 1/3: Populate database")

    dump_path_in_container = f"/tmp/mongodb_{db_name}_{size_name}.archive"
    mongo_uri = MONGO_URI_TEMPLATE.format(db_name=db_name)
    mongodump_command = [
        "docker",
        "exec",
        MONGO_CONTAINER,
        "sh",
        "-lc",
        f"mongodump --uri \"{mongo_uri}\" --archive={dump_path_in_container}",
    ]
    _run_command(mongodump_command, "Mongo Step 2/3: Create dump inside Docker")

    output_file = db_dir / f"mongodb_{size_name}.archive"
    docker_cp_command = [
        "docker",
        "cp",
        f"{MONGO_CONTAINER}:{dump_path_in_container}",
        str(output_file),
    ]
    _run_command(docker_cp_command, "Mongo Step 3/3: Copy backup to local data directory")
    print(f"Mongo backup ready: {output_file}")


def _generate_couchdb_backup(db_name: str, size: int, size_name: str, db_dir: Path) -> None:
    generator_command = [
        "py",
        "util_scripts/generate_random_data_couchdb.py",
        "--db",
        db_name,
        "--size",
        str(size),
        "--size-label",
        size_name,
        "--batch-size",
        str(BATCH_SIZE),
        "--reset",
        "--output-dir",
        str(db_dir),
    ]
    _run_command(generator_command, "CouchDB Step 1/1: Populate database and export backup")

    output_file = db_dir / f"couchdb_{size_name}.json"
    print(f"CouchDB backup ready: {output_file}")


def main() -> None:
    project_root = Path.cwd()
    data_root = project_root / "data/db_backups"

    print("=" * 68)
    print("Generating all NoSQL backups")
    print(f"Working directory: {project_root}")
    print("=" * 68)

    for db_name in DB_NAMES:
        db_dir = data_root / db_name
        db_dir.mkdir(parents=True, exist_ok=True)

        print("\n" + "-" * 68)
        print(f"Database: {db_name}")
        print("-" * 68)

        for size, size_name in zip(SIZES, SIZE_NAMES):
            print("\n" + ">" * 68)
            print(f"Starting: db={db_name}, size={size} ({size_name})")

            _generate_mongodb_backup(db_name=db_name, size=size, size_name=size_name, db_dir=db_dir)
            _generate_couchdb_backup(db_name=db_name, size=size, size_name=size_name, db_dir=db_dir)

            print("<" * 68)

    print("\n" + "=" * 68)
    print("All NoSQL backups generated successfully.")
    print("=" * 68)


if __name__ == "__main__":
    main()
