from __future__ import annotations

import subprocess
from pathlib import Path

DB_NAMES = ["main_db", "indexed_db", "roles_db"]
# DB_NAMES = ["roles_db"]
TARGETS = ["lts", "11"]
# TARGETS = ["11"]
TARGET_CONTAINER_NAMES = ["lts", "11_22"]
# TARGET_CONTAINER_NAMES = ["11_22"]

SIZES = [500_000, 1_000_000, 10_000_000]
# SIZES = [500_000]
SIZE_NAMES = ["500k", "1m", "10m"]
# SIZE_NAMES = ["500k"]

BATCH_SIZE = 5000
PG_PASSWORD = "password123"


def _run_command(command: list[str], step_name: str) -> None:
    print(f"{step_name}")
    print("Command:")
    print("  " + " ".join(command))
    subprocess.run(command, check=True)


def main() -> None:
    project_root = Path.cwd()
    data_root = project_root / "data/db_backups"

    print("=" * 68)
    print("Generating all PostgreSQL backups")
    print(f"Working directory: {project_root}")
    print("=" * 68)

    for target, target_container_name in zip(TARGETS, TARGET_CONTAINER_NAMES):
        docker_container = f"postgres_{target_container_name}"

        print("\n" + "-" * 68)
        print(f"Target: {target} | Docker container: {docker_container}")
        print("-" * 68)

        for db_name in DB_NAMES:
            db_dir = data_root / db_name
            db_dir.mkdir(parents=True, exist_ok=True)

            for size, size_name in zip(SIZES, SIZE_NAMES):
                print("\n" + ">" * 68)
                print(
                    f"Starting: target={target}, db={db_name}, size={size} ({size_name})"
                )

                generator_command = [
                    "py",
                    "util_scripts/generate_random_data.py",
                    "--size",
                    str(size),
                    "--target",
                    target,
                    "--batch-size",
                    str(BATCH_SIZE),
                    "--reset",
                    "--db",
                    db_name,
                ]
                _run_command(generator_command, "Step 1/3: Populate database")

                dump_path_in_container = f"/tmp/database_{db_name}.backup"
                pg_dump_command = [
                    "docker",
                    "exec",
                    "-e",
                    f"PGPASSWORD={PG_PASSWORD}",
                    docker_container,
                    "pg_dump",
                    "-U",
                    "admin",
                    "-d",
                    db_name,
                    "-Fc",
                    "-f",
                    dump_path_in_container,
                ]
                _run_command(pg_dump_command, "Step 2/3: Create dump inside Docker")

                output_file = db_dir / f"postgresql_{target}_{size_name}.backup"
                docker_cp_command = [
                    "docker",
                    "cp",
                    f"{docker_container}:{dump_path_in_container}",
                    str(output_file),
                ]
                _run_command(
                    docker_cp_command,
                    "Step 3/3: Copy backup from Docker to local data directory",
                )

                print(f"Done: {output_file}")
                print("<" * 68)

    print("\n" + "=" * 68)
    print("All backups generated successfully.")
    print("=" * 68)


if __name__ == "__main__":
    main()
