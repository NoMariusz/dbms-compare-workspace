# Multi-Database Docker Environment

This project orchestrates a multi-database environment using Docker Compose. It includes NoSQL (MongoDB, CouchDB) and Relational (PostgreSQL) databases, each paired with a web-based management interface.

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed.
- [Docker Compose](https://docs.docker.com/compose/install/) installed.

### Installation

1. Clone this repository or copy the files into a project folder.
   1.5. !!! download or create own db_backups files
2. Ensure your directory structure looks like this:

    ```text
    .
    ├── docker-compose.yml
    │
    ├── mongodb/
    │   ├── Dockerfile
    │   └── init/
    │       ├── create-collections.js
    │       └── indexes.js
    │
    ├── couchdb/
    │   ├── Dockerfile
    │   ├── local.d/
    │   │   └── single-node.ini
    │   └── init/
    │       └── init-couchdb.sh
    │
    ├── postgres/
    │   ├── postgres_lts/
    │   │   └── Dockerfile
    │   ├── postgres_11/
    │   │   └── Dockerfile
    │   ├── init-multiple-dbs.sh
    │   └── sql_dumps/
    │
    └── pgadmin/
        ├── servers.json
        └── pgpass
    ```

3. Run the following command to build and start the containers:
    ```bash
    docker-compose up -d
    ```

---

## 📊 Database Connectivity Matrix

| Database           | Version     | Host Port | Internal Port | Default User | Password      |
| ------------------ | ----------- | --------- | ------------- | ------------ | ------------- |
| **MongoDB**        | 8.2 (LTS)   | `27017`   | `27017`       | `admin`      | `password123` |
| **CouchDB**        | 3.5.1 (LTS) | `5984`    | `5984`        | `admin`      | `password123` |
| **PostgreSQL LTS** | 18.3 (LTS)  | `5432`    | `5432`        | `admin`      | `password123` |
| **PostgreSQL 11**  | 11.22       | `5433`    | `5432`        | `admin`      | `password123` |

---

## 🛠️ Management Tools Access

### 1. MongoDB (via Mongo Express)

- **URL:** [http://localhost:8081](http://localhost:8081)
- **Login:** Use the base Mongo Express credentials (admin/pass).
- **Setup:** No configuration needed. It is pre-linked to the MongoDB container.

### 2. CouchDB (via Fauxton UI)

- **URL:** [http://localhost:5984/\_utils](http://localhost:5984/_utils)
- **Login:** Use the CouchDB credentials (admin/password123).

### 3. PostgreSQL (via pgAdmin 4)

- **URL:** [http://localhost:5050](http://localhost:5050)
- **Connect to servers:** Use the PostgreSQL passwords (password123).

| Setting            | For Postgres LTS | For Postgres 11.22 |
| ------------------ | ---------------- | ------------------ |
| **Name**           | Postgres LTS     | Postgres 11.22     |
| **Host**           | `postgres_lts`   | `postgres_11`      |
| **Port**           | `5432`           | `5432`             |
| **Maintenance DB** | `main_db`        | `main_db`          |
| **Username**       | `admin`          | `admin`            |
| **Password**       | `password123`    | `password123`      |

> **Note:** Because pgAdmin runs inside the Docker network, it connects using the **Service Name** and the internal port **5432**, regardless of the host port mapping.

## ⚙️ Automatic Database Initialization

During the first startup, each database container initializes its schema automatically:

### MongoDB

Initialization scripts in `mongodb/init/` create collections and indexes inside the `skates_shop` database.

### CouchDB

The `couchdb-init` helper container:

- waits until CouchDB becomes available
- creates the `skates_shop` database
- creates Mango indexes required for queries.

### PostgreSQL

Initialization scripts in `postgres/` load the relational schema and initial dataset into the `main_db` database.

---

## 📈 Compare App (Benchmark Module)

The `compare_app/` folder contains a Python benchmark runner that executes selected test cases against selected DBMS engines and exports timing results to CSV.

### Module overview

```text
compare_app/
├── main.py                  # entry point and connector/test registration
├── config.py                # tested sizes, tested dbms, output path, db names
├── constants.py             # enums and size labels
├── runner.py                # BenchmarkRunner loop (size -> connector -> test)
├── data_manager.py          # in-memory matrix + CSV export
├── connectors/
│   ├── base.py              # BaseConnector lifecycle interface
│   ├── postgres.py          # PostgreSQL implementation (raw SQL CRUD methods)
│   ├── mongodb.py           # Mongo lifecycle + mocked restore
│   └── couchdb.py           # Couch lifecycle + mocked restore
└── test_cases/
    ├── base.py              # BaseTestCase dispatch + timing
    ├── c1_insert_user.py    # sample create test
    └── r1_read_user.py      # sample read test
```

### Main elements and flow

1. `main.py` loads `compare_app/.env` and constructs connectors from `config.TESTED_DBMS`.
2. `BenchmarkRunner` connects once, then runs nested loops:
   size -> connector -> test case.
3. Each test case is timed with `time.perf_counter()`.
4. Results are stored by `DataManager` and exported to `config.OUTPUT_FILE_PATH`.

### Current status

- PostgreSQL test branches are implemented and use simple raw SQL methods from `PostgresConnector`.
- MongoDB and CouchDB branches in sample test cases currently raise `NotImplementedError` intentionally.
- `restore_data(size_label)` is mocked in each connector and currently prints debug output.

### How to run compare_app

1. Start containers:
    ```bash
    docker-compose up -d
    ```
2. Install Python dependencies:
    ```bash
    pip install -r compare_app/requirements.txt
    ```
3. Run the benchmark app:
    ```bash
    cd compare_app
    python main.py
    ```
4. Check CSV output at the path from `config.OUTPUT_FILE_PATH` (default `./data/base_results.csv`).

### Configuration points

- `compare_app/config.py`
    - `TESTED_DBMS`: chooses which connectors are instantiated
    - `TESTED_SIZES`: chooses benchmark size levels
    - `OUTPUT_FILE_PATH`: output CSV location
- `compare_app/.env`
    - DB connection credentials and ports
    - optional host overrides (defaults to `localhost` in `main.py`)

### How to add a new test case

1. Create a new file in `compare_app/test_cases/` (for example `u1_update_user.py`).
2. Inherit from `BaseTestCase` and set a unique `name`.
3. Implement:
    - `run_for_postgresql`
    - `run_for_mongodb`
    - `run_for_couchdb`
4. If a branch is not ready yet, raise `NotImplementedError` explicitly.
5. Register the class in `build_test_cases()` in `compare_app/main.py`.

### How to extend connector capabilities

- PostgreSQL:
    - Add methods in `compare_app/connectors/postgres.py` that accept SQL query + params.
    - Keep query semantics in test cases, connector methods focused on execution.
- MongoDB / CouchDB:
    - Add DB-specific helper methods directly in each connector when needed by tests.
    - Then replace `NotImplementedError` branches in test cases with concrete calls.

### How to scale toward more scenarios

- Keep each scenario in a separate file under `test_cases/`.
- Reuse seeded deterministic test data (emails/ids) to make runs repeatable.
- When real data presets are ready, replace mocked `restore_data` with actual reset/import scripts per connector.
- For fair comparison, keep each test case semantically equivalent across DBMS branches.

---

## 🧹 Maintenance Commands

**Stop the environment:**

```bash
docker-compose stop

```

**Remove containers and networks (keeps data):**

```bash
docker-compose down

```

**Remove everything including volumes (deletes data):**

```bash
docker-compose down -v

```
