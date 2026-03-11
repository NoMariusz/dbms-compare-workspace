# Multi-Database Docker Environment

This project orchestrates a multi-database environment using Docker Compose. It includes NoSQL (MongoDB, CouchDB) and Relational (PostgreSQL) databases, each paired with a web-based management interface.

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed.
- [Docker Compose](https://docs.docker.com/compose/install/) installed.

### Installation

1. Clone this repository or copy the files into a project folder.
2. Ensure your directory structure looks like this:

    ```text
    .
    ├── docker-compose.yml
    ├── mongodb/Dockerfile
    ├── couchdb/Dockerfile
    ├── postgres_lts/Dockerfile
    └── postgres_11/Dockerfile
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
| **PostgreSQL 11**  | 11.22        | `5433`    | `5432`        | `admin`      | `password123` |

---

## 🛠️ Management Tools Access

### 1. MongoDB (via Mongo Express)

- **URL:** [http://localhost:8081](http://localhost:8081)
- **Login:** Use the base Mongo Express credentials (admin/pass).
- **Setup:** No configuration needed. It is pre-linked to the MongoDB container.

### 2. CouchDB (via Fauxton UI)

- **URL:** [http://localhost:5984/\_utils](http://localhost:5984/_utils)
- **Login:** Use the CouchDB credentials (admin/password123).
- **First Run:** Go to the "Setup" tab on the left sidebar and select **"Configure Single Node"** to initialize system databases.

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
