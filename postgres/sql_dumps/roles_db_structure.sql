-- Create Schema
CREATE TABLE
    user_roles (
        id_role INT PRIMARY KEY,
        role_name VARCHAR(50) NOT NULL
    );

CREATE TABLE
    users (
        id_user SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        password VARCHAR(100) NOT NULL,
        phone VARCHAR(20),
        id_role INT REFERENCES user_roles (id_role)
    );

CREATE TABLE
    manufacturers (
        id_manufacturer SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );

CREATE TABLE
    product_types (
        id_type SERIAL PRIMARY KEY,
        type_name VARCHAR(50) NOT NULL
    );

CREATE TABLE
    models (
        id_model SERIAL PRIMARY KEY,
        id_manufacturer INT REFERENCES manufacturers (id_manufacturer) ON DELETE CASCADE,
        model_name VARCHAR(100) NOT NULL,
        description TEXT,
        release_date DATE
    );

CREATE TABLE
    models_to_product_types (
        id_model INT REFERENCES models (id_model) ON DELETE CASCADE,
        id_type INT REFERENCES product_types (id_type) ON DELETE CASCADE,
        PRIMARY KEY (id_model, id_type)
    );

CREATE TABLE
    gear_specifications (
        id_specification SERIAL PRIMARY KEY,
        wheel_size INT,
        number_of_wheels INT,
        blade_material VARCHAR(50),
        boot_material VARCHAR(50),
        bearing_type VARCHAR(50)
    );

CREATE TABLE
    product (
        id_product SERIAL PRIMARY KEY,
        id_model INT REFERENCES models (id_model) ON DELETE CASCADE,
        id_specification INT REFERENCES gear_specifications (id_specification) ON DELETE CASCADE,
        color_name VARCHAR(50),
        size_value VARCHAR(20),
        stock_quantity INT NOT NULL DEFAULT 0,
        price DECIMAL(10, 2) NOT NULL,
        description TEXT
    );

CREATE TABLE
    order_status (
        id_status INT PRIMARY KEY,
        status_name VARCHAR(50) NOT NULL
    );

CREATE TABLE
    orders (
        id_order SERIAL PRIMARY KEY,
        id_user INT REFERENCES users (id_user) ON DELETE RESTRICT,
        id_status INT REFERENCES order_status (id_status),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(10, 2) NOT NULL,
        shipping_address TEXT NOT NULL
    );

CREATE TABLE
    order_items (
        id_order_item SERIAL PRIMARY KEY,
        id_order INT REFERENCES orders (id_order) ON DELETE CASCADE,
        id_product INT REFERENCES product (id_product),
        quantity INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL
    );

-- Indexes tuned for benchmark test cases and common lookup patterns.
-- Keep set moderate to avoid excessive insert/update overhead.
-- Foreign-key and relationship traversal indexes
CREATE INDEX idx_users_id_role ON users (id_role);
CREATE INDEX idx_models_id_manufacturer ON models (id_manufacturer);
CREATE INDEX idx_models_to_product_types_id_type ON models_to_product_types (id_type);
CREATE INDEX idx_product_id_model ON product (id_model);
CREATE INDEX idx_product_id_specification ON product (id_specification);
CREATE INDEX idx_orders_id_user ON orders (id_user);
CREATE INDEX idx_orders_id_status ON orders (id_status);
CREATE INDEX idx_order_items_id_order ON order_items (id_order);
CREATE INDEX idx_order_items_id_product ON order_items (id_product);

-- Search/filter indexes used in read/update/delete test cases
CREATE INDEX idx_models_model_name_id_model ON models (model_name, id_model DESC);
CREATE INDEX idx_manufacturers_name ON manufacturers (name);
CREATE INDEX idx_product_types_type_name ON product_types (type_name);
CREATE INDEX idx_orders_order_date_id_order ON orders (order_date DESC, id_order DESC);
CREATE INDEX idx_product_variant_lookup ON product (id_model, color_name, size_value, id_product DESC);

-- Partial index for cleanup of stale out-of-stock variants
CREATE INDEX idx_product_zero_stock_id_product ON product (id_product)
WHERE
    stock_quantity = 0;

-- Seed Initial Data
INSERT INTO
    user_roles (id_role, role_name)
VALUES
    (1, 'Admin'),
    (2, 'User');

INSERT INTO
    order_status (id_status, status_name)
VALUES
    (1, 'Draft'),
    (2, 'Waiting for a payment'),
    (3, 'Shipment in progress'),
    (4, 'Completed'),
    (5, 'Cancelled');

INSERT INTO
    product_types (type_name)
VALUES
    ('Man'),
    ('Woman'),
    ('Unisex'),
    ('In-line Skates'),
    ('Accessories'),
    ('Ice Skates');

-- Roles and permissions
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'standard_user') THEN
        CREATE ROLE standard_user NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'moderator') THEN
        CREATE ROLE moderator NOLOGIN;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'moderator_user') THEN
        CREATE ROLE moderator_user LOGIN PASSWORD 'moderator_password123';
    END IF;
END
$$;

GRANT moderator TO moderator_user;

GRANT USAGE ON SCHEMA public TO standard_user;
GRANT USAGE ON SCHEMA public TO moderator;

-- standard_user: read all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO standard_user;

-- standard_user: write only selected tables
GRANT INSERT, UPDATE, DELETE ON TABLE users TO standard_user;
GRANT INSERT, UPDATE, DELETE ON TABLE orders TO standard_user;
GRANT INSERT, UPDATE, DELETE ON TABLE order_items TO standard_user;
GRANT INSERT, UPDATE, DELETE ON TABLE order_status TO standard_user;

-- sequence access needed for inserts
GRANT USAGE, SELECT ON SEQUENCE users_id_user_seq TO standard_user;
GRANT USAGE, SELECT ON SEQUENCE orders_id_order_seq TO standard_user;
GRANT USAGE, SELECT ON SEQUENCE order_items_id_order_item_seq TO standard_user;

-- moderator: full access to application tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO moderator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO moderator;

-- user_roles is read-only for moderator
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE user_roles FROM moderator;
GRANT SELECT ON TABLE user_roles TO moderator;

-- best-effort restriction from technical schemas
REVOKE USAGE ON SCHEMA pg_catalog FROM moderator;
REVOKE USAGE ON SCHEMA information_schema FROM moderator;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA pg_catalog FROM moderator;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA information_schema FROM moderator;

-- default privileges for future tables created by current owner in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO standard_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, UPDATE, DELETE ON TABLES TO standard_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO moderator;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO moderator;
