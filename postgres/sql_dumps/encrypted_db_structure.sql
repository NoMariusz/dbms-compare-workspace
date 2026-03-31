-- Create pgcrypto extension for encryption
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================================
-- ACTUAL TABLE DEFINITIONS (with encrypted columns as BYTEA)
-- ============================================================================

CREATE TABLE
    user_roles (
        id_role INT PRIMARY KEY,
        role_name VARCHAR(50) NOT NULL
    );

CREATE TABLE
    users (
        id_user SERIAL PRIMARY KEY,
        username VARCHAR(100) NOT NULL,
        email BYTEA NOT NULL, -- Encrypted column
        password BYTEA NOT NULL, -- Encrypted column
        phone BYTEA, -- Encrypted column
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
        shipping_address BYTEA NOT NULL -- Encrypted column
    );

CREATE TABLE
    order_items (
        id_order_item SERIAL PRIMARY KEY,
        id_order INT REFERENCES orders (id_order) ON DELETE CASCADE,
        id_product INT REFERENCES product (id_product),
        quantity INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL
    );

-- ============================================================================
-- INDEXES (same 15 indexes as indexed_db_structure.sql)
-- ============================================================================

CREATE INDEX idx_users_id_role ON users (id_role);
CREATE INDEX idx_models_id_manufacturer ON models (id_manufacturer);
CREATE INDEX idx_models_to_product_types_id_type ON models_to_product_types (id_type);
CREATE INDEX idx_product_id_model ON product (id_model);
CREATE INDEX idx_product_id_specification ON product (id_specification);
CREATE INDEX idx_orders_id_user ON orders (id_user);
CREATE INDEX idx_orders_id_status ON orders (id_status);
CREATE INDEX idx_order_items_id_order ON order_items (id_order);
CREATE INDEX idx_order_items_id_product ON order_items (id_product);
CREATE INDEX idx_models_model_name_id_model ON models (model_name, id_model DESC);
CREATE INDEX idx_manufacturers_name ON manufacturers (name);
CREATE INDEX idx_product_types_type_name ON product_types (type_name);
CREATE INDEX idx_orders_order_date_id_order ON orders (order_date DESC, id_order DESC);
CREATE INDEX idx_product_variant_lookup ON product (id_model, color_name, size_value, id_product DESC);
CREATE INDEX idx_product_zero_stock_id_product ON product (id_product)
WHERE
    stock_quantity = 0;

-- ============================================================================
-- ENCRYPTION/DECRYPTION HELPER FUNCTIONS
-- ============================================================================

-- Symmetric encryption key (hardcoded for benchmarking)
CREATE OR REPLACE FUNCTION get_encryption_key() RETURNS TEXT AS $$
    SELECT 'benchmark-secret-key'::TEXT;
$$ LANGUAGE SQL IMMUTABLE;

-- Encrypt plaintext to bytea
CREATE OR REPLACE FUNCTION encrypt_field(plaintext TEXT) RETURNS BYTEA AS $$
    SELECT pgp_sym_encrypt(COALESCE(plaintext, ''), get_encryption_key());
$$ LANGUAGE SQL;

-- Decrypt bytea to plaintext
CREATE OR REPLACE FUNCTION decrypt_field(ciphertext BYTEA) RETURNS TEXT AS $$
    SELECT pgp_sym_decrypt(COALESCE(ciphertext, ''::BYTEA), get_encryption_key());
$$ LANGUAGE SQL;

-- ============================================================================
-- DECRYPTION VIEWS (transparent interface for application)
-- ============================================================================

-- Users view with decrypted columns
CREATE VIEW users_decrypted AS
    SELECT
        id_user,
        username,
        decrypt_field(email) AS email,
        decrypt_field(password) AS password,
        decrypt_field(phone) AS phone,
        id_role
    FROM users;

-- Orders view with decrypted columns
CREATE VIEW orders_decrypted AS
    SELECT
        id_order,
        id_user,
        id_status,
        order_date,
        total_amount,
        decrypt_field(shipping_address) AS shipping_address
    FROM orders;

-- ============================================================================
-- INSTEAD-OF TRIGGERS FOR VIEWS (allow INSERT/UPDATE/DELETE through views)
-- ============================================================================

-- INSTEAD OF INSERT on users_decrypted
CREATE OR REPLACE FUNCTION trigger_users_decrypted_insert() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO users (username, email, password, phone, id_role)
    VALUES (
        NEW.username,
        encrypt_field(NEW.email),
        encrypt_field(NEW.password),
        CASE WHEN NEW.phone IS NOT NULL THEN encrypt_field(NEW.phone) ELSE NULL END,
        NEW.id_role
    )
    RETURNING id_user INTO NEW.id_user;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_decrypted_instead_insert
    INSTEAD OF INSERT ON users_decrypted
    FOR EACH ROW
    EXECUTE FUNCTION trigger_users_decrypted_insert();

-- INSTEAD OF UPDATE on users_decrypted
CREATE OR REPLACE FUNCTION trigger_users_decrypted_update() RETURNS TRIGGER AS $$
BEGIN
    UPDATE users
    SET
        username = NEW.username,
        email = encrypt_field(NEW.email),
        password = encrypt_field(NEW.password),
        phone = CASE WHEN NEW.phone IS NOT NULL THEN encrypt_field(NEW.phone) ELSE NULL END,
        id_role = NEW.id_role
    WHERE id_user = OLD.id_user;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_decrypted_instead_update
    INSTEAD OF UPDATE ON users_decrypted
    FOR EACH ROW
    EXECUTE FUNCTION trigger_users_decrypted_update();

-- INSTEAD OF DELETE on users_decrypted
CREATE OR REPLACE FUNCTION trigger_users_decrypted_delete() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM users WHERE id_user = OLD.id_user;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER users_decrypted_instead_delete
    INSTEAD OF DELETE ON users_decrypted
    FOR EACH ROW
    EXECUTE FUNCTION trigger_users_decrypted_delete();

-- INSTEAD OF INSERT on orders_decrypted
CREATE OR REPLACE FUNCTION trigger_orders_decrypted_insert() RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO orders (id_user, id_status, order_date, total_amount, shipping_address)
    VALUES (
        NEW.id_user,
        NEW.id_status,
        NEW.order_date,
        NEW.total_amount,
        encrypt_field(NEW.shipping_address)
    )
    RETURNING id_order INTO NEW.id_order;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_decrypted_instead_insert
    INSTEAD OF INSERT ON orders_decrypted
    FOR EACH ROW
    EXECUTE FUNCTION trigger_orders_decrypted_insert();

-- INSTEAD OF UPDATE on orders_decrypted
CREATE OR REPLACE FUNCTION trigger_orders_decrypted_update() RETURNS TRIGGER AS $$
BEGIN
    UPDATE orders
    SET
        id_user = NEW.id_user,
        id_status = NEW.id_status,
        order_date = NEW.order_date,
        total_amount = NEW.total_amount,
        shipping_address = encrypt_field(NEW.shipping_address)
    WHERE id_order = OLD.id_order;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_decrypted_instead_update
    INSTEAD OF UPDATE ON orders_decrypted
    FOR EACH ROW
    EXECUTE FUNCTION trigger_orders_decrypted_update();

-- INSTEAD OF DELETE on orders_decrypted
CREATE OR REPLACE FUNCTION trigger_orders_decrypted_delete() RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM orders WHERE id_order = OLD.id_order;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_decrypted_instead_delete
    INSTEAD OF DELETE ON orders_decrypted
    FOR EACH ROW
    EXECUTE FUNCTION trigger_orders_decrypted_delete();

-- ============================================================================
-- SEED INITIAL DATA
-- ============================================================================

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
