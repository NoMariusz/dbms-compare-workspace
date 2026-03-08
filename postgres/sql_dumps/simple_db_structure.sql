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
    models (
        id_model SERIAL PRIMARY KEY,
        id_manufacturer INT REFERENCES manufacturers (id_manufacturer),
        model_name VARCHAR(100) NOT NULL
    );

CREATE TABLE
    product_types (
        id_type SERIAL PRIMARY KEY,
        type_name VARCHAR(50) NOT NULL
    );

CREATE TABLE
    products (
        id_product SERIAL PRIMARY KEY,
        id_type INT REFERENCES product_types (id_type),
        id_model INT REFERENCES models (id_model),
        description TEXT,
        release_date DATE
    );

CREATE TABLE
    product_variants (
        id_variant SERIAL PRIMARY KEY,
        id_product INT REFERENCES products (id_product),
        color_name VARCHAR(50),
        size_value VARCHAR(20),
        stock_quantity INT NOT NULL DEFAULT 0,
        price DECIMAL(10, 2) NOT NULL
    );

CREATE TABLE
    gear_specifications (
        id_product INT PRIMARY KEY REFERENCES products (id_product),
        wheel_size INT,
        number_of_wheels INT,
        blade_material VARCHAR(50),
        boot_material VARCHAR(50),
        bearing_type VARCHAR(50)
    );

CREATE TABLE
    order_status (
        id_status INT PRIMARY KEY,
        status_name VARCHAR(50) NOT NULL
    );

CREATE TABLE
    orders (
        id_order SERIAL PRIMARY KEY,
        id_user INT REFERENCES users (id_user),
        id_status INT REFERENCES order_status (id_status),
        order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        total_amount DECIMAL(10, 2) NOT NULL,
        shipping_address TEXT NOT NULL
    );

CREATE TABLE
    order_items (
        id_order_item SERIAL PRIMARY KEY,
        id_order INT REFERENCES orders (id_order) ON DELETE CASCADE,
        id_product INT REFERENCES products (id_product),
        id_variant INT REFERENCES product_variants (id_variant),
        quantity INT NOT NULL,
        unit_price DECIMAL(10, 2) NOT NULL
    );

-- Seed Initial Data
INSERT INTO
    user_roles (id_role, role_name)
VALUES
    (1, 'Admin'),
    (2, 'User');

INSERT INTO
    order_status (id_status, status_name)
VALUES
    (1, 'Pending'),
    (2, 'Shipped');

INSERT INTO
    product_types (type_name)
VALUES
    ('In-line Skates'),
    ('Ice Skates');