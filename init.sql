CREATE TABLE public.mock_data (
                                  id int4 NULL,
                                  customer_first_name varchar(50) NULL,
                                  customer_last_name varchar(50) NULL,
                                  customer_age int4 NULL,
                                  customer_email varchar(50) NULL,
                                  customer_country varchar(50) NULL,
                                  customer_postal_code varchar(50) NULL,
                                  customer_pet_type varchar(50) NULL,
                                  customer_pet_name varchar(50) NULL,
                                  customer_pet_breed varchar(50) NULL,
                                  seller_first_name varchar(50) NULL,
                                  seller_last_name varchar(50) NULL,
                                  seller_email varchar(50) NULL,
                                  seller_country varchar(50) NULL,
                                  seller_postal_code varchar(50) NULL,
                                  product_name varchar(50) NULL,
                                  product_category varchar(50) NULL,
                                  product_price float4 NULL,
                                  product_quantity int4 NULL,
                                  sale_date varchar(50) NULL,
                                  sale_customer_id int4 NULL,
                                  sale_seller_id int4 NULL,
                                  sale_product_id int4 NULL,
                                  sale_quantity int4 NULL,
                                  sale_total_price float4 NULL,
                                  store_name varchar(50) NULL,
                                  store_location varchar(50) NULL,
                                  store_city varchar(50) NULL,
                                  store_state varchar(50) NULL,
                                  store_country varchar(50) NULL,
                                  store_phone varchar(50) NULL,
                                  store_email varchar(50) NULL,
                                  pet_category varchar(50) NULL,
                                  product_weight float4 NULL,
                                  product_color varchar(50) NULL,
                                  product_size varchar(50) NULL,
                                  product_brand varchar(50) NULL,
                                  product_material varchar(50) NULL,
                                  product_description varchar(1024) NULL,
                                  product_rating float4 NULL,
                                  product_reviews int4 NULL,
                                  product_release_date varchar(50) NULL,
                                  product_expiry_date varchar(50) NULL,
                                  supplier_name varchar(50) NULL,
                                  supplier_contact varchar(50) NULL,
                                  supplier_email varchar(50) NULL,
                                  supplier_phone varchar(50) NULL,
                                  supplier_address varchar(50) NULL,
                                  supplier_city varchar(50) NULL,
                                  supplier_country varchar(50) NULL
);

COPY mock_data FROM '/csv/MOCK_DATA.csv'      DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (1).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (2).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (3).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (4).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (5).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (6).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (7).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (8).csv'  DELIMITER ',' CSV HEADER;
COPY mock_data FROM '/csv/MOCK_DATA (9).csv'  DELIMITER ',' CSV HEADER;

-- ==================== Snowflake schema for Flink sink ====================

CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country      VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE dim_city (
    city_id    SERIAL PRIMARY KEY,
    city       VARCHAR(100) NOT NULL,
    state      VARCHAR(100),
    CONSTRAINT uq_dim_city UNIQUE (city, state)
);

CREATE TABLE dim_location (
    location_id  SERIAL PRIMARY KEY,
    city_id      INT REFERENCES dim_city(city_id),
    country_id   INT REFERENCES dim_country(country_id),
    postal_code  VARCHAR(20),
    address      VARCHAR(255),
    CONSTRAINT uq_dim_location UNIQUE (city_id, country_id, postal_code, address)
);

CREATE TABLE dim_pet_type (
    pet_type_id SERIAL PRIMARY KEY,
    type VARCHAR(50),
    CONSTRAINT uq_dim_pet_type UNIQUE (type)
);

CREATE TABLE dim_pet_breed (
    pet_breed_id SERIAL PRIMARY KEY,
    breed VARCHAR(100),
    CONSTRAINT uq_dim_pet_breed UNIQUE (breed)
);

CREATE TABLE dim_pet (
    pet_id      SERIAL PRIMARY KEY,
    pet_type_id INT REFERENCES dim_pet_type(pet_type_id),
    pet_breed   INT REFERENCES dim_pet_breed(pet_breed_id),
    CONSTRAINT uq_dim_pet UNIQUE (pet_type_id, pet_breed)
);

CREATE TABLE dim_category (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_category UNIQUE (category_name)
);

CREATE TABLE dim_pet_category (
    pet_category_id   SERIAL PRIMARY KEY,
    category_name     VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_pet_category UNIQUE (category_name)
);

CREATE TABLE dim_brand (
    brand_id    SERIAL PRIMARY KEY,
    brand_name  VARCHAR(100) NOT NULL,
    CONSTRAINT uq_dim_brand UNIQUE (brand_name)
);

CREATE TABLE dim_date (
    date_id      SERIAL PRIMARY KEY,
    full_date    DATE NOT NULL,
    year         INT,
    month        INT,
    day_of_week  INT,
    CONSTRAINT uq_dim_date UNIQUE (full_date)
);

CREATE TABLE dim_customer (
    customer_id   SERIAL PRIMARY KEY,
    first_name    VARCHAR(100),
    last_name     VARCHAR(100),
    age           INT,
    email         VARCHAR(255),
    location_id   INT REFERENCES dim_location(location_id),
    pet_id        INT REFERENCES dim_pet(pet_id),
    pet_name      VARCHAR(100),
    CONSTRAINT uq_dim_customer UNIQUE (first_name, last_name, email, location_id, pet_id, pet_name)
);

CREATE TABLE dim_seller (
    seller_id    SERIAL PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    email        VARCHAR(255),
    location_id  INT REFERENCES dim_location(location_id),
    CONSTRAINT uq_dim_seller UNIQUE (first_name, last_name, email, location_id)
);

CREATE TABLE dim_product (
    product_id          SERIAL PRIMARY KEY,
    product_name        VARCHAR(255),
    category_id         INT REFERENCES dim_category(category_id),
    pet_category_id     INT REFERENCES dim_pet_category(pet_category_id),
    brand_id            INT REFERENCES dim_brand(brand_id),
    price               NUMERIC(10,2),
    quantity            INT,
    weight              NUMERIC(10,2),
    color               VARCHAR(50),
    size                VARCHAR(50),
    material            VARCHAR(100),
    description         TEXT,
    rating              NUMERIC(3,1),
    reviews             INT,
    release_date_id     INT REFERENCES dim_date(date_id),
    expiry_date_id      INT REFERENCES dim_date(date_id),
    CONSTRAINT uq_dim_product UNIQUE (product_name, category_id, pet_category_id, brand_id, release_date_id, expiry_date_id)
);

CREATE TABLE dim_store (
    store_id     SERIAL PRIMARY KEY,
    store_name   VARCHAR(255),
    location_id  INT REFERENCES dim_location(location_id),
    phone        VARCHAR(50),
    email        VARCHAR(255),
    CONSTRAINT uq_dim_store UNIQUE (store_name, location_id, phone, email)
);

CREATE TABLE dim_supplier (
    supplier_id  SERIAL PRIMARY KEY,
    name         VARCHAR(255),
    contact      VARCHAR(255),
    email        VARCHAR(255),
    phone        VARCHAR(50),
    location_id  INT REFERENCES dim_location(location_id),
    CONSTRAINT uq_dim_supplier UNIQUE (name, contact, email, phone, location_id)
);

CREATE TABLE fact_sales (
    sale_id      SERIAL PRIMARY KEY,
    date_id      INT REFERENCES dim_date(date_id),
    customer_id  INT REFERENCES dim_customer(customer_id),
    seller_id    INT REFERENCES dim_seller(seller_id),
    product_id   INT REFERENCES dim_product(product_id),
    store_id     INT REFERENCES dim_store(store_id),
    supplier_id  INT REFERENCES dim_supplier(supplier_id),
    quantity     INT,
    total_price  NUMERIC(10,2),
    CONSTRAINT uq_fact_sales UNIQUE (date_id, customer_id, seller_id, product_id, store_id, supplier_id)
);
