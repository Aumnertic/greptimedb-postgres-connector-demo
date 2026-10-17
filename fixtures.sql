-- Isolated demo database; these credentials are public, disposable fixtures.
CREATE ROLE connector_reader LOGIN PASSWORD 'demo_reader_password';
CREATE SCHEMA connector_demo;

CREATE TABLE connector_demo.customers (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    level TEXT,
    active BOOLEAN NOT NULL,
    credit DOUBLE PRECISION
);

INSERT INTO connector_demo.customers VALUES
    (1, 'Alice', 'bronze', true, 10.0),
    (2, 'Bob', 'silver', true, 20.0),
    (3, 'Carol', 'gold', true, NULL),
    (4, 'Dave', NULL, false, 40.0),
    (5, 'Eve', 'gold', true, 50.0),
    (6, 'O''Reilly', 'silver', false, 60.0);

CREATE TABLE connector_demo.stream_rows (id BIGINT NOT NULL, label TEXT);
INSERT INTO connector_demo.stream_rows
SELECT n, CASE WHEN n % 3 = 0 THEN NULL ELSE 'row-' || n END
FROM generate_series(1, 10000) n;

CREATE TABLE connector_demo.supported_types (
    small SMALLINT, medium INTEGER, large BIGINT, flag BOOLEAN,
    single REAL, double_value DOUBLE PRECISION, text_value TEXT,
    bytes BYTEA
);
INSERT INTO connector_demo.supported_types VALUES
    (-32768, -2147483648, -9223372036854775808, true,
     1.5, 2.5, '你好 PostgreSQL', decode('00ff', 'hex')),
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

CREATE TABLE connector_demo.empty_customers (LIKE connector_demo.customers);
CREATE TABLE connector_demo.unsupported_type (amount NUMERIC(38, 2));
CREATE TABLE connector_demo.timestamp_collision (greptime_timestamp BIGINT);
CREATE TABLE connector_demo."odd""table" ("odd""id" BIGINT);
INSERT INTO connector_demo."odd""table" VALUES (7);

GRANT USAGE ON SCHEMA connector_demo TO connector_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA connector_demo TO connector_reader;
