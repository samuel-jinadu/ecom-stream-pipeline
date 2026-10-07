DROP SCHEMA public CASCADE;
CREATE SCHEMA public;


CREATE TABLE IF NOT EXISTS orders (
    order_id     TEXT PRIMARY KEY,
    product_id   TEXT NOT NULL,
    quantity     INT  NOT NULL,
    order_date   DATE
);

CREATE TABLE IF NOT EXISTS orders_enriched (
    order_id     TEXT PRIMARY KEY,
    product_id   TEXT NOT NULL,
    quantity     INT  NOT NULL,
    order_date   DATE,
    revenue      NUMERIC(12,2)
);

CREATE INDEX IF NOT EXISTS idx_orders_product ON orders(product_id);

CREATE TABLE IF NOT EXISTS inventory (
    product_id  TEXT PRIMARY KEY,
    stock       INT  NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    id          SERIAL PRIMARY KEY,
    product_id  TEXT NOT NULL,
    alert_type  TEXT NOT NULL,
    message     TEXT,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    id  TEXT PRIMARY KEY,
    name        TEXT,
    price       NUMERIC(10,2)
);

-- views 

CREATE OR REPLACE VIEW top_ten_products AS
SELECT products.name as product, SUM(orders_enriched.revenue) as revenue
FROM products INNER JOIN orders_enriched
ON products.id = orders_enriched.product_id
WHERE orders_enriched.order_date = CURRENT_DATE - 1
GROUP BY products.id, products.name
ORDER BY SUM(orders_enriched.revenue) DESC
LIMIT 10;

CREATE OR REPLACE VIEW low_stock_products AS
SELECT products.id, products.name, inventory.stock, inventory.updated_at
FROM inventory INNER JOIN products
ON inventory.product_id = products.id
WHERE  inventory.stock < 10 ORDER BY  inventory.stock ASC;