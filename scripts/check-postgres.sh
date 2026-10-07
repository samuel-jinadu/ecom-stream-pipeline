#!/bin/bash

PGPASSWORD=1234 psql -U kafka_user -h localhost -d ecommerce -c "
SELECT 'orders' AS t, count(*) FROM orders
UNION ALL SELECT 'orders_enriched', count(*) FROM orders_enriched
UNION ALL SELECT 'products', count(*) FROM products
UNION ALL SELECT 'inventory', count(*) FROM inventory
UNION ALL SELECT 'top_ten_products', count(*) FROM top_ten_products
UNION ALL SELECT 'alerts', count(*) FROM alerts;"