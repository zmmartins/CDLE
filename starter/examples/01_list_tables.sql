-- The 25 TPC-DS tables.
-- Seven are fact tables (store_sales, store_returns, catalog_sales, catalog_returns,
-- web_sales, web_returns, inventory); the others are dimension tables that describe them.
select table_name
from information_schema.tables
order by table_name;
