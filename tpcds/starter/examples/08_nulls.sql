-- TPC-DS deliberately leaves some values empty (NULL), so queries must handle them.
-- count(column) skips NULLs, so count(*) - count(column) is the number of NULLs.
select count(*) as total_rows,
       count(*) - count(ss_sold_date_sk) as null_sold_date,
       count(*) - count(ss_item_sk) as null_item,
       count(*) - count(ss_customer_sk) as null_customer,
       count(*) - count(ss_store_sk) as null_store,
       count(*) - count(ss_sales_price) as null_sales_price
from store_sales;
