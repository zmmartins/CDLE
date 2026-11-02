-- A few rows of the store_sales fact table.
-- Each row is one line of a sales ticket: keys to the date, item, customer and store,
-- then quantity and amounts. Empty cells are NULLs.
-- Without ORDER BY, which 10 rows you get is not guaranteed.
select ss_ticket_number,
       ss_sold_date_sk,
       ss_item_sk,
       ss_customer_sk,
       ss_store_sk,
       ss_quantity,
       ss_sales_price,
       ss_net_profit
from store_sales
limit 10;
