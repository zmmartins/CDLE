-- A small star-schema query: the store_sales fact table joined with two dimension tables.
-- Revenue per item category in the year 2000.
-- The last row has category NULL: some items have no category, and GROUP BY puts them together.
select i.i_category,
       count(*) as sales_lines,
       sum(ss.ss_ext_sales_price) as revenue
from store_sales ss
join date_dim d on ss.ss_sold_date_sk = d.d_date_sk
join item i on ss.ss_item_sk = i.i_item_sk
where d.d_year = 2000
group by i.i_category
order by revenue desc;
