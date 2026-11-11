-- ss_sold_date_sk is a number, not a date: it is a key into the date_dim table.
-- Joining with date_dim turns it into a real date, a day name, a year and a month.
-- Consecutive keys are consecutive days. Sales with an empty (NULL) date key disappear in the join.
select ss.ss_sold_date_sk,
       d.d_date,
       d.d_day_name,
       d.d_year,
       d.d_moy,
       count(*) as sales_lines
from store_sales ss
join date_dim d on ss.ss_sold_date_sk = d.d_date_sk
group by ss.ss_sold_date_sk, d.d_date, d.d_day_name, d.d_year, d.d_moy
order by ss.ss_sold_date_sk
limit 10;
