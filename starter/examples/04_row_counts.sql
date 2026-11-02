-- Row count of every table. Compare with Appendix A of the handout.
-- Dimension tables such as date_dim or time_dim have the same size at every scale.
select 'call_center' as table_name, count(*) as row_count from call_center
union all select 'catalog_page', count(*) from catalog_page
union all select 'catalog_returns', count(*) from catalog_returns
union all select 'catalog_sales', count(*) from catalog_sales
union all select 'customer', count(*) from customer
union all select 'customer_address', count(*) from customer_address
union all select 'customer_demographics', count(*) from customer_demographics
union all select 'date_dim', count(*) from date_dim
union all select 'dbgen_version', count(*) from dbgen_version
union all select 'household_demographics', count(*) from household_demographics
union all select 'income_band', count(*) from income_band
union all select 'inventory', count(*) from inventory
union all select 'item', count(*) from item
union all select 'promotion', count(*) from promotion
union all select 'reason', count(*) from reason
union all select 'ship_mode', count(*) from ship_mode
union all select 'store', count(*) from store
union all select 'store_returns', count(*) from store_returns
union all select 'store_sales', count(*) from store_sales
union all select 'time_dim', count(*) from time_dim
union all select 'warehouse', count(*) from warehouse
union all select 'web_page', count(*) from web_page
union all select 'web_returns', count(*) from web_returns
union all select 'web_sales', count(*) from web_sales
union all select 'web_site', count(*) from web_site
order by table_name;
