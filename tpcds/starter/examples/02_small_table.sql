-- All rows of a small dimension table: the 20 shipping modes.
-- Every dimension table has a numeric surrogate key (*_sk) and a text business key (*_id).
select *
from ship_mode
order by sm_ship_mode_sk;
