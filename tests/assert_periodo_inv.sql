with
    test_periodo_inv as (
        select cast(max(kafi_dh_ocorrreal) as date) as max_periodo
        from {{ source("prevencao-perdas", "kardex_perdas") }}
        where kafi_tp_mov in ('EA', 'SA', 'E9', 'S9') and kafi_tx_nr_docto like '%/%'
    ),
    diff_date as (
        select date_diff('day', max_periodo, current_date) as diff from test_periodo_inv
    )

select case when diff >= 2 then false else true end as test
from diff_date
