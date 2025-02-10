select count(*)
from {{ source("prevencao-perdas", "kardex_perdas") }} as kp
where
    kafi_dh_ocorrreal > (
        select
            cast(max(ultimo_inventario) as timestamp)
            + interval '1' day
            - interval '0.001' second
        from {{ ref("pd_periodo_inventario_filial") }}
    )
    and kp.kafi_tp_mov in ('EA', 'SA', 'E9', 'S9')
    and kp.kafi_tx_nr_docto like '%/%'
having count(*) = 0
