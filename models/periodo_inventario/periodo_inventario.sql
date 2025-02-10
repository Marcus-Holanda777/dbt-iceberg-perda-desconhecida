{% set inventarios = ["ultimo", "penultimo", "ante_penultimo"] %}

with
    pre_inventarios as (
        select
            kafi_cd_filial as filial,
            date_trunc('month', kp.kafi_dh_ocorrreal) as dt_mov,
            max(date(kp.kafi_dh_ocorrreal)) as dt_inv
        from {{ source("prevencao-perdas", "kardex_perdas") }} as kp
        where
            kp.kafi_tp_mov in ('EA', 'SA', 'E9', 'S9')
            and kp.kafi_tx_nr_docto like '%/%'
            {% if is_incremental() %}

                and kafi_cd_filial in(
                    select distinct kafi_cd_filial
                    from {{ source("prevencao-perdas", "kardex_perdas") }} as kp
                    where kafi_dh_ocorrreal > (
                        select cast(max(ultimo_inventario) as timestamp) + interval '1' day - interval '0.001' second
                        from "prevencao-perdas".pd_periodo_inventario_filial
                    )
                    and kp.kafi_tp_mov in('EA', 'SA', 'E9', 'S9')
                    and kp.kafi_tx_nr_docto like '%/%'
                )

            {% endif %}
            and {{ filtra_periodo() }}
        group by 1, 2
    ),

    pos_inventarios as (
        select t.filial, t.dt_inv, t.id
        from
            (
                select
                    filial,
                    dt_inv,
                    row_number() over (partition by filial order by dt_inv desc) as id
                from pre_inventarios
            ) as t
        where t.id <= 3
    )

select
    filial,
    {%- for inv in inventarios %}
        max(
            case when id = {{ loop.index }} then dt_inv else null end
        ) as {{ inv }}_inventario
        {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
from pos_inventarios
group by 1
