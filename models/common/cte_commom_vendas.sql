with
    start_periodo as (
        select
            cast(min(ante_penultimo_inventario) as timestamp)
            - interval '0.001' second as start_at
        from {{ ref("periodo_inventario") }}
        where ante_penultimo_inventario is not null
    )
select
    inv.filial,
    kp.kafi_cd_produto as prme_cd_produto,
    inv.ultimo_inventario,
    inv.penultimo_inventario,
    inv.ante_penultimo_inventario,
    sum(kafi_vl_preven) filter (
        where {{ range_inv("penultimo_inventario", "ultimo_inventario") }}
    ) as venda_inv,
    sum(kafi_vl_preven) filter (
        where {{ range_inv("ante_penultimo_inventario", "penultimo_inventario") }}
    ) as venda_inv_ant
from {{ source("prevencao-perdas", "kardex_vendas") }} as kp
inner join {{ ref("periodo_inventario") }} as inv on kp.kafi_cd_filial = inv.filial
inner join
    {{ source("modelled", "cosmos_v14b_dbo_produto_mestre") }} as pm
    on kp.kafi_cd_produto = pm.prme_cd_produto
where
    0 = 0
    and pm.capn_cd_categoria not like '3%'
    and pm.capn_cd_categoria not like '1.101.009%'
    and pm.capn_cd_categoria not like '1.102.009%'
    and pm.capn_cd_categoria not like '2.504.001%'
    {% if is_incremental() %}
        and kp.kafi_dh_ocorrreal > (select start_at from start_periodo)
    {% else %} and {{ filtra_periodo() }}
    {% endif %}
group by 1, 2, 3, 4, 5
