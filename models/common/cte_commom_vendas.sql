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
group by 1, 2, 3, 4, 5
