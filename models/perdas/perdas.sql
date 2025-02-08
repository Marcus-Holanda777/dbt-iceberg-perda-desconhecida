with pre_perdas as 
(
SELECT * FROM {{ ref('cte_commom_perdas') }}
)

select 
    inv.filial,
    inv.ultimo_inventario,
    inv.penultimo_inventario,
    inv.ante_penultimo_inventario,
	{{ columns_tipos('valor_inv') }},
	{{ columns_tipos('valor_inv_ant') }}
from pre_perdas as inv inner join {{ ref('info_produtos') }} as info using(prme_cd_produto)
group by 1, 2, 3, 4