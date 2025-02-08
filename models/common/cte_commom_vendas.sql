SELECT 
	inv.filial,
	kp.kafi_cd_produto as prme_cd_produto,
	inv.ultimo_inventario,
	inv.penultimo_inventario,
	inv.ante_penultimo_inventario,
	SUM(kafi_vl_preven) filter(where {{ range_inv('penultimo_inventario', 'ultimo_inventario') }})         as venda_inv,
	SUM(kafi_vl_preven) filter(where {{ range_inv('ante_penultimo_inventario', 'penultimo_inventario') }}) as venda_inv_ant 
FROM {{ source('prevencao-perdas', 'kardex_vendas') }}                as kp
INNER JOIN {{ ref('periodo_inventario') }}                            as inv  on kp.kafi_cd_filial  = inv.filial
INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_produto_mestre') }} as pm   on kp.kafi_cd_produto = pm.prme_cd_produto
WHERE 0 = 0
    AND pm.capn_cd_categoria NOT LIKE '3%'
    AND pm.capn_cd_categoria NOT LIKE '1.101.009%'
    AND pm.capn_cd_categoria NOT LIKE '1.102.009%'
    AND pm.capn_cd_categoria NOT LIKE '2.504.001%'
GROUP BY 1, 2, 3, 4, 5