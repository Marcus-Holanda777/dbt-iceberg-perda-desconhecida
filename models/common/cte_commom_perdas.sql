SELECT 
    inv.filial,
    kp.kafi_cd_produto as prme_cd_produto,
    kp.kafi_tp_mov,
    inv.ultimo_inventario,
    inv.penultimo_inventario,
    inv.ante_penultimo_inventario,
    SUM({{ totais_pd('kafi_vl_cmpcsicms') }}) filter(where {{ range_inv('penultimo_inventario', 'ultimo_inventario') }})         as valor_inv,
    SUM({{ totais_pd('kafi_vl_cmpcsicms') }}) filter(where {{ range_inv('ante_penultimo_inventario', 'penultimo_inventario') }}) as valor_inv_ant 
FROM {{ source('prevencao-perdas', 'kardex_perdas') }}                as kp
INNER JOIN {{ ref('periodo_inventario') }}                            as inv  on kp.kafi_cd_filial  = inv.filial
INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_produto_mestre') }} as pm   on kp.kafi_cd_produto = pm.prme_cd_produto
WHERE 
    REGEXP_LIKE(kp.kafi_tp_mov, '^.{1}[AO96]$')
    AND kp.kafi_tx_nr_docto  NOT LIKE '%INVT.INI.EF%'
    AND kp.kafi_fl_tipoperda IS NULL
    AND pm.capn_cd_categoria NOT LIKE '3%'
    AND pm.capn_cd_categoria NOT LIKE '1.101.009%'
    AND pm.capn_cd_categoria NOT LIKE '1.102.009%'
    AND pm.capn_cd_categoria NOT LIKE '2.504.001%'
GROUP BY 1, 2, 3, 4, 5, 6