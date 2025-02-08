with entrada_direta as 
	 (
	    SELECT
	        DISTINCT det.nede_cd_produto AS prme_cd_produto
	    FROM {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_cab') }} cab 
	    INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_det') }} det ON cab.neca_sq_notafiscal = det.neca_sq_notafiscal
	    WHERE cab.fili_cd_filial IS NOT NULL
	
	    EXCEPT
	
	    SELECT 
	        DISTINCT det.nede_cd_produto AS prme_cd_produto
	    FROM {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_cab') }} cab 
	    INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_det') }} det ON cab.neca_sq_notafiscal = det.neca_sq_notafiscal 
	    WHERE cab.depo_cd_deposito IS NOT NULL
	 ),

termolabeis as
	 (
	    SELECT DISTINCT dep.prme_cd_produto
	    FROM {{ source('modelled', 'cosmos_v14b_dbo_produto_deposito') }}     AS dep
	    INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_zona_separacao') }} AS z   ON dep.id_zona = z.id_zona
	    WHERE UPPER(z.descricao) LIKE 'TERMO%' OR UPPER(z.descricao) LIKE 'GELA%'
	
	    UNION
	
	    SELECT DISTINCT dep.prme_cd_produto 
	    FROM {{ source('prevencao-perdas', 'cosmos_v14b_dbo_endereco_produto') }}    AS dep
	    INNER JOIN {{ source('prevencao-perdas', 'cosmos_v14b_dbo_item') }}          AS it  ON dep.id_item   = it.id_item
	    INNER JOIN {{ source('prevencao-perdas', 'cosmos_v14b_dbo_nivel') }}         AS nv  ON it.id_nivel   = nv.id_nivel
	    INNER JOIN {{ source('prevencao-perdas', 'cosmos_v14b_dbo_estante') }}       AS es  ON nv.id_estante = es.id_estante
	    INNER JOIN {{ source('prevencao-perdas', 'cosmos_v14b_dbo_estacao') }}       AS est ON es.id_estacao = est.id_estacao
	    INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_zona_separacao') }}        AS z   ON est.id_zona   = z.id_zona
	    WHERE UPPER(z.descricao) LIKE 'TERMO%' OR UPPER(z.descricao) LIKE 'GELA%'
	 ), categoria_nivel2 AS
	 (
	    SELECT 
	       pm.prme_cd_produto,
	       UPPER(TRIM(n2.capn_nm_categoria)) categ_n2
	    FROM {{ source('modelled', 'cosmos_v14b_dbo_produto_mestre') }}               AS pm
	    INNER JOIN {{ source('modelled', 'cosmos_v14b_dbo_categoria_produto_novo') }} AS n2 ON SUBSTR(pm.capn_cd_categoria, 1, 5) || '.000.00.00.00.00.00' = n2.capn_cd_categoria
	 
	 )

select 
	pm.prme_cd_produto,
	case when categ_n2 = 'RX' then 1 else 0 end                                               as rx,
	case when categ_n2 in('OTC', 'DERMO', 'HIGIENE', 'BELEZA') then 1 else 0 end              as salao,
	case when pm.tplp_sg_psico is not null or t.prme_cd_produto is not NULL then 1 else 0 end as psico_termo,
	case when ed.prme_cd_produto is not null then 1 else 0 end                                as entrada_direta,
	case when UPPER(TRIM(pm.marca_propria)) = 'S' then 1 else 0 end                           as marca_propria
from {{ source('modelled', 'cosmos_v14b_dbo_produto_mestre') }}  as pm
left join categoria_nivel2 as n  on pm.prme_cd_produto  = n.prme_cd_produto
left join entrada_direta   as ed on pm.prme_cd_produto  = ed.prme_cd_produto
left join termolabeis      as t  on pm.prme_cd_produto  = t.prme_cd_produto