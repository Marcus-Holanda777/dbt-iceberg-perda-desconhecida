with
    entrada_direta as (
        select distinct det.nede_cd_produto as prme_cd_produto
        from {{ source("modelled", "cosmos_v14b_dbo_nota_entr_cab") }} cab
        inner join
            {{ source("modelled", "cosmos_v14b_dbo_nota_entr_det") }} det
            on cab.neca_sq_notafiscal = det.neca_sq_notafiscal
        where cab.fili_cd_filial is not null

        except

        select distinct det.nede_cd_produto as prme_cd_produto
        from {{ source("modelled", "cosmos_v14b_dbo_nota_entr_cab") }} cab
        inner join
            {{ source("modelled", "cosmos_v14b_dbo_nota_entr_det") }} det
            on cab.neca_sq_notafiscal = det.neca_sq_notafiscal
        where cab.depo_cd_deposito is not null
    ),

    termolabeis as (
        select distinct dep.prme_cd_produto
        from {{ source("modelled", "cosmos_v14b_dbo_produto_deposito") }} as dep
        inner join
            {{ source("modelled", "cosmos_v14b_dbo_zona_separacao") }} as z
            on dep.id_zona = z.id_zona
        where upper(z.descricao) like 'TERMO%' or upper(z.descricao) like 'GELA%'

        union

        select distinct dep.prme_cd_produto
        from {{ source("prevencao-perdas", "cosmos_v14b_dbo_endereco_produto") }} as dep
        inner join
            {{ source("prevencao-perdas", "cosmos_v14b_dbo_item") }} as it
            on dep.id_item = it.id_item
        inner join
            {{ source("prevencao-perdas", "cosmos_v14b_dbo_nivel") }} as nv
            on it.id_nivel = nv.id_nivel
        inner join
            {{ source("prevencao-perdas", "cosmos_v14b_dbo_estante") }} as es
            on nv.id_estante = es.id_estante
        inner join
            {{ source("prevencao-perdas", "cosmos_v14b_dbo_estacao") }} as est
            on es.id_estacao = est.id_estacao
        inner join
            {{ source("modelled", "cosmos_v14b_dbo_zona_separacao") }} as z
            on est.id_zona = z.id_zona
        where upper(z.descricao) like 'TERMO%' or upper(z.descricao) like 'GELA%'
    ),
    categoria_nivel2 as (
        select pm.prme_cd_produto, upper(trim(n2.capn_nm_categoria)) categ_n2
        from {{ source("modelled", "cosmos_v14b_dbo_produto_mestre") }} as pm
        inner join
            {{ source("modelled", "cosmos_v14b_dbo_categoria_produto_novo") }} as n2
            on substr(pm.capn_cd_categoria, 1, 5) || '.000.00.00.00.00.00'
            = n2.capn_cd_categoria

    )

select
    pm.prme_cd_produto,
    case when categ_n2 = 'RX' then 1 else 0 end as rx,
    case
        when categ_n2 in ('OTC', 'DERMO', 'HIGIENE', 'BELEZA') then 1 else 0
    end as salao,
    case
        when pm.tplp_sg_psico is not null or t.prme_cd_produto is not null then 1 else 0
    end as psico_termo,
    case when ed.prme_cd_produto is not null then 1 else 0 end as entrada_direta,
    case when upper(trim(pm.marca_propria)) = 'S' then 1 else 0 end as marca_propria
from {{ source("modelled", "cosmos_v14b_dbo_produto_mestre") }} as pm
left join categoria_nivel2 as n on pm.prme_cd_produto = n.prme_cd_produto
left join entrada_direta as ed on pm.prme_cd_produto = ed.prme_cd_produto
left join termolabeis as t on pm.prme_cd_produto = t.prme_cd_produto
