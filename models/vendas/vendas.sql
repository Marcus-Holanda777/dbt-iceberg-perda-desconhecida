with pre_vendas as (select * from {{ ref("cte_commom_vendas") }})

select
    inv.filial,
    {{ columns_tipos("venda_inv", flag=false) }},
    {{ columns_tipos("venda_inv_ant", flag=false) }}
from pre_vendas as inv
inner join {{ ref("info_produtos") }} as info using (prme_cd_produto)
group by 1
