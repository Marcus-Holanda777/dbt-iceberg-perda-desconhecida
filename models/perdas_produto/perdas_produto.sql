with pre_perdas as (select * from {{ ref("cte_commom_perdas") }})

select
    inv.filial,
    prme_cd_produto,
    {{ columns_tipos("valor_inv") }},
    {{ columns_tipos("valor_inv_ant") }}
from pre_perdas as inv
inner join {{ ref("info_produtos") }} as info using (prme_cd_produto)
group by 1, 2
