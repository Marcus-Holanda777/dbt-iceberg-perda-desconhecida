with
    resumo as (
        select *
        from {{ ref("perdas_produto") }}
        full outer join {{ ref("vendas_produto") }} using (filial, prme_cd_produto)
    )
select rst.*
from resumo as rst
inner join
    {{ source("prevencao-perdas", "excel_base_supervisores") }} as fil
    on rst.filial = fil.filial
