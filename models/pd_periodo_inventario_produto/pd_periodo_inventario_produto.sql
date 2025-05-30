{{
    config(
        materialized="incremental",
        table_type="iceberg",
        incremental_strategy="merge",
        partitioned_by=['bucket(filial, 10)', 'bucket(prme_cd_produto, 16)'],
        unique_key=["filial", "prme_cd_produto"],
        format="parquet",
        write_compression="ZSTD",
        table_properties={"optimize_rewrite_delete_file_threshold": "2"},
        pre_hook=[
            "{% if is_incremental(name = this.identifier ) %} DELETE FROM {{ this }} WHERE filial in(select distinct filial from {{ ref('periodo_inventario').render() }}) {% endif %}"
        ],
        post_hook=[
            "OPTIMIZE {{ this.render_pure() }} REWRITE DATA USING BIN_PACK",
            "VACUUM {{ this.render_pure() }}",
        ],
    )
}}

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
