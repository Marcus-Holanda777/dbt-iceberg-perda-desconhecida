{{
    config(
        materialized="incremental",
        table_type="iceberg",
        incremental_strategy="merge",
        unique_key="filial",
        format="parquet",
        write_compression="ZSTD",
        table_properties={"optimize_rewrite_delete_file_threshold": "2"},
        post_hook=[
            "OPTIMIZE {{ this.render_pure() }} REWRITE DATA USING BIN_PACK",
            "VACUUM {{ this.render_pure() }}",
        ],
    )
}}

with
    pre_resumo as (
        select *
        from {{ ref("perdas") }}
        full outer join {{ ref("vendas") }} using (filial)
    ),
    pos_resumo as (
        select
            filial,
            ultimo_inventario,
            penultimo_inventario,
            ante_penultimo_inventario,
            {{ columns_rst("valor_inv") }},
            {{ columns_rst("valor_inv_ant") }},
            {{ columns_rst("venda_inv", flag=false) }},
            {{ columns_rst("venda_inv_ant", flag=false) }},
            {{ columns_perc("valor_inv") }},
            {{ columns_perc("valor_inv_ant") }},
            {{ columns_ind("valor_inv", "venda_inv") }},
            {{ columns_ind("valor_inv_ant", "venda_inv_ant") }}
        from pre_resumo
    ),
    resumo as (
        select
            *,
            case
                when ind_inv * 100 > 0.70
                then 'Extrema'
                when ind_inv * 100 > 0.40
                then 'Alta'
                when ind_inv * 100 > 0.30
                then 'Media'
                when ind_inv * 100 < 0.30
                then 'Baixa'
            end as criticidade,
            valor_inv / nullif(
                date_diff('day', penultimo_inventario, ultimo_inventario), 0
            ) as perda_diaria_inv,
            valor_inv_ant / nullif(
                date_diff('day', ante_penultimo_inventario, penultimo_inventario), 0
            ) as perda_diaria_inv_ant
        from pos_resumo
    )
select
    rst.filial,
    rst.ultimo_inventario,
    rst.penultimo_inventario,
    rst.ante_penultimo_inventario,
    fil.estado,
    fil.cidade,
    fil.diretor,
    fil.gr_novo,
    fil.supervisor,
    fil.coordenador,
    atl.antenas_af,
    atl.agente_pp,
    atl.vig_dinamica,
    atl.fiscal_pp,
    rst.valor_inv,
    rst.valor_inv_rx,
    rst.valor_inv_salao,
    rst.valor_inv_psico_termo,
    rst.valor_inv_entrada_direta,
    rst.valor_inv_grt,
    rst.valor_inv_marca_propria,
    rst.valor_inv_ant,
    rst.valor_inv_ant_rx,
    rst.valor_inv_ant_salao,
    rst.valor_inv_ant_psico_termo,
    rst.valor_inv_ant_entrada_direta,
    rst.valor_inv_ant_grt,
    rst.valor_inv_ant_marca_propria,
    rst.venda_inv,
    rst.venda_inv_rx,
    rst.venda_inv_salao,
    rst.venda_inv_psico_termo,
    rst.venda_inv_entrada_direta,
    rst.venda_inv_marca_propria,
    rst.venda_inv_ant,
    rst.venda_inv_ant_rx,
    rst.venda_inv_ant_salao,
    rst.venda_inv_ant_psico_termo,
    rst.venda_inv_ant_entrada_direta,
    rst.venda_inv_ant_marca_propria,
    rst.perc_inv_rx,
    rst.perc_inv_salao,
    rst.perc_inv_psico_termo,
    rst.perc_inv_entrada_direta,
    rst.perc_inv_grt,
    rst.perc_inv_marca_propria,
    rst.perc_inv_ant_rx,
    rst.perc_inv_ant_salao,
    rst.perc_inv_ant_psico_termo,
    rst.perc_inv_ant_entrada_direta,
    rst.perc_inv_ant_grt,
    rst.perc_inv_ant_marca_propria,
    rst.ind_inv,
    rst.ind_inv_rx,
    rst.ind_inv_salao,
    rst.ind_inv_psico_termo,
    rst.ind_inv_entrada_direta,
    rst.ind_inv_marca_propria,
    rst.ind_inv_ant,
    rst.ind_inv_ant_rx,
    rst.ind_inv_ant_salao,
    rst.ind_inv_ant_psico_termo,
    rst.ind_inv_ant_entrada_direta,
    rst.ind_inv_ant_marca_propria,
    rst.criticidade,
    rst.perda_diaria_inv,
    rst.perda_diaria_inv_ant
from resumo as rst
inner join
    {{ source("prevencao-perdas", "excel_base_supervisores") }} as fil
    on rst.filial = fil.filial
left join
    {{ source("prevencao-perdas", "excel_base_seguranca_atual") }} as atl
    on rst.filial = atl.filial
