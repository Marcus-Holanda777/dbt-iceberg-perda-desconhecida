{% macro is_incremental() %}

    {% set stmt %}
        select 1 from information_schema.tables
        where table_schema = 'prevencao-perdas'
        and table_name = 'pd_periodo_inventario_filial'
    {% endset %}

    {% if execute %}
        {% set if_exists = run_query(stmt) %}
        {% if if_exists | length > 0 %}
            {{ return(true) }}
        {% else %}
            {{ return(false) }}
        {% endif %}
    {% endif %}

{% endmacro %}