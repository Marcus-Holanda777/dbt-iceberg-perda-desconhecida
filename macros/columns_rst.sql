{% macro set_tipos() %}
    {% set tipos = [
        "rx",
        "salao",
        "psico_termo",
        "entrada_direta",
        "grt",
        "marca_propria",
    ] %}
    {{ return(tipos) }}
{% endmacro %}

{% macro columns_rst(column_value, flag=true) %}
    {%- set tipos = set_tipos() -%}
    {{ column_value }},
    {% for tp in tipos -%}
        {%- if tp == "grt" and not flag -%} {% continue %} {%- endif -%}
        {{ column_value }}_{{ tp }}
        {%- if not loop.last -%}, {% endif %}
    {%- endfor -%}
{% endmacro %}

{% macro columns_perc(column_value, flag=true) %}
    {%- set tipos = set_tipos() -%}
    {%- set suff = column_value.split("_", 1)[1] -%}
    {% for tp in tipos -%}
        {%- if tp == "grt" and not flag -%} {% continue %} {%- endif -%}
        {{ column_value }}_{{ tp }}
        / nullif({{ column_value }}, 0) as perc_{{ suff }}_{{ tp }}
        {%- if not loop.last -%}, {% endif %}
    {%- endfor -%}
{% endmacro %}

{% macro columns_ind(column_inv, column_venda, flag=false) %}
    {%- set tipos = set_tipos() -%}
    {%- set suff = column_inv.split("_", 1)[1] -%}
    {{ column_inv }} / nullif({{ column_venda }}, 0) as ind_{{ suff }},
    {% for tp in tipos -%}
        {%- if tp == "grt" and not flag -%} {% continue %} {%- endif -%}
        {{ column_inv }}_{{ tp }}
        / nullif({{ column_venda }}, 0) as ind_{{ suff }}_{{ tp }}
        {%- if not loop.last -%}, {% endif %}
    {%- endfor -%}
{% endmacro %}
