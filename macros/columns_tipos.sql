
{% macro columns_tipos(column_value, flag = true) %}
    {%- set tipos = ["rx", "salao", "psico_termo", "entrada_direta", "kafi_tp_mov", "marca_propria"] -%}
    sum({{ column_value }}) as {{ column_value }},
    {% for tp in tipos -%}
    {%- if tp == 'kafi_tp_mov'-%}
       {%- if not flag -%}{% continue %}{%- endif -%}
    sum({{ column_value }}) filter(where {{ tp }} in('EO', 'SO')) as {{ column_value }}_grt
    {%- else -%}
    sum({{ column_value }}) filter(where {{ tp }} = 1) as {{ column_value }}_{{ tp }}
    {%- endif -%}
    {%- if not loop.last -%}
        ,
    {% endif %}
    {%- endfor -%}
{% endmacro %}