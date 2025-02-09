{% macro filtra_periodo() %}
kafi_dh_ocorrreal > cast(current_date as timestamp) - interval '2' year - interval '0.001' second
{% endmacro %}