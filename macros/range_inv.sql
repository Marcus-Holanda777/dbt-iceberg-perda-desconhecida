
{%- macro range_inv(column_first, column_last) -%}
DATE(kafi_dh_ocorrreal) > {{ column_first }} and DATE(kafi_dh_ocorrreal) <= {{ column_last }}
{%- endmacro -%}