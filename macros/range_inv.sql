{%- macro range_inv(column_first, column_last) -%}
    date(kafi_dh_ocorrreal) > {{ column_first }}
    and date(kafi_dh_ocorrreal) <= {{ column_last }}
{%- endmacro -%}
