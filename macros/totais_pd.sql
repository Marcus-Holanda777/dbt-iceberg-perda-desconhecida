{%- macro totais_pd(column_custo) -%}
    if(substr(kafi_tp_mov, 1, 1) = 'E', - kafi_qt_mov, kafi_qt_mov) * {{ column_custo }}
{%- endmacro -%}
