{% macro true_range(high,low,prev_close) -%}
greatest(
  coalesce({{ high }} - {{ low }}, 0),
  abs({{ high }} - coalesce({{ prev_close }}, {{ high }})),
  abs({{ low }} - coalesce({{ prev_close }}, {{ low }}))
)
{%- endmacro %}

{% macro atr(tr_col, n) -%}
  avg({{ tr_col }}) over (
    partition by asset_pair_symbol
    order by open_date
    rows between {{ n - 1 }} preceding and current row
  )
{%- endmacro %}
