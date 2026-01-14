{% macro sma(col, n) -%}
  avg({{ col }}) over (
    partition by asset_pair_symbol
    order by open_date
    rows between {{ n - 1 }} preceding and current row
  )
{%- endmacro %}
