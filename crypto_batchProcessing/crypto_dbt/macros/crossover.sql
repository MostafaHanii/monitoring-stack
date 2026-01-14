{% macro crossover(short, long) -%}
case
  when lag({{ short }}) over (partition by asset_pair_symbol order by open_date) <= lag({{ long }}) over (partition by asset_pair_symbol order by open_date)
       and {{ short }} > {{ long }} then 'golden_cross'
  when lag({{ short }}) over (partition by asset_pair_symbol order by open_date) >= lag({{ long }}) over (partition by asset_pair_symbol order by open_date)
       and {{ short }} < {{ long }} then 'death_cross'
  else null
end
{%- endmacro %}
