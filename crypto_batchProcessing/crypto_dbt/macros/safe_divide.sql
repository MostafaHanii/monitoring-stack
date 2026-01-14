{% macro safe_divide(num, den) -%}
case when {{ den }} is null or {{ den }} = 0 then null else {{ num }} / {{ den }} end
{%- endmacro %}
