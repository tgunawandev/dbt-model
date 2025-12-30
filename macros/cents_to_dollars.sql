{% macro cents_to_dollars(column_name, precision=2) %}
    round(cast({{ column_name }} / 100.0 as numeric), {{ precision }})
{% endmacro %}
