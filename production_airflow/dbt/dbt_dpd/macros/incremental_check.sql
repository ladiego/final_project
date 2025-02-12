{% macro check_if_incremental() %}
    {{ return(is_incremental()) }}
{% endmacro %}  