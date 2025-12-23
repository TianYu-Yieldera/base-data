-- macros/format_address.sql

-- Normalize address to lowercase
{% macro format_address(address_column) %}
    lower({{ address_column }})
{% endmacro %}

-- Check if address is zero address
{% macro is_zero_address(address_column) %}
    {{ address_column }} = '0x0000000000000000000000000000000000000000'
{% endmacro %}

-- Truncate address for display (e.g., 0x1234...abcd)
{% macro truncate_address(address_column, prefix_len=6, suffix_len=4) %}
    concat(
        substring({{ address_column }}, 1, {{ prefix_len }}),
        '...',
        substring({{ address_column }}, length({{ address_column }}) - {{ suffix_len }} + 1, {{ suffix_len }})
    )
{% endmacro %}

-- Validate ethereum address format
{% macro is_valid_address(address_column) %}
    length({{ address_column }}) = 42
    and substring({{ address_column }}, 1, 2) = '0x'
{% endmacro %}
