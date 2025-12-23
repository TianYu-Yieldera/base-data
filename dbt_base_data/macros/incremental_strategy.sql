-- macros/incremental_strategy.sql

-- ClickHouse-specific incremental strategy helpers

{% macro clickhouse_incremental_filter(timestamp_column, default_date='2024-01-01') %}
    {% if is_incremental() %}
    where {{ timestamp_column }} > (select coalesce(max({{ timestamp_column }}), '{{ default_date }}') from {{ this }})
    {% endif %}
{% endmacro %}

{% macro clickhouse_incremental_filter_by_date(date_column, default_date='2024-01-01') %}
    {% if is_incremental() %}
    where {{ date_column }} >= (select coalesce(max({{ date_column }}), toDate('{{ default_date }}')) from {{ this }})
    {% endif %}
{% endmacro %}

{% macro clickhouse_incremental_filter_by_block(block_column) %}
    {% if is_incremental() %}
    where {{ block_column }} > (select coalesce(max({{ block_column }}), 0) from {{ this }})
    {% endif %}
{% endmacro %}
