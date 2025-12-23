-- macros/data_quality.sql

-- Test: Check if value is positive
{% test positive_value(model, column_name) %}

select
    {{ column_name }} as invalid_value
from {{ model }}
where {{ column_name }} < 0

{% endtest %}

-- Test: Check if address is valid ethereum format
{% test valid_address(model, column_name) %}

select
    {{ column_name }} as invalid_address
from {{ model }}
where length({{ column_name }}) != 42
   or substring({{ column_name }}, 1, 2) != '0x'

{% endtest %}

-- Test: Check if data is recent (not stale)
{% test recent_data(model, column_name, days=1) %}

select count(*) as row_count
from {{ model }}
having max({{ column_name }}) < now() - interval {{ days }} day

{% endtest %}

-- Test: Check for duplicate records
{% test no_duplicates(model, columns) %}

select
    {{ columns | join(', ') }},
    count(*) as cnt
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
