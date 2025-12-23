-- macros/calculate_risk_level.sql
{% macro calculate_risk_level(health_factor_column) %}
    case
        when {{ health_factor_column }} >= {{ var('health_factor_safe', 2.0) }} then 'SAFE'
        when {{ health_factor_column }} >= {{ var('health_factor_warning', 1.5) }} then 'WARNING'
        when {{ health_factor_column }} >= {{ var('health_factor_danger', 1.2) }} then 'DANGER'
        when {{ health_factor_column }} >= {{ var('health_factor_critical', 1.0) }} then 'CRITICAL'
        else 'LIQUIDATABLE'
    end
{% endmacro %}

-- Calculate position size tier based on collateral
{% macro calculate_position_size_tier(collateral_usd_column) %}
    case
        when {{ collateral_usd_column }} >= {{ var('whale_collateral_usd', 1000000) }} then 'whale'
        when {{ collateral_usd_column }} >= 100000 then 'large'
        when {{ collateral_usd_column }} >= 10000 then 'medium'
        else 'small'
    end
{% endmacro %}
