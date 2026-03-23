{#
    Cleans a free-text column by:
      1. lower()  - normalises case
      2. trim()   - strips leading/trailing whitespace
      3. nullif() - turns empty strings into NULL

    Used in every staging model to clean fields like name, email,
    city, cuisine_type, etc. that arrive dirty from the raw layer.

    Usage:
        select {{ standardize_text('email') }} as email from {{ ref('raw_customers') }}
#}
{% macro standardize_text(column_name) %}
    nullif(trim(lower({{ column_name }})), '')
{% endmacro %}