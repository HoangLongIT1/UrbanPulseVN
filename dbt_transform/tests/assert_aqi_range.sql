-- =============================================
-- Custom test: AQI values must be between 0-500
-- =============================================
-- Fails if any row has AQI outside valid EPA range.
-- Usage in schema.yml:
--   tests:
--     - assert_aqi_range

{% test assert_aqi_range(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 500)

{% endtest %}
