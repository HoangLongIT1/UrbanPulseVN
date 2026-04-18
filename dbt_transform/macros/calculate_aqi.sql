-- =============================================
-- Macro: calculate_aqi
-- =============================================
-- Calculates AQI sub-index from raw pollutant concentration
-- using EPA linear interpolation formula.
--
-- Usage: {{ calculate_aqi('pm25', 'concentration_column') }}

{% macro calculate_aqi(pollutant_code, value_column) %}

case
    {% if pollutant_code == 'pm25' %}
    -- PM2.5 breakpoints (µg/m³) → AQI
    when {{ value_column }} <= 12.0  then round(cast({{ value_column }} / 12.0 * 50 as numeric))
    when {{ value_column }} <= 35.4  then round(cast(50 + ({{ value_column }} - 12.1) / (35.4 - 12.1) * 50 as numeric))
    when {{ value_column }} <= 55.4  then round(cast(100 + ({{ value_column }} - 35.5) / (55.4 - 35.5) * 50 as numeric))
    when {{ value_column }} <= 150.4 then round(cast(150 + ({{ value_column }} - 55.5) / (150.4 - 55.5) * 50 as numeric))
    when {{ value_column }} <= 250.4 then round(cast(200 + ({{ value_column }} - 150.5) / (250.4 - 150.5) * 100 as numeric))
    when {{ value_column }} <= 500.4 then round(cast(300 + ({{ value_column }} - 250.5) / (500.4 - 250.5) * 200 as numeric))
    else 500

    {% elif pollutant_code == 'pm10' %}
    -- PM10 breakpoints (µg/m³) → AQI
    when {{ value_column }} <= 54   then round(cast({{ value_column }} / 54.0 * 50 as numeric))
    when {{ value_column }} <= 154  then round(cast(50 + ({{ value_column }} - 55) / (154.0 - 55) * 50 as numeric))
    when {{ value_column }} <= 254  then round(cast(100 + ({{ value_column }} - 155) / (254.0 - 155) * 50 as numeric))
    when {{ value_column }} <= 354  then round(cast(150 + ({{ value_column }} - 255) / (354.0 - 255) * 50 as numeric))
    when {{ value_column }} <= 424  then round(cast(200 + ({{ value_column }} - 355) / (424.0 - 355) * 100 as numeric))
    when {{ value_column }} <= 604  then round(cast(300 + ({{ value_column }} - 425) / (604.0 - 425) * 200 as numeric))
    else 500

    {% elif pollutant_code == 'o3' %}
    -- O3 breakpoints (ppb for 8-hr avg) → AQI
    when {{ value_column }} <= 54  then round(cast({{ value_column }} / 54.0 * 50 as numeric))
    when {{ value_column }} <= 70  then round(cast(50 + ({{ value_column }} - 55) / (70.0 - 55) * 50 as numeric))
    when {{ value_column }} <= 85  then round(cast(100 + ({{ value_column }} - 71) / (85.0 - 71) * 50 as numeric))
    when {{ value_column }} <= 105 then round(cast(150 + ({{ value_column }} - 86) / (105.0 - 86) * 50 as numeric))
    when {{ value_column }} <= 200 then round(cast(200 + ({{ value_column }} - 106) / (200.0 - 106) * 100 as numeric))
    else 500

    {% else %}
    -- Generic fallback: return raw value capped at 500
    when {{ value_column }} is not null then least(cast({{ value_column }} as integer), 500)

    {% endif %}
end

{% endmacro %}
