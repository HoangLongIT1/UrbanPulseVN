-- =============================================
-- Macro: calculate_risk_score
-- =============================================
-- Calculates composite environmental risk (0-100)
-- Components: AQI (40pts) + Flood (30pts) + Fire (30pts)
--
-- Usage: {{ calculate_risk_score('aqi_value', 'discharge_m3s', 'fire_count') }}

{% macro calculate_risk_score(aqi_column, flood_column, fire_column) %}

least(
    round(cast(
        -- AQI component (0-40): scale AQI 0-500 → 0-40
        least(coalesce({{ aqi_column }}, 0) / 500.0 * 40, 40)
        -- Flood component (0-30): scale discharge 0-5000 m³/s → 0-30
        + least(coalesce({{ flood_column }}, 0) / 5000.0 * 30, 30)
        -- Fire component (0-30): each hotspot = 3 points, max 30
        + least(coalesce({{ fire_column }}, 0) * 3, 30)
    as numeric), 1),
    100
)

{% endmacro %}
