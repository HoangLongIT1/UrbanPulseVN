-- =============================================
-- fact_weather_observations: Weather fact table
-- =============================================
-- Grain: one row per city × observation time
-- Joins: dim_location, dim_time

{{ config(materialized='table') }}

select
    -- Keys
    cast(date_trunc('day', w.observed_at) as date)  as date_key,

    -- Location
    w.city_name,
    w.latitude,
    w.longitude,

    -- Time detail
    w.observed_at,
    w.observation_date,
    w.observation_hour,

    -- Measures
    w.temperature_celsius,
    w.humidity_percent,
    w.precipitation_mm,
    w.wind_speed_kmh,
    w.wind_direction_deg,
    w.uv_index,

    -- Derived
    w.heat_index,
    w.comfort_level,
    w.rain_intensity,

    -- Metadata
    w.ingested_at

from {{ ref('int_weather_hourly') }} w
