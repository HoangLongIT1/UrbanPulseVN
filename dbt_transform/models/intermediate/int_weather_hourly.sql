-- =============================================
-- int_weather_hourly: Hourly weather with heat index
-- =============================================
-- Source: stg_weather
-- Adds: heat index, comfort level classification

with weather as (
    select * from {{ ref('stg_weather') }}
),

with_heat_index as (
    select
        city_name,
        latitude,
        longitude,
        observed_at,
        date_trunc('day', observed_at)              as observation_date,
        extract(hour from observed_at)              as observation_hour,
        temperature_celsius,
        humidity_percent,
        precipitation_mm,
        wind_speed_kmh,
        wind_direction_deg,
        uv_index,

        -- Heat Index (simplified Steadman formula, valid for T > 27°C)
        case
            when temperature_celsius > 27 then
                round(cast(
                    -8.785 + 1.611 * temperature_celsius
                    + 2.339 * humidity_percent
                    - 0.1461 * temperature_celsius * humidity_percent
                as numeric), 1)
            else temperature_celsius
        end                                         as heat_index,

        -- Comfort level
        case
            when temperature_celsius < 15 then 'cold'
            when temperature_celsius < 25 then 'comfortable'
            when temperature_celsius < 33 then 'warm'
            when temperature_celsius < 40 then 'hot'
            else 'extreme_heat'
        end                                         as comfort_level,

        -- Rain intensity
        case
            when precipitation_mm = 0 then 'none'
            when precipitation_mm < 2.5 then 'light'
            when precipitation_mm < 7.6 then 'moderate'
            when precipitation_mm < 50 then 'heavy'
            else 'extreme'
        end                                         as rain_intensity,

        ingested_at

    from weather
)

select * from with_heat_index
