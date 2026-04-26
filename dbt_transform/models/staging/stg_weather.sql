-- =============================================
-- stg_weather: Cleaned Open-Meteo weather data
-- =============================================
-- Source: Open-Meteo Weather API (63 provinces)
-- Transforms: cast types, filter outliers, COALESCE nulls

with source as (
    select * from {{ source('bronze', 'weather') }}
),

cleaned as (
    select
        -- Location
        coalesce(city, 'Unknown')                   as city_name,
        cast(latitude as numeric(10, 6))            as latitude,
        cast(longitude as numeric(10, 6))           as longitude,

        -- Time
        cast(time as timestamp)                     as observed_at,

        -- Measurements
        cast(temperature_2m as numeric(6, 2))       as temperature_celsius,
        cast(relative_humidity_2m as numeric(5, 2))  as humidity_percent,
        coalesce(cast(precipitation as numeric(8, 2)), 0) as precipitation_mm,
        cast(wind_speed_10m as numeric(6, 2))       as wind_speed_kmh,
        cast(wind_direction_10m as numeric(5, 1))   as wind_direction_deg,
        coalesce(cast(uv_index as numeric(4, 1)), 0) as uv_index,
        cast(surface_pressure as numeric(8, 2))     as surface_pressure_hpa,

        -- Metadata
        cast(_extracted_at as timestamp)            as ingested_at

    from source
    where time is not null
      -- Filter extreme outliers
      and cast(temperature_2m as numeric(6, 2)) between -10 and 50
)

select * from cleaned
