-- =============================================
-- stg_air_quality: Cleaned OpenAQ air quality data
-- =============================================
-- Source: OpenAQ API (PM2.5, PM10, O3, NO2, SO2, CO)
-- Transforms: standardize column names, cast types,
--   convert timestamps to UTC, COALESCE nulls

with source as (
    select * from {{ source('bronze', 'air_quality') }}
),

cleaned as (
    select
        -- IDs
        cast(location_id as integer)          as station_id,
        coalesce(location_name, 'Unknown')    as station_name,
        coalesce(city, 'Unknown')             as city,
        coalesce(country, 'VN')               as country_code,

        -- Geo
        cast(latitude as numeric(10, 6))      as latitude,
        cast(longitude as numeric(10, 6))     as longitude,

        -- Measurement
        lower(trim(parameter))                as pollutant_code,
        cast(value as numeric(12, 4))         as measurement_value,
        coalesce(unit, 'µg/m³')               as unit,

        -- Time (normalize to UTC)
        cast(date_utc as timestamp)           as measured_at_utc,
        cast(date_local as timestamp)         as measured_at_local,

        -- Metadata
        cast(_ingested_at as timestamp)       as ingested_at

    from source
    where value is not null
      and cast(value as numeric(12, 4)) >= 0
)

select * from cleaned
