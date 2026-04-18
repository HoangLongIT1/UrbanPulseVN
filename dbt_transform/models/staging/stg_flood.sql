-- =============================================
-- stg_flood: Cleaned Open-Meteo flood/river data
-- =============================================
-- Source: Open-Meteo Flood API (12 river monitoring points)
-- Transforms: cast types, filter negative discharge

with source as (
    select * from {{ source('bronze', 'flood') }}
),

cleaned as (
    select
        -- Location
        coalesce(river_name, 'Unknown')                as river_name,
        cast(latitude as numeric(10, 6))               as latitude,
        cast(longitude as numeric(10, 6))              as longitude,

        -- Time
        cast(time as timestamp)                        as observed_at,

        -- Measurement (m³/s)
        cast(river_discharge as numeric(12, 2))        as discharge_m3s,

        -- Metadata
        cast(_ingested_at as timestamp)                as ingested_at

    from source
    where time is not null
      and cast(river_discharge as numeric(12, 2)) >= 0
)

select * from cleaned
