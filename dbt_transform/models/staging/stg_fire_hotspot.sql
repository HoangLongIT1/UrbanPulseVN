-- =============================================
-- stg_fire_hotspot: Cleaned NASA FIRMS fire data
-- =============================================
-- Source: NASA FIRMS API (VIIRS satellite)
-- Transforms: cast types, filter low confidence, combine date+time

with source as (
    select * from {{ source('bronze', 'fire_hotspot') }}
),

cleaned as (
    select
        -- Geo
        cast(latitude as numeric(10, 6))            as latitude,
        cast(longitude as numeric(10, 6))           as longitude,

        -- Fire metrics (VIIRS uses bright_ti4 instead of brightness)
        cast(bright_ti4 as numeric(8, 2))           as brightness_kelvin,
        cast(scan as numeric(4, 2))                 as scan_km,
        cast(track as numeric(4, 2))                as track_km,
        cast(frp as numeric(10, 2))                 as fire_radiative_power_mw,

        -- Satellite info
        coalesce(satellite, 'Unknown')              as satellite,
        coalesce(instrument, 'Unknown')             as instrument,

        -- Confidence
        confidence                                  as confidence_level,

        -- Day/Night
        coalesce(daynight, 'D')                     as daynight,

        -- Time (combine acq_date + acq_time)
        cast(acq_date as date)                      as detection_date,
        lpad(cast(acq_time as text), 4, '0')        as detection_time_hhmm,

        -- Metadata
        cast(_extracted_at as timestamp)            as ingested_at

    from source
    where latitude is not null
      and longitude is not null
)

select * from cleaned
