-- =============================================
-- stg_cem_aqi: Cleaned CEM.gov.vn AQI data
-- =============================================
-- Source: Web crawler (cem.gov.vn)
-- Transforms: cast AQI to integer, map category, parse timestamp

with source as (
    select * from {{ source('bronze', 'cem_aqi') }}
),

cleaned as (
    select
        -- Station
        coalesce(trim(station_name), 'Unknown')    as station_name,

        -- AQI value
        cast(aqi_value as integer)                 as aqi_value,

        -- Category mapping (Vietnamese → standardized code)
        case
            when lower(trim(category)) like '%tốt%'        then 'good'
            when lower(trim(category)) like '%trung bình%'  then 'moderate'
            when lower(trim(category)) like '%kém%'         then 'unhealthy_sg'
            when lower(trim(category)) like '%xấu%'         then 'unhealthy'
            when lower(trim(category)) like '%rất xấu%'     then 'very_unhealthy'
            when lower(trim(category)) like '%nguy hại%'    then 'hazardous'
            else 'unknown'
        end                                         as aqi_category,
        coalesce(trim(category), 'Unknown')         as aqi_category_vi,

        -- Time
        published_time                              as measured_at,

        -- Metadata
        cast(_crawled_at as timestamp)              as ingested_at

    from source
    where aqi_value is not null
)

select * from cleaned
