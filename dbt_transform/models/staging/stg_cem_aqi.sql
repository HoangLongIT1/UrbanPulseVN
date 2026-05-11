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
        -- Station & City Normalization
        coalesce(trim(station_name), 'Unknown')    as station_name,
        case
            when station_name like '%Hà Nội%' or station_name like '%Hanoi%' then 'Hà Nội'
            when station_name like '%Hồ Chí Minh%' or station_name like '%HCM%' then 'Hồ Chí Minh'
            when station_name like '%Đà Nẵng%' then 'Đà Nẵng'
            when station_name like '%Hải Phòng%' then 'Hải Phòng'
            when station_name like '%Cần Thơ%' then 'Cần Thơ'
            else station_name
        end                                         as city_name,

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

        -- Robust Time Parsing (Handles "HH:MI DD/MM/YYYY")
        case
            when published_time ~ '\d{2}:\d{2} \d{2}/\d{2}/\d{4}'
                then to_timestamp(published_time, 'HH24:MI DD/MM/YYYY')
            else null
        end                                         as measured_at,

        -- Metadata
        cast(_crawled_at as timestamp)              as ingested_at

    from source
    where aqi_value is not null
)

select * from cleaned
