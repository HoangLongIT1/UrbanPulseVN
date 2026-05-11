-- =============================================
-- stg_nchmf_disaster: Cleaned NCHMF disaster data
-- =============================================
-- Source: Web crawler (nchmf.gov.vn)
-- Transforms: categorize disaster type, extract severity, parse date

with source as (
    select * from {{ source('bronze', 'nchmf_disaster') }}
),

cleaned as (
    select
        -- Raw info
        coalesce(trim(title), 'No Title')           as title,
        coalesce(trim(summary), 'No Summary')       as summary,
        coalesce(detail_url, 'N/A')                 as detail_url,

        -- Standardized disaster type (regex mapping)
        case
            when lower(title) like '%bão%' or lower(summary) like '%bão%' then 'storm'
            when lower(title) like '%lũ%' or lower(summary) like '%lũ%' then 'flood'
            when lower(title) like '%sạt lở%' or lower(summary) like '%sạt lở%' then 'landslide'
            when lower(title) like '%động đất%' or lower(summary) like '%động đất%' then 'earthquake'
            when lower(title) like '%hạn hán%' or lower(summary) like '%hạn hán%' then 'drought'
            when lower(title) like '%triều cường%' or lower(summary) like '%triều cường%' then 'high_tide'
            when lower(title) like '%rét%' then 'cold_wave'
            when lower(title) like '%nắng nóng%' then 'heatwave'
            when lower(title) like '%giông%' or lower(title) like '%lốc%' then 'thunderstorm'
            else 'other'
        end                                         as disaster_type,

        -- Extract severity indicator
        case
            when lower(title) like '%khẩn cấp%' or lower(title) like '%rất mạnh%' then 'extreme'
            when lower(title) like '%cảnh báo%' or lower(title) like '%mạnh%' then 'high'
            else 'normal'
        end                                         as severity_level,

        -- Robust Time Parsing
        case
            when publish_date ~ '\d{2}:\d{2} \d{2}/\d{2}/\d{4}'
                then to_timestamp(publish_date, 'HH24:MI DD/MM/YYYY')
            else null
        end                                         as published_at,

        -- Metadata
        cast(_crawled_at as timestamp)              as ingested_at

    from source
    where title is not null 
      and title != 'N/A'
)

select * from cleaned
