-- =============================================
-- stg_nchmf_disaster: Cleaned NCHMF disaster warnings
-- =============================================
-- Source: Web crawler (nchmf.gov.vn)
-- Transforms: parse dates, classify disaster type from title

with source as (
    select * from {{ source('bronze', 'nchmf_disaster') }}
),

cleaned as (
    select
        -- Content
        coalesce(trim(title), '')                  as title,
        coalesce(trim(summary), '')                as summary,
        coalesce(trim(detail_url), '')             as detail_url,

        -- Disaster classification (inferred from title keywords)
        case
            when lower(title) like '%bão%'         then 'storm'
            when lower(title) like '%lũ%'          then 'flood'
            when lower(title) like '%lụt%'         then 'flood'
            when lower(title) like '%sạt lở%'      then 'landslide'
            when lower(title) like '%động đất%'    then 'earthquake'
            when lower(title) like '%hạn hán%'     then 'drought'
            when lower(title) like '%triều cường%' then 'high_tide'
            when lower(title) like '%rét%'         then 'cold_wave'
            when lower(title) like '%nắng nóng%'   then 'heatwave'
            when lower(title) like '%giông%'       then 'thunderstorm'
            else 'other'
        end                                         as disaster_type,

        -- Severity (inferred from title keywords)
        case
            when lower(title) like '%khẩn cấp%'   then 'critical'
            when lower(title) like '%cảnh báo%'    then 'warning'
            when lower(title) like '%dự báo%'      then 'watch'
            else 'info'
        end                                         as severity_level,

        -- Time
        publish_date                                as published_at,

        -- Metadata
        cast(_crawled_at as timestamp)              as ingested_at

    from source
    where title is not null
      and trim(title) != ''
)

select * from cleaned
