-- =============================================
-- dim_station: Air quality monitoring stations
-- =============================================
-- Extracted from stg_air_quality distinct stations

with stations as (
    select distinct
        station_id,
        station_name,
        city,
        country_code,
        latitude,
        longitude
    from {{ ref('stg_air_quality') }}
    where station_id is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['station_id']) }} as station_sk,
    station_id,
    station_name,
    city,
    country_code,
    latitude,
    longitude
from stations
