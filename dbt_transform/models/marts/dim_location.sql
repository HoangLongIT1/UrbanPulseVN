-- =============================================
-- dim_location: 63 Vietnamese provinces + monitoring points
-- =============================================
-- Sourced from dbt seed data + geo features

with cities as (
    select
        city_code                                   as location_code,
        city_name                                   as location_name,
        latitude,
        longitude,
        region,
        'province'                                  as location_type
    from {{ ref('vietnam_cities') }}
),

rivers as (
    select
        river_id                                    as location_code,
        river_name || ' (' || monitoring_point || ')' as location_name,
        latitude,
        longitude,
        basin                                       as region,
        'river_station'                             as location_type
    from {{ ref('vietnam_rivers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['location_code']) }} as location_sk,
    location_code,
    location_name,
    latitude,
    longitude,
    region,
    location_type
from cities

union all

select
    {{ dbt_utils.generate_surrogate_key(['location_code']) }} as location_sk,
    location_code,
    location_name,
    latitude,
    longitude,
    region,
    location_type
from rivers
