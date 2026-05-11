-- =============================================
-- int_daily_aqi_city: Daily AQI aggregated by city
-- =============================================
-- Combines OpenAQ station data + CEM official AQI
-- Calculates: avg, min, max AQI per city per day

with openaq_daily as (
    select
        city                                        as city_name,
        date_trunc('day', measured_at_utc)          as measurement_date,
        pollutant_code,
        avg(measurement_value)                      as avg_value,
        min(measurement_value)                      as min_value,
        max(measurement_value)                      as max_value,
        count(*)                                    as reading_count,
        -- Representative lat/lon for the city
        avg(latitude)                               as latitude,
        avg(longitude)                              as longitude
    from {{ ref('stg_air_quality') }}
    group by 1, 2, 3
),

cem_daily as (
    select
        city_name,
        date_trunc('day', measured_at)              as measurement_date,
        avg(aqi_value)                              as avg_aqi,
        min(aqi_value)                              as min_aqi,
        max(aqi_value)                              as max_aqi,
        count(*)                                    as reading_count,
        -- Most frequent category in the day
        mode() within group (order by aqi_category) as dominant_category
    from {{ ref('stg_cem_aqi') }}
    where measured_at is not null
    group by 1, 2
)

select
    coalesce(o.city_name, c.city_name)              as city_name,
    coalesce(o.measurement_date, c.measurement_date) as measurement_date,
    o.latitude,
    o.longitude,
    o.pollutant_code,
    o.avg_value                                     as avg_pollutant_value,
    o.min_value                                     as min_pollutant_value,
    o.max_value                                     as max_pollutant_value,
    o.reading_count                                 as openaq_readings,
    c.avg_aqi                                       as cem_avg_aqi,
    c.min_aqi                                       as cem_min_aqi,
    c.max_aqi                                       as cem_max_aqi,
    c.dominant_category                             as cem_dominant_category,
    c.reading_count                                 as cem_readings
from openaq_daily o
full outer join cem_daily c
    on o.city_name = c.city_name
    and o.measurement_date = c.measurement_date
