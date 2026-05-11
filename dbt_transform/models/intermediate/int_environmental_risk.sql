-- =============================================
-- int_environmental_risk: Combined risk score
-- =============================================
-- Cross-source risk: AQI + flood + fire hotspots
-- Produces a 0-100 composite risk score per day per province

with aqi_risk as (
    select
        measurement_date                            as risk_date,
        city_name,
        latitude,
        longitude,
        -- AQI risk component (0-40 points)
        case
            when cem_avg_aqi is not null then
                least(round(cast(cem_avg_aqi / 500.0 * 40 as numeric), 1), 40)
            when avg_pollutant_value is not null and pollutant_code = 'pm25' then
                least(round(cast(avg_pollutant_value / 250.0 * 40 as numeric), 1), 40)
            else 0
        end                                         as aqi_risk_score
    from {{ ref('int_daily_aqi_city') }}
),

-- Fix: Aggregate flood risk by city/province before joining
flood_risk_prep as (
    select
        f.observed_at,
        r.river_name,
        -- Map river monitoring point to nearby city
        r.monitoring_point                          as city_name,
        f.discharge_m3s
    from {{ ref('stg_flood') }} f
    join {{ ref('vietnam_rivers') }} r
        on f.river_name = r.river_name
        -- Since stg_flood doesn't have station ID, we match on name
),

flood_risk as (
    select
        date_trunc('day', observed_at)              as risk_date,
        city_name,
        -- Flood risk component (0-30 points)
        least(round(cast(
            avg(discharge_m3s) / 5000.0 * 30
        as numeric), 1), 30)                        as flood_risk_score,
        avg(discharge_m3s)                          as avg_discharge
    from flood_risk_prep
    group by 1, 2
),

fire_risk as (
    select
        detection_date                              as risk_date,
        -- Fire risk component (0-30 points)
        least(count(*) * 3, 30)                     as fire_risk_score,
        count(*)                                    as hotspot_count,
        avg(fire_radiative_power_mw)                as avg_frp
    from {{ ref('stg_fire_hotspot') }}
    where confidence_level in ('high', 'nominal')
    group by 1
)

select
    coalesce(a.risk_date, fl.risk_date, fi.risk_date) as risk_date,
    coalesce(a.city_name, fl.city_name, 'Regional') as province_name,
    a.latitude,
    a.longitude,

    -- Component scores
    coalesce(a.aqi_risk_score, 0)                   as aqi_risk_score,
    coalesce(fl.flood_risk_score, 0)                as flood_risk_score,
    coalesce(fi.fire_risk_score, 0)                 as fire_risk_score,

    -- Composite score (0-100)
    round(
        coalesce(a.aqi_risk_score, 0)
        + coalesce(fl.flood_risk_score, 0)
        + coalesce(fi.fire_risk_score, 0)
    , 1)                                            as composite_risk_score,

    -- Risk level
    case
        when (coalesce(a.aqi_risk_score, 0) + coalesce(fl.flood_risk_score, 0) + coalesce(fi.fire_risk_score, 0)) >= 70 then 'critical'
        when (coalesce(a.aqi_risk_score, 0) + coalesce(fl.flood_risk_score, 0) + coalesce(fi.fire_risk_score, 0)) >= 50 then 'high'
        when (coalesce(a.aqi_risk_score, 0) + coalesce(fl.flood_risk_score, 0) + coalesce(fi.fire_risk_score, 0)) >= 25 then 'moderate'
        else 'low'
    end                                             as risk_level,

    fl.avg_discharge,
    fi.hotspot_count

from aqi_risk a
full outer join flood_risk fl
    on a.risk_date = fl.risk_date
    and a.city_name = fl.city_name
full outer join fire_risk fi
    on coalesce(a.risk_date, fl.risk_date) = fi.risk_date
