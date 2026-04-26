-- =============================================
-- fact_air_measurements: Central fact table for air quality
-- =============================================
-- Grain: one row per station × pollutant × measurement time
-- FKs: dim_station, dim_location, dim_time, dim_pollutant

{{ config(materialized='table') }}

with measurements as (
    select
        station_id,
        station_name,
        city,
        latitude,
        longitude,
        pollutant_code,
        measurement_value,
        unit,
        measured_at_utc,
        ingested_at
    from {{ ref('stg_air_quality') }}
),

enriched as (
    select
        -- Surrogate keys for dimension joins
        {{ dbt_utils.generate_surrogate_key(['m.station_id']) }}       as station_sk,
        {{ dbt_utils.generate_surrogate_key(['m.pollutant_code']) }}   as pollutant_sk,
        cast(date_trunc('day', m.measured_at_utc) as date)            as date_key,

        -- Degenerate dimensions
        m.station_id,
        m.station_name,
        m.city,
        m.latitude,
        m.longitude,

        -- Measures
        m.pollutant_code,
        m.measurement_value,
        m.unit,

        -- AQI category (based on PM2.5 thresholds)
        case
            when m.pollutant_code = 'pm25' then
                case
                    when m.measurement_value <= 12   then 'good'
                    when m.measurement_value <= 35.4  then 'moderate'
                    when m.measurement_value <= 55.4  then 'unhealthy_sg'
                    when m.measurement_value <= 150.4 then 'unhealthy'
                    when m.measurement_value <= 250.4 then 'very_unhealthy'
                    else 'hazardous'
                end
            else null
        end                                         as pm25_aqi_category,

        -- Time
        m.measured_at_utc,
        m.ingested_at

    from measurements m
)

select * from enriched
