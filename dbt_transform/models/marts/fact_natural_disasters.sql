-- =============================================
-- fact_natural_disasters: Disaster events fact table
-- =============================================
-- Grain: one row per disaster event (warning)
-- Joins: dim_disaster_type, dim_time

{{ config(materialized='table') }}

with disasters as (
    select * from {{ ref('stg_nchmf_disaster') }}
),

disaster_types as (
    select * from {{ ref('dim_disaster_type') }}
)

select
    -- Keys
    {{ dbt_utils.generate_surrogate_key(['d.title', 'd.published_at']) }} as disaster_event_sk,
    dt.disaster_type_sk,
    cast(date_trunc('day', d.published_at) as date) as date_key,

    -- Event details
    d.title,
    d.summary,
    d.detail_url,
    d.disaster_type,
    d.severity_level,

    -- Enriched from dimension
    dt.disaster_name_en,
    dt.disaster_name_vi,
    dt.category                                     as hazard_category,

    -- Time
    d.published_at,
    d.ingested_at

from disasters d
left join disaster_types dt
    on d.disaster_type = dt.disaster_code
