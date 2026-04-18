-- =============================================
-- stg_geo_features: Cleaned OpenStreetMap features
-- =============================================
-- Source: OSM Overpass API (hospitals, schools, industrial zones)
-- Transforms: filter unnamed nodes, standardize feature_type

with source as (
    select * from {{ source('bronze', 'geo_features') }}
),

cleaned as (
    select
        -- IDs
        cast(osm_id as bigint)                      as osm_id,

        -- Feature info
        coalesce(trim(name), 'Unnamed')             as feature_name,
        lower(trim(feature_type))                   as feature_type,

        -- Geo
        cast(latitude as numeric(10, 6))            as latitude,
        cast(longitude as numeric(10, 6))           as longitude,

        -- Raw tags (JSON string from OSM)
        coalesce(tags, '{}')                        as tags_json,

        -- Metadata
        cast(_ingested_at as timestamp)             as ingested_at

    from source
    where name is not null
      and trim(name) != ''
      and latitude is not null
)

select * from cleaned
