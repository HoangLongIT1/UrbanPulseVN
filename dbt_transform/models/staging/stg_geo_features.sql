-- =============================================
-- stg_geo_features: Cleaned OpenStreetMap features
-- =============================================
-- Source: OSM Overpass API (hospitals, fire stations)
-- Transforms: filter unnamed nodes, standardize amenity_type

with source as (
    select * from {{ source('bronze', 'geo_features') }}
),

cleaned as (
    select
        -- IDs
        cast(node_id as bigint)                     as osm_id,

        -- Feature info
        coalesce(trim(name), 'Unnamed')             as feature_name,
        lower(trim(amenity_type))                   as feature_type,

        -- Geo
        cast(latitude as numeric(10, 6))            as latitude,
        cast(longitude as numeric(10, 6))           as longitude,

        -- Metadata
        cast(_extracted_at as timestamp)            as ingested_at

    from source
    where name is not null
      and trim(name) != ''
      and latitude is not null
)

select * from cleaned
