-- =============================================
-- dim_disaster_type: Disaster classification dimension
-- =============================================
-- Reference table for natural disaster types in Vietnam

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['disaster_code']) }} as disaster_type_sk,
    disaster_code,
    disaster_name_en,
    disaster_name_vi,
    category
from (values
    ('storm',        'Tropical Storm/Typhoon', 'Bão',             'meteorological'),
    ('flood',        'Flood',                  'Lũ lụt',          'hydrological'),
    ('landslide',    'Landslide',              'Sạt lở đất',      'geophysical'),
    ('earthquake',   'Earthquake',             'Động đất',        'geophysical'),
    ('drought',      'Drought',                'Hạn hán',         'climatological'),
    ('high_tide',    'High Tide/Storm Surge',  'Triều cường',     'hydrological'),
    ('cold_wave',    'Cold Wave',              'Rét đậm/rét hại', 'meteorological'),
    ('heatwave',     'Heatwave',               'Nắng nóng',       'meteorological'),
    ('thunderstorm', 'Thunderstorm',           'Giông bão',       'meteorological'),
    ('other',        'Other',                  'Khác',             'other')
) as t(disaster_code, disaster_name_en, disaster_name_vi, category)
