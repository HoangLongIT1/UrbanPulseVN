-- =============================================
-- dim_pollutant: Pollutant reference dimension
-- =============================================
-- Sourced from dbt seed: pollutant_standards.csv

select
    {{ dbt_utils.generate_surrogate_key(['pollutant_code']) }} as pollutant_sk,
    pollutant_code,
    pollutant_name,
    unit,
    who_guideline_24h,
    qcvn_standard_24h,
    who_guideline_annual,
    qcvn_standard_annual,
    aqi_breakpoint_good,
    aqi_breakpoint_moderate,
    aqi_breakpoint_unhealthy_sg,
    aqi_breakpoint_unhealthy,
    aqi_breakpoint_very_unhealthy,
    aqi_breakpoint_hazardous
from {{ ref('pollutant_standards') }}
