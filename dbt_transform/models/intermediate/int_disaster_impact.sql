-- =============================================
-- int_disaster_impact: Disaster impact analysis
-- =============================================
-- Groups warnings by region, type, severity
-- Calculates active alert counts and duration

with disasters as (
    select * from {{ ref('stg_nchmf_disaster') }}
),

daily_impact as (
    select
        date_trunc('day', cast(published_at as timestamp)) as event_date,
        disaster_type,
        severity_level,
        count(*)                                    as alert_count,
        min(cast(published_at as timestamp))        as first_alert_at,
        max(cast(published_at as timestamp))        as last_alert_at,

        -- Severity score (for aggregation)
        sum(
            case severity_level
                when 'critical' then 4
                when 'warning'  then 3
                when 'watch'    then 2
                when 'info'     then 1
                else 0
            end
        )                                           as total_severity_score

    from disasters
    group by 1, 2, 3
)

select
    event_date,
    disaster_type,
    severity_level,
    alert_count,
    first_alert_at,
    last_alert_at,
    total_severity_score,

    -- Impact level (based on count + severity)
    case
        when total_severity_score >= 12 then 'extreme'
        when total_severity_score >= 8  then 'high'
        when total_severity_score >= 4  then 'moderate'
        else 'low'
    end                                             as impact_level

from daily_impact
