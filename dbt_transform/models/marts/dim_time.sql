-- =============================================
-- dim_time: Date dimension table
-- =============================================
-- Generates a date spine for time-series analysis

{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date=cast('2024-01-01' as date),
        end_date=cast('2027-01-01' as date)
    ) }}
),

enriched as (
    select
        cast(date_day as date)                      as date_key,
        extract(year from date_day)                 as year,
        extract(quarter from date_day)              as quarter,
        extract(month from date_day)                as month,
        extract(week from date_day)                 as week_of_year,
        extract(dow from date_day)                  as day_of_week,
        extract(day from date_day)                  as day_of_month,
        extract(doy from date_day)                  as day_of_year,

        -- Vietnamese month names
        case extract(month from date_day)
            when 1 then 'Tháng 1'
            when 2 then 'Tháng 2'
            when 3 then 'Tháng 3'
            when 4 then 'Tháng 4'
            when 5 then 'Tháng 5'
            when 6 then 'Tháng 6'
            when 7 then 'Tháng 7'
            when 8 then 'Tháng 8'
            when 9 then 'Tháng 9'
            when 10 then 'Tháng 10'
            when 11 then 'Tháng 11'
            when 12 then 'Tháng 12'
        end                                         as month_name_vi,

        -- Vietnamese seasons
        case
            when extract(month from date_day) in (3, 4, 5) then 'Xuân'
            when extract(month from date_day) in (6, 7, 8) then 'Hè'
            when extract(month from date_day) in (9, 10, 11) then 'Thu'
            else 'Đông'
        end                                         as season_vi,

        -- Is weekend
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend

    from date_spine
)

select * from enriched
