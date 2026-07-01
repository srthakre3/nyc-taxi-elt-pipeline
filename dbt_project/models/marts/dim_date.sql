-- dim_date: date dimension covering trip data range

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart="day",
            start_date="cast('2024-01-01' as date)",
            end_date="cast('2025-01-01' as date)"
        )
    }}
),

final as (
    select
        date_day                                            as date_id,
        date_day                                            as date,
        extract(year from date_day)::integer                as year,
        extract(month from date_day)::integer               as month,
        extract(day from date_day)::integer                 as day,
        to_char(date_day, 'Month')                          as month_name,
        extract(dow from date_day)::integer                 as day_of_week,
        to_char(date_day, 'Day')                            as day_name,
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                                 as is_weekend,
        extract(quarter from date_day)::integer             as quarter,
        extract(week from date_day)::integer                as week_of_year

    from date_spine
)

select * from final
