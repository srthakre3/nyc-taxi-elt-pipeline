-- int_trips_enriched: add derived business metrics to cleaned trips

with trips as (
    select * from {{ ref('stg_trips') }}
),

enriched as (
    select
        *,

        -- derived time fields
        date_trunc('day', pickup_at)::date              as trip_date,
        extract(hour from pickup_at)::integer           as pickup_hour,
        extract(dow from pickup_at)::integer            as day_of_week,   -- 0=Sun, 6=Sat
        case
            when extract(dow from pickup_at) in (0, 6) then true
            else false
        end                                             as is_weekend,

        -- derived trip metrics
        round(
            extract(epoch from (dropoff_at - pickup_at)) / 60.0, 2
        )                                               as trip_duration_minutes,

        -- fare per mile (guard against zero distance)
        case
            when trip_distance_miles > 0
            then round(total_amount / trip_distance_miles, 2)
            else null
        end                                             as fare_per_mile,

        -- tip percentage
        case
            when fare_amount > 0
            then round((tip_amount / fare_amount) * 100, 2)
            else 0
        end                                             as tip_pct

    from trips
    where
        -- filter out unreasonable durations (< 1 min or > 4 hours)
        extract(epoch from (dropoff_at - pickup_at)) between 60 and 14400
)

select * from enriched
