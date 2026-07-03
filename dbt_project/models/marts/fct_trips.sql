-- fct_trips: fact table — one row per taxi trip

with trips as (
    select * from {{ ref('int_trips_enriched') }}
),

with_key as (
    select
        {{ dbt_utils.generate_surrogate_key(['pickup_at', 'dropoff_at', 'pickup_location_id', 'dropoff_location_id', 'total_amount', 'passenger_count']) }}
                                            as trip_id,

        -- foreign keys
        trip_date                           as date_id,
        pickup_location_id,
        dropoff_location_id,

        -- timestamps
        pickup_at,
        dropoff_at,

        -- metrics
        trip_duration_minutes,
        trip_distance_miles,
        passenger_count,
        fare_amount,
        tip_amount,
        tip_pct,
        total_amount,
        fare_per_mile,

        -- dimensions
        pickup_hour,
        day_of_week,
        is_weekend,
        payment_type_id,

        -- dedup rank: keep one row per surrogate key
        row_number() over (
            partition by {{ dbt_utils.generate_surrogate_key(['pickup_at', 'dropoff_at', 'pickup_location_id', 'dropoff_location_id', 'total_amount', 'passenger_count']) }}
            order by pickup_at
        ) as rn

    from trips
),

final as (
    select
        trip_id, date_id, pickup_location_id, dropoff_location_id,
        pickup_at, dropoff_at, trip_duration_minutes, trip_distance_miles,
        passenger_count, fare_amount, tip_amount, tip_pct, total_amount,
        fare_per_mile, pickup_hour, day_of_week, is_weekend, payment_type_id
    from with_key
    where rn = 1
)

select * from final
