-- fct_trips: fact table — one row per taxi trip

with trips as (
    select * from {{ ref('int_trips_enriched') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['pickup_at', 'dropoff_at', 'pickup_location_id', 'total_amount']) }}
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
        payment_type_id

    from trips
)

select * from final
