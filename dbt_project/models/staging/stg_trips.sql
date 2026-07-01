-- stg_trips: clean and type-cast raw NYC taxi trip data

with source as (
    select * from {{ source('raw', 'yellow_trips') }}
),

cleaned as (
    select
        -- timestamps
        tpep_pickup_datetime                                    as pickup_at,
        tpep_dropoff_datetime                                   as dropoff_at,

        -- trip details
        cast(passenger_count as integer)                        as passenger_count,
        cast(trip_distance as numeric(10, 2))                   as trip_distance_miles,
        cast("PULocationID" as integer)                         as pickup_location_id,
        cast("DOLocationID" as integer)                         as dropoff_location_id,

        -- payment
        cast(payment_type as integer)                           as payment_type_id,
        cast(fare_amount as numeric(10, 2))                     as fare_amount,
        cast(tip_amount as numeric(10, 2))                      as tip_amount,
        cast(total_amount as numeric(10, 2))                    as total_amount

    from source
    where
        tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and tpep_dropoff_datetime > tpep_pickup_datetime
        and trip_distance > 0
        and total_amount > 0
)

select * from cleaned
