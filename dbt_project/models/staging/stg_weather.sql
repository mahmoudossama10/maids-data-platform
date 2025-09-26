select
    city,
    cast(date as date) as date,
    temp_max::float as temp_max,
    temp_min::float as temp_min,
    precipitation::float as precipitation,
    windspeed_max::float as windspeed_max,
    cast(updated_at as timestamp_ntz) as updated_at
from {{ source('raw','weather') }}
