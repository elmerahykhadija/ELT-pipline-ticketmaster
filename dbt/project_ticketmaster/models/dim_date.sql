select
    md5(cast(event_timestamp_utc as string)) as date_id,
    event_date,
    event_time,
    event_timestamp_utc
from {{ ref('silver_events') }}
group by 1, 2, 3, 4