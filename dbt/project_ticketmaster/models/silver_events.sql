with evenement as (
    select * from TICKETMASTER_DB.RAW_DATA.EVENTS_RAW
),
cleaned as (
    select 
        id as event_id,
        nom as event_nom,
        type as event_type,
        coalesce(segment, 'Unknown') as segment, 
        coalesce(ville, 'Unknown') as ville,    
        coalesce(lieu, 'Unknown') as lieu,     
        date_locale::date as event_date,
        heure_locale::time as event_time,
        date_utc::timestamp_ntz as event_timestamp_utc,
        ingested_at                           
    from evenement
)
select *
from cleaned
qualify row_number() over (
    partition by event_id 
    order by ingested_at desc
) = 1