select
    event_id,
    event_nom as nom,
    event_type as type,
    -- FK catégorie
    MD5(CAST(segment AS STRING)) AS id_categorie,
    md5(cast(event_timestamp_utc as string)) as date_id,
    md5(cast(ville as string) || cast(lieu as string)) as location_id,

    1 as event_count
from {{ ref('silver_events') }}