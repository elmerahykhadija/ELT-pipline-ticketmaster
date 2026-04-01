select
    md5(cast(ville as string) || cast(lieu as string)) as location_id,
    ville,
    lieu
from {{ ref('silver_events') }} -- On utilise le silver pour avoir les données propres
group by ville, lieu, location_id