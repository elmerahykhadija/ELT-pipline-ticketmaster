-- dim_categories.sql

SELECT
    MD5(CAST(segment AS STRING)) AS id_categorie,
    segment AS nom_categorie

FROM {{ ref('silver_events') }}

WHERE segment IS NOT NULL

GROUP BY 1, 2