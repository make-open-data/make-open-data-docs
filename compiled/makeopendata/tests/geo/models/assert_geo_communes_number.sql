--- Le nombre officiel de communes en France est de 34935 en 2024
--- https://www.collectivites-locales.gouv.fr/bulletin-dinformation-statistique-bis-de-la-dgcl



with source as (
    SELECT COUNT(DISTINCT code_commune_insee) as commune_count
    FROM "test_db"."public"."geo_communes"
)

select *
from source
where commune_count != 34935