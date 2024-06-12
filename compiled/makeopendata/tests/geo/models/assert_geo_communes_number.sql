--- Le nombre officiel de communes est de 35029 soit
--- zone = metro & drom est de 34935 en 2024
--- i.e. france metropole et departement d'outre mer
--- https://www.collectivites-locales.gouv.fr/bulletin-dinformation-statistique-bis-de-la-dgcl
--- PLUS zone = com  est 94 
--- i.e. collectivité d'outre mer



with source as (
    SELECT COUNT(DISTINCT code_commune) as commune_count
    FROM "make_open_data"."prep"."geo_communes"
)

select *
from source
where commune_count != 35029