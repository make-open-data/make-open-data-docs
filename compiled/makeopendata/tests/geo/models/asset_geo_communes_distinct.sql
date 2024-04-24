--- Vérifie que les communes du modèle sont distinctes



with counts as (
    SELECT code_commune_insee, COUNT(*) as num_com
    FROM "test_db"."public"."geo_communes"
    GROUP BY code_commune_insee
)

select *
from counts
where num_com > 1