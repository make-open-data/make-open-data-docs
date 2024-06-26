--- Vérifie que les communes du modèle sont distinctes



with counts as (
    SELECT code_commune, COUNT(*) as num_com
    FROM "defaultdb"."prep"."geo_communes"
    GROUP BY code_commune
)

select *
from counts
where num_com > 1