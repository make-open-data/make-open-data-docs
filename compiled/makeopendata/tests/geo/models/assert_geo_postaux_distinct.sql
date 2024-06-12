--- Vérifie que les codes postaux du modèle sont distincts



with counts as (
    SELECT code_postal, COUNT(*) as num_cp
    FROM "make_open_data"."prep"."geo_postaux"
    GROUP BY code_postal
)

select *
from counts
where num_cp > 1