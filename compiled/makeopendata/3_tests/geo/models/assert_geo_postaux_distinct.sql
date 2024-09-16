--- Vérifie que les codes postaux du modèle sont distincts



with counts as (
    SELECT code_postal, COUNT(*) as num_cp
    FROM "defaultdb"."prepare"."infos_postes"
    GROUP BY code_postal
)

select *
from counts
where num_cp > 1