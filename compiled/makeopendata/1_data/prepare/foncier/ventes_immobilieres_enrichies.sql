-- Enrichie la base dvf groupée par mutation
-- Ajoute le knn des prix au m2



WITH dvf_knn_5 as(
    
WITH knn AS (
    SELECT 
        a.id_mutation AS id,
        AVG(b.prix_m2) AS prix_m2_knn_5
    FROM 
        "defaultdb"."intermediaires"."ventes_immobilieres" a
        JOIN LATERAL (
            SELECT prix_m2
            FROM "defaultdb"."intermediaires"."ventes_immobilieres"
            WHERE (id_mutation != a.id_mutation) AND (millesime = a.millesime)
            ORDER BY a.geopoint <-> geopoint
            LIMIT 5
        ) b ON TRUE
    GROUP BY a.id_mutation
)

SELECT * FROM knn



)

select ventes_immobilieres.*, 
       dvf_knn_5.prix_m2_knn_5 
from "defaultdb"."intermediaires"."ventes_immobilieres" ventes_immobilieres
left join dvf_knn_5 on dvf_knn_5.id = ventes_immobilieres.id_mutation