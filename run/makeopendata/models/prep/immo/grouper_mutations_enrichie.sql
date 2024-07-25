
  
    

  create  table "defaultdb"."prep"."grouper_mutations_enrichie__dbt_tmp"
  
  
    as
  
  (
    -- Enrichie la base dvf groupée par mutation
-- Ajoute le knn des prix au m2



WITH dvf_knn_5 as(
    
WITH knn AS (
    SELECT 
        a.id_mutation AS id,
        AVG(b.prix_m2) AS prix_m2_knn_5
    FROM 
        "defaultdb"."prep"."grouper_mutations" a
        JOIN LATERAL (
            SELECT prix_m2
            FROM "defaultdb"."prep"."grouper_mutations"
            WHERE (id_mutation != a.id_mutation)
            ORDER BY a.geopoint <-> geopoint
            LIMIT 5
        ) b ON TRUE
    GROUP BY a.id_mutation
)

SELECT * FROM knn



)

select grouper_mutations.*, 
       dvf_knn_5.prix_m2_knn_5 
from "defaultdb"."prep"."grouper_mutations" grouper_mutations
left join dvf_knn_5 on dvf_knn_5.id = grouper_mutations.id_mutation
  );
  