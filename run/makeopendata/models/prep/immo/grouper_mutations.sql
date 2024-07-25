
  
    

  create  table "defaultdb"."prep"."grouper_mutations__dbt_tmp"
  
  
    as
  
  (
    -- Filtres nature transaction et nature bien :
-- Il convient aussi de garder que les vente (explure les VEFA et les échanges)
-- Et que les transactions qui concernent au moins un appartement et les maisons

-- Filtres sur les surfaces et trautement des pièces :
-- Il convient de garder que les transactions qui concernent des biens de plus de 9m2 
-- Le nombre de pièces est souvent mal renseigné, il convient de le corriger en fonction de la surface

-- Filtres sur les prix :
-- Il convient de garder que les transactions dont le prix au metre carré n'est pas 50% de plus que ses 10 plus proches voisins

-- Données par mutation : 
-- Les données DVF sont initilement présentées sous forme d'une ligne par mutation (transaction)
-- Une mutation peut concerner plusieurs biens
-- Le prix est le prix total de la mutation, il apparait sur les biens concernés



WITH source_dvf AS (
    select * from "defaultdb"."sources"."dvf_2023" as dvf_2023
),
filtrer_dvf AS (
    
    SELECT 
        id_mutation,
        CAST(valeur_fonciere AS FLOAT) as valeur_fonciere,
        CAST(longitude AS FLOAT) as longitude,
        CAST(latitude AS FLOAT) as latitude,
        CAST(nombre_pieces_principales AS NUMERIC) as nombre_pieces_principales,
        CAST(surface_reelle_bati AS NUMERIC) as surface_reelle_bati,
        type_local,
        LPAD(CAST(code_postal AS TEXT), 5, '0') as code_postal,
        code_commune
    FROM 
        source_dvf 
     WHERE 
        EXISTS (
            SELECT 1
            FROM source_dvf d1
            WHERE d1.id_mutation = source_dvf.id_mutation AND d1.type_local IN ('Appartement', 'Maison')
        ) AND
        NOT EXISTS (
            SELECT 1
            FROM source_dvf d2
            WHERE d2.id_mutation = source_dvf.id_mutation AND d2.nature_mutation != 'Vente'
        )

),
aggreger_dvf AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        filtrer_dvf
    GROUP BY 
        id_mutation

),
bien_principal_dvf AS (
    
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id_mutation
                ORDER BY 
                    CASE WHEN type_local = 'Maison' THEN 1
                         WHEN type_local = 'Appartement' THEN 2
                         ELSE 3
                    END,
                    surface_reelle_bati DESC
            ) AS rang
        FROM 
            filtrer_dvf
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_dvf.id_mutation,
    bien_principal_dvf.valeur_fonciere,
    bien_principal_dvf.longitude,
    bien_principal_dvf.latitude,
    aggreger_dvf.total_pieces,
    aggreger_dvf.total_surface,
    bien_principal_dvf.type_local,
    bien_principal_dvf.code_postal,
    bien_principal_dvf.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_dvf.latitude, bien_principal_dvf.longitude), 4326) as geopoint,
    bien_principal_dvf.valeur_fonciere / aggreger_dvf.total_surface as prix_m2
FROM 
    bien_principal_dvf
JOIN 
    aggreger_dvf ON aggreger_dvf.id_mutation = bien_principal_dvf.id_mutation
LEFT JOIN
    "defaultdb"."prep"."geo_communes" as cog_communes on cog_communes.code_commune = bien_principal_dvf.code_commune
  );
  