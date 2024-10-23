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






    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2014_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2014 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2015_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2015 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2016_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2016 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2017_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2017 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2018_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2018 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2019_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2019 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2020_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2020 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2021_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2021 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2022_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2022 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2023_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2023 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
     union 

    select *
    from (

WITH source_dvf AS (
    
        select * from "defaultdb"."sources"."dvf_2024_dev"
    
),
ventes_immobiliers_filtrees AS (
    
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
ventes_immobiliers_aggregees_au_bien AS (
    
    SELECT 
        id_mutation,
        SUM(CAST(surface_reelle_bati AS numeric)) AS total_surface,
        SUM(CAST(nombre_pieces_principales AS numeric)) AS total_pieces
    FROM 
        ventes_immobiliers_filtrees
    GROUP BY 
        id_mutation

),
bien_principal_de_la_vente AS (
    
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
            ventes_immobiliers_filtrees
    ) subquery
    WHERE
        rang = 1

) 
SELECT 
    bien_principal_de_la_vente.id_mutation,
    bien_principal_de_la_vente.valeur_fonciere,
    bien_principal_de_la_vente.longitude,
    bien_principal_de_la_vente.latitude,
    ventes_immobiliers_aggregees_au_bien.total_pieces,
    ventes_immobiliers_aggregees_au_bien.total_surface,
    bien_principal_de_la_vente.type_local,
    bien_principal_de_la_vente.code_postal,
    bien_principal_de_la_vente.code_commune,
    ST_SetSRID(ST_MakePoint(bien_principal_de_la_vente.longitude, bien_principal_de_la_vente.latitude), 4326) as geopoint,
    bien_principal_de_la_vente.valeur_fonciere / ventes_immobiliers_aggregees_au_bien.total_surface as prix_m2,
    infos_communes.nom_commune,
    infos_communes.code_arrondissement,
    infos_communes.code_departement,
    infos_communes.code_region,
    infos_communes.nom_arrondissement,
    infos_communes.nom_departement,
    infos_communes.nom_region,
    2024 as millesime
FROM 
    bien_principal_de_la_vente
JOIN 
    ventes_immobiliers_aggregees_au_bien ON ventes_immobiliers_aggregees_au_bien.id_mutation = bien_principal_de_la_vente.id_mutation
LEFT JOIN
    "defaultdb"."prepare"."infos_communes" as infos_communes on infos_communes.code_commune = bien_principal_de_la_vente.code_commune

)
    
