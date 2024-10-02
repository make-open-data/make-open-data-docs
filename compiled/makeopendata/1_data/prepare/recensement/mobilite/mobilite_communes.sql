--- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.




with aggregated as (
  





with poids_par_geo as (
    SELECT
      code_commune_insee,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '1') AS nombre_de_menage_base_ou_logements_occupee,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '2') AS nombre_de_logements_occasionnels,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '3') AS nombre_de_logements_residences_secondaires,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '4') AS nombre_de_logements_vacants,
      SUM(poids_du_logement) AS nombre_de_logements_total_tous_status_occupation
    FROM
      "defaultdb"."intermediaires"."mobilite_renomee"
    GROUP BY
      code_commune_insee
  ), 
  poids_par_geo_clean as (
    SELECT  
      code_commune_insee,
      CAST(COALESCE(nombre_de_menage_base_ou_logements_occupee, 0) AS INT) as nombre_de_menage_base_ou_logements_occupee,
      CAST(COALESCE(nombre_de_logements_occasionnels, 0) AS INT) as nombre_de_logements_occasionnels,
      CAST(COALESCE(nombre_de_logements_residences_secondaires, 0) AS INT) as nombre_de_logements_residences_secondaires,
      CAST(COALESCE(nombre_de_logements_vacants, 0) AS INT) as nombre_de_logements_vacants
    FROM
      poids_par_geo

  ),
  aggregated as (

    SELECT * 

    FROM poids_par_geo_clean

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('IMMIM' as TEXT) as "champs",
      cast(  
           "IMMIM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_immigration_immigre'  , 
        
            'pr_immigration_non_immigre' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'pr_immigration_immigre'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_immigration_immigre"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_immigration_non_immigre'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_immigration_non_immigre"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_IMMIM_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('ILETUDM' as TEXT) as "champs",
      cast(  
           "ILETUDM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_etudes_commune_actuelle'  , 
        
            'pr_etudes_commune_departement_actuel'  , 
        
            'pr_etudes_department_region_actuel'  , 
        
            'pr_etudes_hors_region_actuelle_metropole'  , 
        
            'pr_etudes_hors_region_actuelle_dom'  , 
        
            'pr_etudes_hors_region_actuelle_com'  , 
        
            'pr_etudes_etranger'  , 
        
            'pr_etudes_sans_objet_non_inscrit' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'pr_etudes_commune_actuelle'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_commune_actuelle"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_commune_departement_actuel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_commune_departement_actuel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_department_region_actuel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_department_region_actuel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_hors_region_actuelle_metropole'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_hors_region_actuelle_metropole"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_hors_region_actuelle_dom'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_hors_region_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_hors_region_actuelle_com'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_hors_region_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_etranger'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_etranger"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_etudes_sans_objet_non_inscrit'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_etudes_sans_objet_non_inscrit"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_ILETUDM_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('VOIT' as TEXT) as "champs",
      cast(  
           "VOIT"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_0_voiture'  , 
        
            'menages_1_voiture'  , 
        
            'menages_2_voitures'  , 
        
            'menages_3_et_plus_voitures' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_0_voiture'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_0_voiture"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_1_voiture'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_1_voiture"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_2_voitures'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_2_voitures"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_3_et_plus_voitures'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_3_et_plus_voitures"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_VOIT_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('ILTM' as TEXT) as "champs",
      cast(  
           "ILTM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_travail_commune_actuelle'  , 
        
            'pr_travail_commune_departement_actuel'  , 
        
            'pr_travail_department_region_actuel'  , 
        
            'pr_travail_hors_region_actuelle_metropole'  , 
        
            'pr_travail_hors_region_actuelle_dom'  , 
        
            'pr_travail_hors_region_actuelle_com'  , 
        
            'pr_travail_etranger'  , 
        
            'pr_travail_sans_objet_sans_emploi' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'pr_travail_commune_actuelle'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_commune_actuelle"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_commune_departement_actuel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_commune_departement_actuel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_department_region_actuel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_department_region_actuel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_hors_region_actuelle_metropole'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_hors_region_actuelle_metropole"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_hors_region_actuelle_dom'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_hors_region_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_hors_region_actuelle_com'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_hors_region_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_etranger'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_etranger"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_travail_sans_objet_sans_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_travail_sans_objet_sans_emploi"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_ILTM_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('TRANSM' as TEXT) as "champs",
      cast(  
           "TRANSM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_pr_transport_travail_pieds'  , 
        
            'menages_pr_transport_travail_velo'  , 
        
            'menages_pr_transport_travail_deux_roues'  , 
        
            'menages_pr_transport_travail_voiture'  , 
        
            'menages_pr_transport_travail_transport_commune' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_pr_transport_travail_pieds'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_pieds"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_transport_travail_velo'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_velo"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_transport_travail_deux_roues'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_transport_travail_voiture'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_voiture"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_transport_travail_transport_commune'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_transport_commune"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_TRANSM_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('DEROU' as TEXT) as "champs",
      cast(  
           "DEROU"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__aucun_deux_roues'  , 
        
            'dom__un_deux_roues'  , 
        
            'dom__deux_deux_roues'  , 
        
            'dom__trois_ou_plus_deux_roues' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'dom__aucun_deux_roues'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__aucun_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__un_deux_roues'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__un_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__deux_deux_roues'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__deux_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__trois_ou_plus_deux_roues'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__trois_ou_plus_deux_roues"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_DEROU_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('IRANM' as TEXT) as "champs",
      cast(  
           "IRANM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_avec_pr_annee_avant_meme_logement'  , 
        
            'menages_avec_pr_annee_avant_meme_commune'  , 
        
            'menages_avec_pr_annee_avant_meme_departement'  , 
        
            'menages_avec_pr_annee_avant_meme_region'  , 
        
            'menages_avec_pr_annee_avant_autre_region_metro'  , 
        
            'menages_avec_pr_annee_avant_autre_region_dom'  , 
        
            'menages_avec_pr_annee_avant_autre_region_com'  , 
        
            'menages_avec_pr_annee_avant_union_europeenne'  , 
        
            'menages_avec_pr_annee_avant_etranger' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_meme_logement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_logement"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_meme_commune'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_commune"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_meme_departement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_departement"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_meme_region'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_region"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_autre_region_metro'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_autre_region_metro"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_autre_region_dom'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_autre_region_dom"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_autre_region_com'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_autre_region_com"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_union_europeenne'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_union_europeenne"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_pr_annee_avant_etranger'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_etranger"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_IRANM_par_geo
      USING (code_commune_insee)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INAIM' as TEXT) as "champs",
      cast(  
           "INAIM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."mobilite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_commune_insee, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_naissance_department_actuelle'  , 
        
            'pr_naissance_department_region_actuelle'  , 
        
            'pr_naissance_hors_region_actuelle_metropole'  , 
        
            'pr_naissance_hors_region_actuelle_dom'  , 
        
            'pr_naissance_hors_region_actuelle_com'  , 
        
            'pr_naissance_etranger' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'pr_naissance_department_actuelle'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_naissance_department_actuelle"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_naissance_department_region_actuelle'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_naissance_department_region_actuelle"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_naissance_hors_region_actuelle_metropole'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_naissance_hors_region_actuelle_metropole"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_naissance_hors_region_actuelle_dom'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_naissance_hors_region_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_naissance_hors_region_actuelle_com'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_naissance_hors_region_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_naissance_etranger'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_naissance_etranger"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  ) as alias_INAIM_par_geo
      USING (code_commune_insee)

    

  )

select * from aggregated

),
aggregated_with_infos_communes as (
    SELECT
      *
    FROM
      aggregated
    LEFT JOIN
	    "defaultdb"."prepare"."infos_communes" as infos_communes
    ON
      aggregated.code_commune_insee = infos_communes.code_commune
  )

SELECT 
    *  
FROM
    aggregated_with_infos_communes