--- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.












with communes as (
    SELECT 
      "COMMUNE" as code_commune_insee,
      CAST( SUM(CAST("IPONDL" AS numeric)) AS INT) AS nombre_de_logements
    FROM 
      "defaultdb"."sources"."logement_2020"
    GROUP BY
      code_commune_insee
  ),
  aggregated as (

    SELECT * 

    FROM communes

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('IMMIM' as TEXT) as "champs",
      cast(  
           "IMMIM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'IMMIM_1'  , 
        
            'IMMIM_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'IMMIM_1' then 'pr_immigration_immigre'
            
                when 'IMMIM_2' then 'pr_immigration_non_immigre'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_immigration_immigre'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_immigration_immigre"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_immigration_non_immigre'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_immigration_non_immigre"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('ILETUDM' as TEXT) as "champs",
      cast(  
           "ILETUDM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'ILETUDM_1'  , 
        
            'ILETUDM_2'  , 
        
            'ILETUDM_3'  , 
        
            'ILETUDM_4'  , 
        
            'ILETUDM_5'  , 
        
            'ILETUDM_6'  , 
        
            'ILETUDM_7'  , 
        
            'ILETUDM_Z' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'ILETUDM_1' then 'pr_etudes_commune_actuelle'
            
                when 'ILETUDM_2' then 'pr_etudes_commune_departement_actuel'
            
                when 'ILETUDM_3' then 'pr_etudes_department_region_actuel'
            
                when 'ILETUDM_4' then 'pr_etudes_hors_region_actuelle_metropole'
            
                when 'ILETUDM_5' then 'pr_etudes_hors_region_actuelle_dom'
            
                when 'ILETUDM_6' then 'pr_etudes_hors_region_actuelle_com'
            
                when 'ILETUDM_7' then 'pr_etudes_etranger'
            
                when 'ILETUDM_Z' then 'pr_etudes_sans_objet_non_inscrit'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_commune_actuelle'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_commune_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_commune_departement_actuel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_commune_departement_actuel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_department_region_actuel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_department_region_actuel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_hors_region_actuelle_metropole'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_hors_region_actuelle_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_hors_region_actuelle_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_hors_region_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_hors_region_actuelle_com'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_hors_region_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_etranger'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_etranger"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_etudes_sans_objet_non_inscrit'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_etudes_sans_objet_non_inscrit"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('VOIT' as TEXT) as "champs",
      cast(  
           "VOIT"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'VOIT_0'  , 
        
            'VOIT_1'  , 
        
            'VOIT_2'  , 
        
            'VOIT_3' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'VOIT_0' then 'menages_0_voiture'
            
                when 'VOIT_1' then 'menages_1_voiture'
            
                when 'VOIT_2' then 'menages_2_voitures'
            
                when 'VOIT_3' then 'menages_3_et_plus_voitures'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_0_voiture'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_0_voiture"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_1_voiture'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_1_voiture"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_2_voitures'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_2_voitures"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_3_et_plus_voitures'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_3_et_plus_voitures"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('ILTM' as TEXT) as "champs",
      cast(  
           "ILTM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'ILTM_1'  , 
        
            'ILTM_2'  , 
        
            'ILTM_3'  , 
        
            'ILTM_4'  , 
        
            'ILTM_5'  , 
        
            'ILTM_6'  , 
        
            'ILTM_7'  , 
        
            'ILTM_Z' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'ILTM_1' then 'pr_travail_commune_actuelle'
            
                when 'ILTM_2' then 'pr_travail_commune_departement_actuel'
            
                when 'ILTM_3' then 'pr_travail_department_region_actuel'
            
                when 'ILTM_4' then 'pr_travail_hors_region_actuelle_metropole'
            
                when 'ILTM_5' then 'pr_travail_hors_region_actuelle_dom'
            
                when 'ILTM_6' then 'pr_travail_hors_region_actuelle_com'
            
                when 'ILTM_7' then 'pr_travail_etranger'
            
                when 'ILTM_Z' then 'pr_travail_sans_objet_sans_emploi'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_commune_actuelle'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_commune_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_commune_departement_actuel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_commune_departement_actuel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_department_region_actuel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_department_region_actuel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_hors_region_actuelle_metropole'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_hors_region_actuelle_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_hors_region_actuelle_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_hors_region_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_hors_region_actuelle_com'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_hors_region_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_etranger'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_etranger"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_travail_sans_objet_sans_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_travail_sans_objet_sans_emploi"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('TRANSM' as TEXT) as "champs",
      cast(  
           "TRANSM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'TRANSM_2'  , 
        
            'TRANSM_3'  , 
        
            'TRANSM_4'  , 
        
            'TRANSM_5'  , 
        
            'TRANSM_6' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'TRANSM_2' then 'menages_pr_transport_travail_pieds'
            
                when 'TRANSM_3' then 'menages_pr_transport_travail_velo'
            
                when 'TRANSM_4' then 'menages_pr_transport_travail_deux_roues'
            
                when 'TRANSM_5' then 'menages_pr_transport_travail_voiture'
            
                when 'TRANSM_6' then 'menages_pr_transport_travail_transport_commune'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_transport_travail_pieds'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_pieds"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_transport_travail_velo'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_velo"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_transport_travail_deux_roues'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_transport_travail_voiture'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_voiture"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_transport_travail_transport_commune'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_transport_travail_transport_commune"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('DEROU' as TEXT) as "champs",
      cast(  
           "DEROU"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'DEROU_0'  , 
        
            'DEROU_1'  , 
        
            'DEROU_2'  , 
        
            'DEROU_3' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'DEROU_0' then 'dom__aucun_deux_roues'
            
                when 'DEROU_1' then 'dom__un_deux_roues'
            
                when 'DEROU_2' then 'dom__deux_deux_roues'
            
                when 'DEROU_3' then 'dom__trois_ou_plus_deux_roues'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__aucun_deux_roues'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__aucun_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__un_deux_roues'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__un_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__deux_deux_roues'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__deux_deux_roues"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__trois_ou_plus_deux_roues'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__trois_ou_plus_deux_roues"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('IRANM' as TEXT) as "champs",
      cast(  
           "IRANM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'IRANM_1'  , 
        
            'IRANM_2'  , 
        
            'IRANM_3'  , 
        
            'IRANM_4'  , 
        
            'IRANM_5'  , 
        
            'IRANM_6'  , 
        
            'IRANM_7'  , 
        
            'IRANM_8'  , 
        
            'IRANM_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'IRANM_1' then 'menages_avec_pr_annee_avant_meme_logement'
            
                when 'IRANM_2' then 'menages_avec_pr_annee_avant_meme_commune'
            
                when 'IRANM_3' then 'menages_avec_pr_annee_avant_meme_departement'
            
                when 'IRANM_4' then 'menages_avec_pr_annee_avant_meme_region'
            
                when 'IRANM_5' then 'menages_avec_pr_annee_avant_autre_region_metro'
            
                when 'IRANM_6' then 'menages_avec_pr_annee_avant_autre_region_dom'
            
                when 'IRANM_7' then 'menages_avec_pr_annee_avant_autre_region_com'
            
                when 'IRANM_8' then 'menages_avec_pr_annee_avant_union_europeenne'
            
                when 'IRANM_9' then 'menages_avec_pr_annee_avant_etranger'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_meme_logement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_logement"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_meme_commune'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_commune"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_meme_departement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_departement"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_meme_region'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_meme_region"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_autre_region_metro'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_autre_region_metro"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_autre_region_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_autre_region_dom"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_autre_region_com'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_autre_region_com"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_union_europeenne'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_union_europeenne"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_pr_annee_avant_etranger'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_pr_annee_avant_etranger"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('INAIM' as TEXT) as "champs",
      cast(  
           "INAIM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'INAIM_1'  , 
        
            'INAIM_2'  , 
        
            'INAIM_3'  , 
        
            'INAIM_4'  , 
        
            'INAIM_5'  , 
        
            'INAIM_6' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INAIM_1' then 'pr_naissance_department_actuelle'
            
                when 'INAIM_2' then 'pr_naissance_department_region_actuelle'
            
                when 'INAIM_3' then 'pr_naissance_hors_region_actuelle_metropole'
            
                when 'INAIM_4' then 'pr_naissance_hors_region_actuelle_dom'
            
                when 'INAIM_5' then 'pr_naissance_hors_region_actuelle_com'
            
                when 'INAIM_6' then 'pr_naissance_etranger'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_naissance_department_actuelle'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_naissance_department_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_naissance_department_region_actuelle'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_naissance_department_region_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_naissance_hors_region_actuelle_metropole'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_naissance_hors_region_actuelle_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_naissance_hors_region_actuelle_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_naissance_hors_region_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_naissance_hors_region_actuelle_com'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_naissance_hors_region_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_naissance_etranger'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_naissance_etranger"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

  ),
  aggregated_with_infos_communes as (
    SELECT
      *
    FROM
      aggregated
    JOIN
	    "defaultdb"."prepare"."infos_communes" as infos_communes
    ON
      aggregated.code_commune_insee = infos_communes.code_commune
  )

SELECT 
    *  
FROM
    aggregated_with_infos_communes