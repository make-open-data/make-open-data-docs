
  
    

  create  table "defaultdb"."prepare"."demographie_communes__dbt_tmp"
  
  
    as
  
  (
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

      cast('INP15M' as TEXT) as "champs",
      cast(  
           "INP15M"
             
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
        
            'INP15M_0'  , 
        
            'INP15M_1'  , 
        
            'INP15M_10'  , 
        
            'INP15M_11'  , 
        
            'INP15M_12'  , 
        
            'INP15M_13'  , 
        
            'INP15M_14'  , 
        
            'INP15M_15'  , 
        
            'INP15M_2'  , 
        
            'INP15M_3'  , 
        
            'INP15M_4'  , 
        
            'INP15M_5'  , 
        
            'INP15M_6'  , 
        
            'INP15M_7'  , 
        
            'INP15M_8'  , 
        
            'INP15M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP15M_0' then 'menages_avec_0_personne_15_ans_et_moins'
            
                when 'INP15M_1' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_10' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_11' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_12' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_13' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_14' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_15' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_2' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_3' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_4' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_5' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_6' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_7' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_8' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
                when 'INP15M_9' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_15_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_15_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_15_ans_et_moins"
      
    
    
  

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

      cast('INPER1' as TEXT) as "champs",
      cast(  
           "INPER1"
             
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
        
            'INPER1_0'  , 
        
            'INPER1_1'  , 
        
            'INPER1_10'  , 
        
            'INPER1_11'  , 
        
            'INPER1_12'  , 
        
            'INPER1_13'  , 
        
            'INPER1_14'  , 
        
            'INPER1_15'  , 
        
            'INPER1_16'  , 
        
            'INPER1_17'  , 
        
            'INPER1_18'  , 
        
            'INPER1_19'  , 
        
            'INPER1_2'  , 
        
            'INPER1_20'  , 
        
            'INPER1_22'  , 
        
            'INPER1_26'  , 
        
            'INPER1_3'  , 
        
            'INPER1_38'  , 
        
            'INPER1_4'  , 
        
            'INPER1_5'  , 
        
            'INPER1_6'  , 
        
            'INPER1_7'  , 
        
            'INPER1_8'  , 
        
            'INPER1_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INPER1_0' then 'menages_avec_0_personne_masculin'
            
                when 'INPER1_1' then 'menages_avec_1_personne_masculin'
            
                when 'INPER1_10' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_11' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_12' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_13' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_14' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_15' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_16' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_17' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_18' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_19' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_2' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_20' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_22' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_26' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_3' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_38' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_4' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_5' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_6' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_7' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_8' then 'menages_avec_2_et_plus_personnes_masculin'
            
                when 'INPER1_9' then 'menages_avec_2_et_plus_personnes_masculin'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_masculin'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_masculin"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_personne_masculin'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_masculin"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_2_et_plus_personnes_masculin'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_masculin"
      
    
    
  

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

      cast('SEXEM' as TEXT) as "champs",
      cast(  
           "SEXEM"
             
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
        
            'SEXEM_1'  , 
        
            'SEXEM_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'SEXEM_1' then 'menages_pr_homme'
            
                when 'SEXEM_2' then 'menages_pr_femmes'
            
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
      when champs_valeur_renomme = 'menages_pr_homme'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_homme"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_femmes'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_femmes"
      
    
    
  

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

      cast('INPER2' as TEXT) as "champs",
      cast(  
           "INPER2"
             
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
        
            'INPER2_0'  , 
        
            'INPER2_1'  , 
        
            'INPER2_10'  , 
        
            'INPER2_11'  , 
        
            'INPER2_12'  , 
        
            'INPER2_13'  , 
        
            'INPER2_14'  , 
        
            'INPER2_15'  , 
        
            'INPER2_16'  , 
        
            'INPER2_17'  , 
        
            'INPER2_19'  , 
        
            'INPER2_2'  , 
        
            'INPER2_20'  , 
        
            'INPER2_3'  , 
        
            'INPER2_39'  , 
        
            'INPER2_4'  , 
        
            'INPER2_5'  , 
        
            'INPER2_6'  , 
        
            'INPER2_7'  , 
        
            'INPER2_8'  , 
        
            'INPER2_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INPER2_0' then 'menages_avec_0_personne_feminin'
            
                when 'INPER2_1' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_10' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_11' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_12' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_13' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_14' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_15' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_16' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_17' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_19' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_2' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_20' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_3' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_39' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_4' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_5' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_6' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_7' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_8' then 'menages_avec_1_personne_feminin'
            
                when 'INPER2_9' then 'menages_avec_1_personne_feminin'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_feminin'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_feminin"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_personne_feminin'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_feminin"
      
    
    
  

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

      cast('INP3M' as TEXT) as "champs",
      cast(  
           "INP3M"
             
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
        
            'INP3M_0'  , 
        
            'INP3M_1'  , 
        
            'INP3M_2'  , 
        
            'INP3M_3'  , 
        
            'INP3M_4'  , 
        
            'INP3M_5'  , 
        
            'INP3M_6'  , 
        
            'INP3M_7'  , 
        
            'INP3M_8' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP3M_0' then 'menages_avec_0_personne_3_ans_et_moins'
            
                when 'INP3M_1' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_2' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_3' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_4' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_5' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_6' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_7' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
                when 'INP3M_8' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_3_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_3_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_3_ans_et_moins"
      
    
    
  

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

      cast('INP65M' as TEXT) as "champs",
      cast(  
           "INP65M"
             
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
        
            'INP65M_0'  , 
        
            'INP65M_1'  , 
        
            'INP65M_11'  , 
        
            'INP65M_12'  , 
        
            'INP65M_16'  , 
        
            'INP65M_18'  , 
        
            'INP65M_2'  , 
        
            'INP65M_3'  , 
        
            'INP65M_4'  , 
        
            'INP65M_5'  , 
        
            'INP65M_55'  , 
        
            'INP65M_6'  , 
        
            'INP65M_7'  , 
        
            'INP65M_8'  , 
        
            'INP65M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP65M_0' then 'menages_avec_0_personne_65_ans_et_plus'
            
                when 'INP65M_1' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_11' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_12' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_16' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_18' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_2' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_3' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_4' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_5' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_55' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_6' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_7' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_8' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
                when 'INP65M_9' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_65_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_65_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_65_ans_et_plus"
      
    
    
  

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

      cast('INP11M' as TEXT) as "champs",
      cast(  
           "INP11M"
             
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
        
            'INP11M_0'  , 
        
            'INP11M_1'  , 
        
            'INP11M_10'  , 
        
            'INP11M_11'  , 
        
            'INP11M_12'  , 
        
            'INP11M_13'  , 
        
            'INP11M_14'  , 
        
            'INP11M_2'  , 
        
            'INP11M_3'  , 
        
            'INP11M_4'  , 
        
            'INP11M_5'  , 
        
            'INP11M_6'  , 
        
            'INP11M_7'  , 
        
            'INP11M_8'  , 
        
            'INP11M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP11M_0' then 'menages_avec_0_personne_11_ans_et_moins'
            
                when 'INP11M_1' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_10' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_11' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_12' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_13' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_14' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_2' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_3' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_4' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_5' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_6' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_7' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_8' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
                when 'INP11M_9' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_11_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_11_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_11_ans_et_moins"
      
    
    
  

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

      cast('INPER' as TEXT) as "champs",
      cast(  
           "INPER"
             
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
        
            'INPER_1'  , 
        
            'INPER_10'  , 
        
            'INPER_11'  , 
        
            'INPER_12'  , 
        
            'INPER_13'  , 
        
            'INPER_14'  , 
        
            'INPER_15'  , 
        
            'INPER_16'  , 
        
            'INPER_17'  , 
        
            'INPER_18'  , 
        
            'INPER_19'  , 
        
            'INPER_2'  , 
        
            'INPER_20'  , 
        
            'INPER_21'  , 
        
            'INPER_22'  , 
        
            'INPER_23'  , 
        
            'INPER_24'  , 
        
            'INPER_25'  , 
        
            'INPER_26'  , 
        
            'INPER_28'  , 
        
            'INPER_3'  , 
        
            'INPER_34'  , 
        
            'INPER_4'  , 
        
            'INPER_41'  , 
        
            'INPER_5'  , 
        
            'INPER_55'  , 
        
            'INPER_6'  , 
        
            'INPER_7'  , 
        
            'INPER_8'  , 
        
            'INPER_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INPER_1' then 'menages_avec_1_personne'
            
                when 'INPER_10' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_11' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_12' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_13' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_14' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_15' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_16' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_17' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_18' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_19' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_2' then 'menages_avec_2_personnes'
            
                when 'INPER_20' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_21' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_22' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_23' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_24' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_25' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_26' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_28' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_3' then 'menages_avec_3_personnes'
            
                when 'INPER_34' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_4' then 'menages_avec_4_personnes'
            
                when 'INPER_41' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_5' then 'menages_avec_5_personnes'
            
                when 'INPER_55' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_6' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_7' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_8' then 'menages_avec_6_et_plus_personnes'
            
                when 'INPER_9' then 'menages_avec_6_et_plus_personnes'
            
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
      when champs_valeur_renomme = 'menages_avec_1_personne'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_personne"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_6_et_plus_personnes'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_6_et_plus_personnes"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_2_personnes'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_2_personnes"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_3_personnes'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_3_personnes"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_4_personnes'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_4_personnes"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_5_personnes'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_5_personnes"
      
    
    
  

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

      cast('INP24M' as TEXT) as "champs",
      cast(  
           "INP24M"
             
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
        
            'INP24M_0'  , 
        
            'INP24M_1'  , 
        
            'INP24M_10'  , 
        
            'INP24M_11'  , 
        
            'INP24M_12'  , 
        
            'INP24M_13'  , 
        
            'INP24M_14'  , 
        
            'INP24M_15'  , 
        
            'INP24M_16'  , 
        
            'INP24M_17'  , 
        
            'INP24M_18'  , 
        
            'INP24M_19'  , 
        
            'INP24M_2'  , 
        
            'INP24M_20'  , 
        
            'INP24M_21'  , 
        
            'INP24M_25'  , 
        
            'INP24M_3'  , 
        
            'INP24M_4'  , 
        
            'INP24M_5'  , 
        
            'INP24M_6'  , 
        
            'INP24M_7'  , 
        
            'INP24M_8'  , 
        
            'INP24M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP24M_0' then 'menages_avec_0_personne_24_ans_et_moins'
            
                when 'INP24M_1' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_10' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_11' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_12' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_13' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_14' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_15' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_16' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_17' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_18' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_19' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_2' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_20' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_21' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_25' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_3' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_4' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_5' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_6' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_7' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_8' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
                when 'INP24M_9' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_24_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_24_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_24_ans_et_moins"
      
    
    
  

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

      cast('INP60M' as TEXT) as "champs",
      cast(  
           "INP60M"
             
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
        
            'INP60M_0'  , 
        
            'INP60M_1'  , 
        
            'INP60M_10'  , 
        
            'INP60M_11'  , 
        
            'INP60M_12'  , 
        
            'INP60M_17'  , 
        
            'INP60M_19'  , 
        
            'INP60M_2'  , 
        
            'INP60M_3'  , 
        
            'INP60M_4'  , 
        
            'INP60M_5'  , 
        
            'INP60M_55'  , 
        
            'INP60M_6'  , 
        
            'INP60M_7'  , 
        
            'INP60M_8' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP60M_0' then 'menages_avec_0_personne_60_ans_et_plus'
            
                when 'INP60M_1' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_10' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_11' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_12' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_17' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_19' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_2' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_3' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_4' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_5' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_55' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_6' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_7' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
                when 'INP60M_8' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_60_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_60_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_60_ans_et_plus"
      
    
    
  

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

      cast('INP19M' as TEXT) as "champs",
      cast(  
           "INP19M"
             
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
        
            'INP19M_0'  , 
        
            'INP19M_1'  , 
        
            'INP19M_10'  , 
        
            'INP19M_11'  , 
        
            'INP19M_12'  , 
        
            'INP19M_13'  , 
        
            'INP19M_14'  , 
        
            'INP19M_15'  , 
        
            'INP19M_16'  , 
        
            'INP19M_17'  , 
        
            'INP19M_18'  , 
        
            'INP19M_19'  , 
        
            'INP19M_2'  , 
        
            'INP19M_20'  , 
        
            'INP19M_21'  , 
        
            'INP19M_22'  , 
        
            'INP19M_23'  , 
        
            'INP19M_25'  , 
        
            'INP19M_3'  , 
        
            'INP19M_4'  , 
        
            'INP19M_41'  , 
        
            'INP19M_5'  , 
        
            'INP19M_55'  , 
        
            'INP19M_6'  , 
        
            'INP19M_7'  , 
        
            'INP19M_8'  , 
        
            'INP19M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP19M_0' then 'menages_avec_0_personne_19_ans_et_moins'
            
                when 'INP19M_1' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_10' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_11' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_12' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_13' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_14' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_15' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_16' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_17' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_18' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_19' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_2' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_20' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_21' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_22' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_23' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_25' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_3' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_4' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_41' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_5' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_55' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_6' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_7' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_8' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
                when 'INP19M_9' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_19_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_19_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_19_ans_et_moins"
      
    
    
  

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

      cast('INP75M' as TEXT) as "champs",
      cast(  
           "INP75M"
             
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
        
            'INP75M_0'  , 
        
            'INP75M_1'  , 
        
            'INP75M_11'  , 
        
            'INP75M_18'  , 
        
            'INP75M_2'  , 
        
            'INP75M_3'  , 
        
            'INP75M_4'  , 
        
            'INP75M_48'  , 
        
            'INP75M_5'  , 
        
            'INP75M_6'  , 
        
            'INP75M_7'  , 
        
            'INP75M_8'  , 
        
            'INP75M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP75M_0' then 'menages_avec_0_personne_75_ans_et_plus'
            
                when 'INP75M_1' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_11' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_18' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_2' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_3' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_4' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_48' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_5' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_6' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_7' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_8' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
                when 'INP75M_9' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_75_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_75_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_75_ans_et_plus"
      
    
    
  

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

      cast('INP17M' as TEXT) as "champs",
      cast(  
           "INP17M"
             
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
        
            'INP17M_0'  , 
        
            'INP17M_1'  , 
        
            'INP17M_10'  , 
        
            'INP17M_11'  , 
        
            'INP17M_12'  , 
        
            'INP17M_13'  , 
        
            'INP17M_14'  , 
        
            'INP17M_15'  , 
        
            'INP17M_16'  , 
        
            'INP17M_17'  , 
        
            'INP17M_2'  , 
        
            'INP17M_24'  , 
        
            'INP17M_3'  , 
        
            'INP17M_4'  , 
        
            'INP17M_5'  , 
        
            'INP17M_6'  , 
        
            'INP17M_7'  , 
        
            'INP17M_8'  , 
        
            'INP17M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP17M_0' then 'menages_avec_0_personne_17_ans_et_moins'
            
                when 'INP17M_1' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_10' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_11' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_12' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_13' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_14' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_15' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_16' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_17' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_2' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_24' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_3' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_4' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_5' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_6' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_7' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_8' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
                when 'INP17M_9' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_17_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_17_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_17_ans_et_moins"
      
    
    
  

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

      cast('STAT_CONJM' as TEXT) as "champs",
      cast(  
           "STAT_CONJM"
             
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
        
            'STAT_CONJM_1'  , 
        
            'STAT_CONJM_2'  , 
        
            'STAT_CONJM_3'  , 
        
            'STAT_CONJM_4'  , 
        
            'STAT_CONJM_5'  , 
        
            'STAT_CONJM_6' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'STAT_CONJM_1' then 'menages_pr_mariee'
            
                when 'STAT_CONJM_2' then 'menages_pr_pacsee'
            
                when 'STAT_CONJM_3' then 'menages_pr_concubinage_union_libre'
            
                when 'STAT_CONJM_4' then 'menages_pr_veuve'
            
                when 'STAT_CONJM_5' then 'menages_pr_divorcee'
            
                when 'STAT_CONJM_6' then 'menages_pr_celibataire'
            
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
      when champs_valeur_renomme = 'menages_pr_mariee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_mariee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_pacsee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_pacsee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_concubinage_union_libre'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_concubinage_union_libre"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_veuve'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_veuve"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_divorcee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_divorcee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_celibataire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_celibataire"
      
    
    
  

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

      cast('INP5M' as TEXT) as "champs",
      cast(  
           "INP5M"
             
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
        
            'INP5M_0'  , 
        
            'INP5M_1'  , 
        
            'INP5M_10'  , 
        
            'INP5M_2'  , 
        
            'INP5M_3'  , 
        
            'INP5M_4'  , 
        
            'INP5M_5'  , 
        
            'INP5M_6'  , 
        
            'INP5M_7'  , 
        
            'INP5M_8'  , 
        
            'INP5M_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INP5M_0' then 'menages_avec_0_personne_5_ans_et_moins'
            
                when 'INP5M_1' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_10' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_2' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_3' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_4' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_5' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_6' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_7' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_8' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
                when 'INP5M_9' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
            
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
      when champs_valeur_renomme = 'menages_avec_0_personne_5_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_5_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_5_ans_et_moins"
      
    
    
  

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
  );
  