--- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.




with aggregated as (
  





with poids_par_geo as (
    SELECT 
      code_commune_insee,
      CAST( SUM(CAST(poids_du_logement AS numeric)) AS INT) AS nombre_de_logements
    FROM
      "defaultdb"."intermediaires"."demographie_renomee"
    GROUP BY
      code_commune_insee
  ),
  aggregated as (

    SELECT * 

    FROM poids_par_geo

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP15M' as TEXT) as "champs",
      cast(  
           "INP15M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_15_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_15_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_15_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_15_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INPER1' as TEXT) as "champs",
      cast(  
           "INPER1"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_masculin'  , 
        
            'menages_avec_1_personne_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin'  , 
        
            'menages_avec_2_et_plus_personnes_masculin' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_masculin'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_masculin"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_personne_masculin'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_masculin"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_2_et_plus_personnes_masculin'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_masculin"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('SEXEM' as TEXT) as "champs",
      cast(  
           "SEXEM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_pr_homme'  , 
        
            'menages_pr_femmes' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_pr_homme'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_homme"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_femmes'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_femmes"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INPER2' as TEXT) as "champs",
      cast(  
           "INPER2"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin'  , 
        
            'menages_avec_1_personne_feminin' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_feminin'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_feminin"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_personne_feminin'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_feminin"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP3M' as TEXT) as "champs",
      cast(  
           "INP3M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_3_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_3_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_3_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_3_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP65M' as TEXT) as "champs",
      cast(  
           "INP65M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_65_ans_et_plus' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_65_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_65_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_65_ans_et_plus"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP11M' as TEXT) as "champs",
      cast(  
           "INP11M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_11_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_11_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_11_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_11_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INPER' as TEXT) as "champs",
      cast(  
           "INPER"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_1_personne'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_2_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_3_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_4_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_5_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes'  , 
        
            'menages_avec_6_et_plus_personnes' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
            
        
      
        
      
        
            
        
      
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_1_personne'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_personne"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_6_et_plus_personnes'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_6_et_plus_personnes"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_2_personnes'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_2_personnes"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_3_personnes'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_3_personnes"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_4_personnes'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_4_personnes"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_5_personnes'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_5_personnes"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP24M' as TEXT) as "champs",
      cast(  
           "INP24M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_24_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_24_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_24_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_24_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP60M' as TEXT) as "champs",
      cast(  
           "INP60M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_60_ans_et_plus' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_60_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_60_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_60_ans_et_plus"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP19M' as TEXT) as "champs",
      cast(  
           "INP19M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_19_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_19_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_19_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_19_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP75M' as TEXT) as "champs",
      cast(  
           "INP75M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_personnes_75_ans_et_plus' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_75_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_75_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_75_ans_et_plus"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP17M' as TEXT) as "champs",
      cast(  
           "INP17M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_17_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_17_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_17_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_17_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('STAT_CONJM' as TEXT) as "champs",
      cast(  
           "STAT_CONJM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_pr_mariee'  , 
        
            'menages_pr_pacsee'  , 
        
            'menages_pr_concubinage_union_libre'  , 
        
            'menages_pr_veuve'  , 
        
            'menages_pr_divorcee'  , 
        
            'menages_pr_celibataire' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_pr_mariee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_mariee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_pacsee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_pacsee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_concubinage_union_libre'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_concubinage_union_libre"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_veuve'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_veuve"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_divorcee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_divorcee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_celibataire'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_celibataire"
      
    
    
  

    from 
        unpivot_filtree
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
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INP5M' as TEXT) as "champs",
      cast(  
           "INP5M"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."demographie_renomee"

    



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
        
            'menages_avec_0_personne_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins'  , 
        
            'menages_avec_1_et_plus_personnes_5_ans_et_moins' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_5_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_5_ans_et_moins"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_personnes_5_ans_et_moins"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

  )

select * from aggregated

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