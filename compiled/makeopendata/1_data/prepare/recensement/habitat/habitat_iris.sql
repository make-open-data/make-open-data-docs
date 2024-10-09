--- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.




with aggregated as (
  





with poids_par_geo as (
    SELECT
      code_iris,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '1') AS nombre_de_menage_base_ou_logements_occupee,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '2') AS nombre_de_logements_occasionnels,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '3') AS nombre_de_logements_residences_secondaires,
      SUM(poids_du_logement) FILTER (WHERE "CATL" = '4') AS nombre_de_logements_vacants,
      SUM(poids_du_logement) AS nombre_de_logements_total_tous_status_occupation
    FROM
      "defaultdb"."intermediaires"."habitat_renomee"
    GROUP BY
      code_iris
  ), 
  poids_par_geo_clean as (
    SELECT  
      code_iris,
      CAST(COALESCE(nombre_de_menage_base_ou_logements_occupee, 0) AS INT) as nombre_de_menage_base_ou_logements_occupee,
      CAST(COALESCE(nombre_de_logements_occasionnels, 0) AS INT) as nombre_de_logements_occasionnels,
      CAST(COALESCE(nombre_de_logements_residences_secondaires, 0) AS INT) as nombre_de_logements_residences_secondaires,
      CAST(COALESCE(nombre_de_logements_vacants, 0) AS INT) as nombre_de_logements_vacants,
      CAST(COALESCE(nombre_de_logements_total_tous_status_occupation, 0) AS INT) as nombre_de_logements_total_tous_status_occupation
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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('AEMMR' as TEXT) as "champs",
      cast(  
           "AEMMR"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'emmenagement_1900_1919'  , 
        
            'emmenagement_1920_1939'  , 
        
            'emmenagement_1940_1959'  , 
        
            'emmenagement_1940_1969'  , 
        
            'emmenagement_1970_1979'  , 
        
            'emmenagement_1980_1989'  , 
        
            'emmenagement_1990_1999'  , 
        
            'emmenagement_apres_1999' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'emmenagement_1900_1919'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1900_1919"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_1920_1939'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1920_1939"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_1940_1959'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1940_1959"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_1940_1969'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1940_1969"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_1970_1979'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1970_1979"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_1980_1989'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1980_1989"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_1990_1999'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_1990_1999"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_apres_1999'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_apres_1999"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_AEMMR_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('ACHL' as TEXT) as "champs",
      cast(  
           "ACHL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'construction_avant_1919'  , 
        
            'construction_1919_1945'  , 
        
            'construction_1916_1970'  , 
        
            'construction_1971_1990'  , 
        
            'construction_1991_2025'  , 
        
            'construction_2006'  , 
        
            'construction_2007'  , 
        
            'construction_2008'  , 
        
            'construction_2009'  , 
        
            'construction_2010'  , 
        
            'construction_2011'  , 
        
            'construction_2012'  , 
        
            'construction_2013'  , 
        
            'construction_2014'  , 
        
            'construction_2015'  , 
        
            'construction_2016'  , 
        
            'construction_2017'  , 
        
            'construction_2018'  , 
        
            'construction_2019'  , 
        
            'construction_2020'  , 
        
            'construction_2021'  , 
        
            'construction_2022' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'construction_avant_1919'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_avant_1919"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_1919_1945'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_1919_1945"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_1916_1970'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_1916_1970"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_1971_1990'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_1971_1990"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_1991_2025'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_1991_2025"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2006'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2006"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2007'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2007"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2008'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2008"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2009'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2009"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2010'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2010"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2011'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2011"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2012'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2012"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2013'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2013"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2014'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2014"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2015'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2015"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2016'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2016"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2017'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2017"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2018'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2018"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2019'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2019"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2020'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2020"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2021'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2021"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'construction_2022'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "construction_2022"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_ACHL_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('STOCD' as TEXT) as "champs",
      cast(  
           "STOCD"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'logements_occupes_par_proprietaires'  , 
        
            'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'  , 
        
            'logements_occupes_par_locataire_sous_locataire_vide_hlm'  , 
        
            'logements_occupes_par_locataire_meuble_hotel'  , 
        
            'logements_occupes_par_loge_gratuitemenent' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'logements_occupes_par_proprietaires'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_occupes_par_proprietaires"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_occupes_par_locataire_sous_locataire_vide_non_hlm"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_occupes_par_locataire_sous_locataire_vide_hlm'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_occupes_par_locataire_sous_locataire_vide_hlm"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_occupes_par_locataire_meuble_hotel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_occupes_par_locataire_meuble_hotel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_occupes_par_loge_gratuitemenent'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_occupes_par_loge_gratuitemenent"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_STOCD_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('AGEMEN8' as TEXT) as "champs",
      cast(  
           "AGEMEN8"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_moins_15'  , 
        
            'pr_15_19'  , 
        
            'pr_20_24'  , 
        
            'pr_25_39'  , 
        
            'pr_40_54'  , 
        
            'pr_55_64'  , 
        
            'pr_64_79'  , 
        
            'pr_plus_80' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'pr_moins_15'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_moins_15"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_15_19'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_15_19"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_20_24'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_20_24"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_25_39'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_25_39"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_40_54'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_40_54"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_55_64'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_55_64"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_64_79'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_64_79"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_plus_80'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_plus_80"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_AGEMEN8_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('TYPC' as TEXT) as "champs",
      cast(  
           "TYPC"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'logement_type_contruction_un_logement_isole'  , 
        
            'logement_type_contruction_un_logement_groupe'  , 
        
            'logement_type_contruction_plusieurs_logements'  , 
        
            'logement_type_contruction_autre_logement'  , 
        
            'logement_type_construction_provisoire' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'logement_type_contruction_un_logement_isole'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_contruction_un_logement_isole"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_contruction_un_logement_groupe'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_contruction_un_logement_groupe"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_contruction_plusieurs_logements'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_contruction_plusieurs_logements"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_contruction_autre_logement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_contruction_autre_logement"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_construction_provisoire'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_construction_provisoire"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_TYPC_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CMBL' as TEXT) as "champs",
      cast(  
           "CMBL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'metro__combustible_chauffage_urbain'  , 
        
            'metro__combustible_gaz_de_ville_ou_reseau'  , 
        
            'metro__combustible_fioul'  , 
        
            'metro__combustible_electricte'  , 
        
            'metro__combustible_gaz_bouteille_ou_citerne'  , 
        
            'metro__combustible_autre' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'metro__combustible_chauffage_urbain'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__combustible_chauffage_urbain"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__combustible_gaz_de_ville_ou_reseau'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__combustible_gaz_de_ville_ou_reseau"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__combustible_fioul'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__combustible_fioul"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__combustible_electricte'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__combustible_electricte"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__combustible_gaz_bouteille_ou_citerne'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__combustible_gaz_bouteille_ou_citerne"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__combustible_autre'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__combustible_autre"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_CMBL_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CLIM' as TEXT) as "champs",
      cast(  
           "CLIM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__avec_piece_climatisee'  , 
        
            'dom__sans_piece_climatisee' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__avec_piece_climatisee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__avec_piece_climatisee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__sans_piece_climatisee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__sans_piece_climatisee"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_CLIM_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('ANEMR' as TEXT) as "champs",
      cast(  
           "ANEMR"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'emmenagement_moins_2_ans'  , 
        
            'emmenagement_2_4_ans'  , 
        
            'emmenagement_5_9_ans'  , 
        
            'emmenagement_10_19_ans'  , 
        
            'emmenagement_20_29_ans'  , 
        
            'emmenagement_30_39_ans'  , 
        
            'emmenagement_40_49_ans'  , 
        
            'emmenagement_50_59_ans'  , 
        
            'emmenagement_60_69_ans'  , 
        
            'emmenagement_plus_70_ans' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'emmenagement_moins_2_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_moins_2_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_2_4_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_2_4_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_5_9_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_5_9_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_10_19_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_10_19_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_20_29_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_20_29_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_30_39_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_30_39_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_40_49_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_40_49_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_50_59_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_50_59_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_60_69_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_60_69_ans"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'emmenagement_plus_70_ans'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "emmenagement_plus_70_ans"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_ANEMR_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('EGOUL' as TEXT) as "champs",
      cast(  
           "EGOUL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__raccordement_egouts'  , 
        
            'dom__raccordement_fosse_septique'  , 
        
            'dom__raccordement_puisard'  , 
        
            'dom__evacuation_sol' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__raccordement_egouts'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__raccordement_egouts"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__raccordement_fosse_septique'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__raccordement_fosse_septique"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__raccordement_puisard'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__raccordement_puisard"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__evacuation_sol'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__evacuation_sol"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_EGOUL_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('GARL' as TEXT) as "champs",
      cast(  
           "GARL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'avec_emplacement_stationnement'  , 
        
            'sans_emplacement_stationnement' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'avec_emplacement_stationnement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "avec_emplacement_stationnement"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'sans_emplacement_stationnement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "sans_emplacement_stationnement"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_GARL_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CUIS' as TEXT) as "champs",
      cast(  
           "CUIS"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__avec_cuisine_interieure_evier'  , 
        
            'dom__sans_cuisine_interieure_evier' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__avec_cuisine_interieure_evier'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__avec_cuisine_interieure_evier"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__sans_cuisine_interieure_evier'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__sans_cuisine_interieure_evier"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_CUIS_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('NBPI' as TEXT) as "champs",
      cast(  
           "NBPI"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'logements_avec_1_piece'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_2_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_3_pieces'  , 
        
            'logements_avec_4_pieces'  , 
        
            'logements_avec_5_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces'  , 
        
            'logements_avec_6_et_plus_pieces' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
            
        
      
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'logements_avec_1_piece'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_avec_1_piece"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_avec_6_et_plus_pieces'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_avec_6_et_plus_pieces"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_avec_2_pieces'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_avec_2_pieces"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_avec_3_pieces'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_avec_3_pieces"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_avec_4_pieces'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_avec_4_pieces"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_avec_5_pieces'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_avec_5_pieces"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_NBPI_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('SURF' as TEXT) as "champs",
      cast(  
           "SURF"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'logements_moins_30_mc'  , 
        
            'logements_30_40_mc'  , 
        
            'logements_40_60_mc'  , 
        
            'logements_60_80_mc'  , 
        
            'logements_80_100_mc'  , 
        
            'logements_100_120_mc'  , 
        
            'logements_plus_120_mc' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'logements_moins_30_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_moins_30_mc"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_30_40_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_30_40_mc"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_40_60_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_40_60_mc"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_60_80_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_60_80_mc"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_80_100_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_80_100_mc"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_100_120_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_100_120_mc"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logements_plus_120_mc'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logements_plus_120_mc"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_SURF_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('SANIDOM' as TEXT) as "champs",
      cast(  
           "SANIDOM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'  , 
        
            'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'  , 
        
            'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'  , 
        
            'dom__logement_ni_baignoire_ni_douche' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__logement_ni_baignoire_ni_douche'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__logement_ni_baignoire_ni_douche"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_SANIDOM_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('BATI' as TEXT) as "champs",
      cast(  
           "BATI"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__habitation_de_fortune'  , 
        
            'dom__cases_traditionnelles'  , 
        
            'dom__habitation_en_bois'  , 
        
            'dom__habitation_en_dur' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__habitation_de_fortune'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__habitation_de_fortune"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__cases_traditionnelles'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__cases_traditionnelles"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__habitation_en_bois'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__habitation_en_bois"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__habitation_en_dur'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__habitation_en_dur"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_BATI_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CHAU' as TEXT) as "champs",
      cast(  
           "CHAU"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__presence_chauffage'  , 
        
            'dom__abscence_chauffage' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__presence_chauffage'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__presence_chauffage"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__abscence_chauffage'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__abscence_chauffage"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_CHAU_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('CHOS' as TEXT) as "champs",
      cast(  
           "CHOS"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__presence_chauffe_eau_solaire'  , 
        
            'dom__abscence_chauffe_eau_solaire' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__presence_chauffe_eau_solaire'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__presence_chauffe_eau_solaire"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__abscence_chauffe_eau_solaire'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__abscence_chauffe_eau_solaire"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_CHOS_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('HLML' as TEXT) as "champs",
      cast(  
           "HLML"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'appartient_hlm'  , 
        
            'appartient_pas_hlm' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'appartient_hlm'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "appartient_hlm"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'appartient_pas_hlm'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "appartient_pas_hlm"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_HLML_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('TYPL' as TEXT) as "champs",
      cast(  
           "TYPL"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'logement_type_maison'  , 
        
            'logement_type_appartement'  , 
        
            'logement_type_appartement_foyer'  , 
        
            'logement_type_chambre_hotel'  , 
        
            'logement_type_habitation_fortune'  , 
        
            'logement_type_piece_independante' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'logement_type_maison'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_maison"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_appartement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_appartement"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_appartement_foyer'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_appartement_foyer"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_chambre_hotel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_chambre_hotel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_habitation_fortune'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_habitation_fortune"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'logement_type_piece_independante'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "logement_type_piece_independante"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_TYPL_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('WC' as TEXT) as "champs",
      cast(  
           "WC"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__logements_avec_wc_interieurs'  , 
        
            'dom__logements_sans_wc_interieurs' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__logements_avec_wc_interieurs'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__logements_avec_wc_interieurs"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__logements_sans_wc_interieurs'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__logements_sans_wc_interieurs"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_WC_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('SANI' as TEXT) as "champs",
      cast(  
           "SANI"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'metro__logement_ni_baignoire_ni_douche'  , 
        
            'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'  , 
        
            'metro__logement_salle_de_bain' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'metro__logement_ni_baignoire_ni_douche'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__logement_ni_baignoire_ni_douche"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__logement_avec_baignoire_ou_douche_hors_piece_reservee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'metro__logement_salle_de_bain'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "metro__logement_salle_de_bain"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_SANI_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('BAIN' as TEXT) as "champs",
      cast(  
           "BAIN"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__beignoire_ou_douche'  , 
        
            'dom__ni_beignoire_ni_douche' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__beignoire_ou_douche'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__beignoire_ou_douche"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__ni_beignoire_ni_douche'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__ni_beignoire_ni_douche"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_BAIN_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('ASCEN' as TEXT) as "champs",
      cast(  
           "ASCEN"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'avec_ascensseur'  , 
        
            'sans_ascensseur' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'avec_ascensseur'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "avec_ascensseur"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'sans_ascensseur'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "sans_ascensseur"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_ASCEN_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('ELEC' as TEXT) as "champs",
      cast(  
           "ELEC"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__avec_electricite'  , 
        
            'dom__sans_electricite' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__avec_electricite'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__avec_electricite"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__sans_electricite'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__sans_electricite"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_ELEC_par_geo
      USING (code_iris)

    

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

    from "defaultdb"."intermediaires"."habitat_renomee"

    union all
    select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('EAU' as TEXT) as "champs",
      cast(  
           "EAU"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."habitat_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'dom__eau_froide_seulement'  , 
        
            'dom__eau_froide_et_chaude'  , 
        
            'dom__aucun_point_eau' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'dom__eau_froide_seulement'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__eau_froide_seulement"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__eau_froide_et_chaude'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__eau_froide_et_chaude"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'dom__aucun_point_eau'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "dom__aucun_point_eau"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_EAU_par_geo
      USING (code_iris)

    

  )

select * from aggregated

), 
aggregated_with_infos_iris as (
    SELECT
      *
    FROM
      aggregated
    LEFT JOIN
	    "defaultdb"."prepare"."infos_iris" as infos_iris
    ON
      aggregated.code_iris = infos_iris.code_iris_2024
  )

SELECT 
    *  
FROM
    aggregated_with_infos_iris