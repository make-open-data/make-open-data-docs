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

      cast('AEMMR' as TEXT) as "champs",
      cast(  
           "AEMMR"
             
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
        
            'AEMMR_2'  , 
        
            'AEMMR_3'  , 
        
            'AEMMR_4'  , 
        
            'AEMMR_5'  , 
        
            'AEMMR_6'  , 
        
            'AEMMR_7'  , 
        
            'AEMMR_8'  , 
        
            'AEMMR_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'AEMMR_2' then 'emmenagement_1900_1919'
            
                when 'AEMMR_3' then 'emmenagement_1920_1939'
            
                when 'AEMMR_4' then 'emmenagement_1940_1959'
            
                when 'AEMMR_5' then 'emmenagement_1940_1969'
            
                when 'AEMMR_6' then 'emmenagement_1970_1979'
            
                when 'AEMMR_7' then 'emmenagement_1980_1989'
            
                when 'AEMMR_8' then 'emmenagement_1990_1999'
            
                when 'AEMMR_9' then 'emmenagement_apres_1999'
            
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
      when champs_valeur_renomme = 'emmenagement_1900_1919'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1900_1919"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_1920_1939'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1920_1939"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_1940_1959'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1940_1959"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_1940_1969'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1940_1969"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_1970_1979'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1970_1979"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_1980_1989'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1980_1989"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_1990_1999'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_1990_1999"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_apres_1999'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_apres_1999"
      
    
    
  

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

      cast('ACHL' as TEXT) as "champs",
      cast(  
           "ACHL"
             
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
        
            'ACHL_A11'  , 
        
            'ACHL_A12'  , 
        
            'ACHL_B11'  , 
        
            'ACHL_B12'  , 
        
            'ACHL_C100'  , 
        
            'ACHL_C106'  , 
        
            'ACHL_C107'  , 
        
            'ACHL_C108'  , 
        
            'ACHL_C109'  , 
        
            'ACHL_C110'  , 
        
            'ACHL_C111'  , 
        
            'ACHL_C112'  , 
        
            'ACHL_C113'  , 
        
            'ACHL_C114'  , 
        
            'ACHL_C115'  , 
        
            'ACHL_C116'  , 
        
            'ACHL_C117'  , 
        
            'ACHL_C2018'  , 
        
            'ACHL_C2019'  , 
        
            'ACHL_C2020'  , 
        
            'ACHL_C2021'  , 
        
            'ACHL_C2022' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'ACHL_A11' then 'construction_avant_1919'
            
                when 'ACHL_A12' then 'construction_1919_1945'
            
                when 'ACHL_B11' then 'construction_1916_1970'
            
                when 'ACHL_B12' then 'construction_1971_1990'
            
                when 'ACHL_C100' then 'construction_1991_2025'
            
                when 'ACHL_C106' then 'construction_2006'
            
                when 'ACHL_C107' then 'construction_2007'
            
                when 'ACHL_C108' then 'construction_2008'
            
                when 'ACHL_C109' then 'construction_2009'
            
                when 'ACHL_C110' then 'construction_2010'
            
                when 'ACHL_C111' then 'construction_2011'
            
                when 'ACHL_C112' then 'construction_2012'
            
                when 'ACHL_C113' then 'construction_2013'
            
                when 'ACHL_C114' then 'construction_2014'
            
                when 'ACHL_C115' then 'construction_2015'
            
                when 'ACHL_C116' then 'construction_2016'
            
                when 'ACHL_C117' then 'construction_2017'
            
                when 'ACHL_C2018' then 'construction_2018'
            
                when 'ACHL_C2019' then 'construction_2019'
            
                when 'ACHL_C2020' then 'construction_2020'
            
                when 'ACHL_C2021' then 'construction_2021'
            
                when 'ACHL_C2022' then 'construction_2022'
            
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
      when champs_valeur_renomme = 'construction_avant_1919'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_avant_1919"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_1919_1945'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_1919_1945"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_1916_1970'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_1916_1970"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_1971_1990'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_1971_1990"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_1991_2025'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_1991_2025"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2006'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2006"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2007'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2007"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2008'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2008"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2009'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2009"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2010'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2010"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2011'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2011"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2012'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2012"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2013'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2013"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2014'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2014"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2015'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2015"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2016'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2016"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2017'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2017"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2018'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2018"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2019'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2019"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2020'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2020"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2021'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2021"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'construction_2022'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "construction_2022"
      
    
    
  

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

      cast('STOCD' as TEXT) as "champs",
      cast(  
           "STOCD"
             
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
        
            'STOCD_10'  , 
        
            'STOCD_21'  , 
        
            'STOCD_22'  , 
        
            'STOCD_23'  , 
        
            'STOCD_30' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'STOCD_10' then 'logements_occupes_par_proprietaires'
            
                when 'STOCD_21' then 'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'
            
                when 'STOCD_22' then 'logements_occupes_par_locataire_sous_locataire_vide_hlm'
            
                when 'STOCD_23' then 'logements_occupes_par_locataire_meuble_hotel'
            
                when 'STOCD_30' then 'logements_occupes_par_loge_gratuitemenent'
            
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
      when champs_valeur_renomme = 'logements_occupes_par_proprietaires'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_occupes_par_proprietaires"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_occupes_par_locataire_sous_locataire_vide_non_hlm"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_occupes_par_locataire_sous_locataire_vide_hlm'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_occupes_par_locataire_sous_locataire_vide_hlm"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_occupes_par_locataire_meuble_hotel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_occupes_par_locataire_meuble_hotel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_occupes_par_loge_gratuitemenent'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_occupes_par_loge_gratuitemenent"
      
    
    
  

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

      cast('AGEMEN8' as TEXT) as "champs",
      cast(  
           "AGEMEN8"
             
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
        
            'AGEMEN8_0'  , 
        
            'AGEMEN8_15'  , 
        
            'AGEMEN8_20'  , 
        
            'AGEMEN8_25'  , 
        
            'AGEMEN8_40'  , 
        
            'AGEMEN8_55'  , 
        
            'AGEMEN8_65'  , 
        
            'AGEMEN8_80' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'AGEMEN8_0' then 'pr_moins_15'
            
                when 'AGEMEN8_15' then 'pr_15_19'
            
                when 'AGEMEN8_20' then 'pr_20_24'
            
                when 'AGEMEN8_25' then 'pr_25_39'
            
                when 'AGEMEN8_40' then 'pr_40_54'
            
                when 'AGEMEN8_55' then 'pr_55_64'
            
                when 'AGEMEN8_65' then 'pr_64_79'
            
                when 'AGEMEN8_80' then 'pr_plus_80'
            
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
      when champs_valeur_renomme = 'pr_moins_15'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_moins_15"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_15_19'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_15_19"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_20_24'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_20_24"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_25_39'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_25_39"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_40_54'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_40_54"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_55_64'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_55_64"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_64_79'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_64_79"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_plus_80'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_plus_80"
      
    
    
  

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

      cast('TYPC' as TEXT) as "champs",
      cast(  
           "TYPC"
             
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
        
            'TYPC_1'  , 
        
            'TYPC_2'  , 
        
            'TYPC_3'  , 
        
            'TYPC_4'  , 
        
            'TYPC_5' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'TYPC_1' then 'logement_type_contruction_un_logement_isole'
            
                when 'TYPC_2' then 'logement_type_contruction_un_logement_groupe'
            
                when 'TYPC_3' then 'logement_type_contruction_plusieurs_logements'
            
                when 'TYPC_4' then 'logement_type_contruction_autre_logement'
            
                when 'TYPC_5' then 'logement_type_construction_provisoire'
            
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
      when champs_valeur_renomme = 'logement_type_contruction_un_logement_isole'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_contruction_un_logement_isole"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_contruction_un_logement_groupe'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_contruction_un_logement_groupe"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_contruction_plusieurs_logements'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_contruction_plusieurs_logements"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_contruction_autre_logement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_contruction_autre_logement"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_construction_provisoire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_construction_provisoire"
      
    
    
  

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

      cast('CMBL' as TEXT) as "champs",
      cast(  
           "CMBL"
             
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
        
            'CMBL_1'  , 
        
            'CMBL_2'  , 
        
            'CMBL_3'  , 
        
            'CMBL_4'  , 
        
            'CMBL_5'  , 
        
            'CMBL_6' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'CMBL_1' then 'metro__combustible_chauffage_urbain'
            
                when 'CMBL_2' then 'metro__combustible_gaz_de_ville_ou_reseau'
            
                when 'CMBL_3' then 'metro__combustible_fioul'
            
                when 'CMBL_4' then 'metro__combustible_electricte'
            
                when 'CMBL_5' then 'metro__combustible_gaz_bouteille_ou_citerne'
            
                when 'CMBL_6' then 'metro__combustible_autre'
            
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
      when champs_valeur_renomme = 'metro__combustible_chauffage_urbain'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__combustible_chauffage_urbain"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__combustible_gaz_de_ville_ou_reseau'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__combustible_gaz_de_ville_ou_reseau"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__combustible_fioul'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__combustible_fioul"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__combustible_electricte'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__combustible_electricte"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__combustible_gaz_bouteille_ou_citerne'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__combustible_gaz_bouteille_ou_citerne"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__combustible_autre'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__combustible_autre"
      
    
    
  

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

      cast('CLIM' as TEXT) as "champs",
      cast(  
           "CLIM"
             
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
        
            'CLIM_1'  , 
        
            'CLIM_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'CLIM_1' then 'dom__avec_piece_climatisee'
            
                when 'CLIM_2' then 'dom__sans_piece_climatisee'
            
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
      when champs_valeur_renomme = 'dom__avec_piece_climatisee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__avec_piece_climatisee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__sans_piece_climatisee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__sans_piece_climatisee"
      
    
    
  

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

      cast('ANEMR' as TEXT) as "champs",
      cast(  
           "ANEMR"
             
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
        
            'ANEMR_0'  , 
        
            'ANEMR_1'  , 
        
            'ANEMR_2'  , 
        
            'ANEMR_3'  , 
        
            'ANEMR_4'  , 
        
            'ANEMR_5'  , 
        
            'ANEMR_6'  , 
        
            'ANEMR_7'  , 
        
            'ANEMR_8'  , 
        
            'ANEMR_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'ANEMR_0' then 'emmenagement_moins_2_ans'
            
                when 'ANEMR_1' then 'emmenagement_2_4_ans'
            
                when 'ANEMR_2' then 'emmenagement_5_9_ans'
            
                when 'ANEMR_3' then 'emmenagement_10_19_ans'
            
                when 'ANEMR_4' then 'emmenagement_20_29_ans'
            
                when 'ANEMR_5' then 'emmenagement_30_39_ans'
            
                when 'ANEMR_6' then 'emmenagement_40_49_ans'
            
                when 'ANEMR_7' then 'emmenagement_50_59_ans'
            
                when 'ANEMR_8' then 'emmenagement_60_69_ans'
            
                when 'ANEMR_9' then 'emmenagement_plus_70_ans'
            
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
      when champs_valeur_renomme = 'emmenagement_moins_2_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_moins_2_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_2_4_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_2_4_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_5_9_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_5_9_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_10_19_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_10_19_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_20_29_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_20_29_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_30_39_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_30_39_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_40_49_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_40_49_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_50_59_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_50_59_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_60_69_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_60_69_ans"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'emmenagement_plus_70_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emmenagement_plus_70_ans"
      
    
    
  

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

      cast('EGOUL' as TEXT) as "champs",
      cast(  
           "EGOUL"
             
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
        
            'EGOUL_1'  , 
        
            'EGOUL_2'  , 
        
            'EGOUL_3'  , 
        
            'EGOUL_4' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'EGOUL_1' then 'dom__raccordement_egouts'
            
                when 'EGOUL_2' then 'dom__raccordement_fosse_septique'
            
                when 'EGOUL_3' then 'dom__raccordement_puisard'
            
                when 'EGOUL_4' then 'dom__evacuation_sol'
            
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
      when champs_valeur_renomme = 'dom__raccordement_egouts'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__raccordement_egouts"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__raccordement_fosse_septique'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__raccordement_fosse_septique"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__raccordement_puisard'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__raccordement_puisard"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__evacuation_sol'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__evacuation_sol"
      
    
    
  

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

      cast('GARL' as TEXT) as "champs",
      cast(  
           "GARL"
             
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
        
            'GARL_1'  , 
        
            'GARL_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'GARL_1' then 'avec_emplacement_stationnement'
            
                when 'GARL_2' then 'sans_emplacement_stationnement'
            
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
      when champs_valeur_renomme = 'avec_emplacement_stationnement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "avec_emplacement_stationnement"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'sans_emplacement_stationnement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "sans_emplacement_stationnement"
      
    
    
  

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

      cast('CUIS' as TEXT) as "champs",
      cast(  
           "CUIS"
             
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
        
            'CUIS_1'  , 
        
            'CUIS_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'CUIS_1' then 'dom__avec_cuisine_interieure_evier'
            
                when 'CUIS_2' then 'dom__sans_cuisine_interieure_evier'
            
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
      when champs_valeur_renomme = 'dom__avec_cuisine_interieure_evier'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__avec_cuisine_interieure_evier"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__sans_cuisine_interieure_evier'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__sans_cuisine_interieure_evier"
      
    
    
  

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

      cast('NBPI' as TEXT) as "champs",
      cast(  
           "NBPI"
             
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
        
            'NBPI_1'  , 
        
            'NBPI_10'  , 
        
            'NBPI_11'  , 
        
            'NBPI_12'  , 
        
            'NBPI_13'  , 
        
            'NBPI_14'  , 
        
            'NBPI_15'  , 
        
            'NBPI_16'  , 
        
            'NBPI_17'  , 
        
            'NBPI_18'  , 
        
            'NBPI_19'  , 
        
            'NBPI_2'  , 
        
            'NBPI_20'  , 
        
            'NBPI_3'  , 
        
            'NBPI_4'  , 
        
            'NBPI_5'  , 
        
            'NBPI_6'  , 
        
            'NBPI_7'  , 
        
            'NBPI_8'  , 
        
            'NBPI_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'NBPI_1' then 'logements_avec_1_piece'
            
                when 'NBPI_10' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_11' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_12' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_13' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_14' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_15' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_16' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_17' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_18' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_19' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_2' then 'logements_avec_2_pieces'
            
                when 'NBPI_20' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_3' then 'logements_avec_3_pieces'
            
                when 'NBPI_4' then 'logements_avec_4_pieces'
            
                when 'NBPI_5' then 'logements_avec_5_pieces'
            
                when 'NBPI_6' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_7' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_8' then 'logements_avec_6_et_plus_pieces'
            
                when 'NBPI_9' then 'logements_avec_6_et_plus_pieces'
            
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
      when champs_valeur_renomme = 'logements_avec_1_piece'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_avec_1_piece"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_avec_6_et_plus_pieces'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_avec_6_et_plus_pieces"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_avec_2_pieces'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_avec_2_pieces"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_avec_3_pieces'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_avec_3_pieces"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_avec_4_pieces'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_avec_4_pieces"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_avec_5_pieces'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_avec_5_pieces"
      
    
    
  

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

      cast('SURF' as TEXT) as "champs",
      cast(  
           "SURF"
             
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
        
            'SURF_1'  , 
        
            'SURF_2'  , 
        
            'SURF_3'  , 
        
            'SURF_4'  , 
        
            'SURF_5'  , 
        
            'SURF_6'  , 
        
            'SURF_7' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'SURF_1' then 'logements_moins_30_mc'
            
                when 'SURF_2' then 'logements_30_40_mc'
            
                when 'SURF_3' then 'logements_40_60_mc'
            
                when 'SURF_4' then 'logements_60_80_mc'
            
                when 'SURF_5' then 'logements_80_100_mc'
            
                when 'SURF_6' then 'logements_100_120_mc'
            
                when 'SURF_7' then 'logements_plus_120_mc'
            
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
      when champs_valeur_renomme = 'logements_moins_30_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_moins_30_mc"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_30_40_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_30_40_mc"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_40_60_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_40_60_mc"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_60_80_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_60_80_mc"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_80_100_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_80_100_mc"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_100_120_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_100_120_mc"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logements_plus_120_mc'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logements_plus_120_mc"
      
    
    
  

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

      cast('SANIDOM' as TEXT) as "champs",
      cast(  
           "SANIDOM"
             
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
        
            'SANIDOM_11'  , 
        
            'SANIDOM_12'  , 
        
            'SANIDOM_21'  , 
        
            'SANIDOM_22' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'SANIDOM_11' then 'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'
            
                when 'SANIDOM_12' then 'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'
            
                when 'SANIDOM_21' then 'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'
            
                when 'SANIDOM_22' then 'dom__logement_ni_baignoire_ni_douche'
            
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
      when champs_valeur_renomme = 'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__logement_ni_baignoire_ni_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__logement_ni_baignoire_ni_douche"
      
    
    
  

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

      cast('BATI' as TEXT) as "champs",
      cast(  
           "BATI"
             
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
        
            'BATI_1'  , 
        
            'BATI_2'  , 
        
            'BATI_3'  , 
        
            'BATI_4' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'BATI_1' then 'dom__habitation_de_fortune'
            
                when 'BATI_2' then 'dom__cases_traditionnelles'
            
                when 'BATI_3' then 'dom__habitation_en_bois'
            
                when 'BATI_4' then 'dom__habitation_en_dur'
            
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
      when champs_valeur_renomme = 'dom__habitation_de_fortune'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__habitation_de_fortune"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__cases_traditionnelles'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__cases_traditionnelles"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__habitation_en_bois'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__habitation_en_bois"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__habitation_en_dur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__habitation_en_dur"
      
    
    
  

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

      cast('CHAU' as TEXT) as "champs",
      cast(  
           "CHAU"
             
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
        
            'CHAU_1'  , 
        
            'CHAU_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'CHAU_1' then 'dom__presence_chauffage'
            
                when 'CHAU_2' then 'dom__abscence_chauffage'
            
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
      when champs_valeur_renomme = 'dom__presence_chauffage'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__presence_chauffage"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__abscence_chauffage'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__abscence_chauffage"
      
    
    
  

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

      cast('CHOS' as TEXT) as "champs",
      cast(  
           "CHOS"
             
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
        
            'CHOS_1'  , 
        
            'CHOS_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'CHOS_1' then 'dom__presence_chauffe_eau_solaire'
            
                when 'CHOS_2' then 'dom__abscence_chauffe_eau_solaire'
            
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
      when champs_valeur_renomme = 'dom__presence_chauffe_eau_solaire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__presence_chauffe_eau_solaire"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__abscence_chauffe_eau_solaire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__abscence_chauffe_eau_solaire"
      
    
    
  

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

      cast('HLML' as TEXT) as "champs",
      cast(  
           "HLML"
             
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
        
            'HLML_1'  , 
        
            'HLML_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'HLML_1' then 'appartient_hlm'
            
                when 'HLML_2' then 'appartient_pas_hlm'
            
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
      when champs_valeur_renomme = 'appartient_hlm'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "appartient_hlm"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'appartient_pas_hlm'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "appartient_pas_hlm"
      
    
    
  

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

      cast('TYPL' as TEXT) as "champs",
      cast(  
           "TYPL"
             
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
        
            'TYPL_1'  , 
        
            'TYPL_2'  , 
        
            'TYPL_3'  , 
        
            'TYPL_4'  , 
        
            'TYPL_5'  , 
        
            'TYPL_6' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'TYPL_1' then 'logement_type_maison'
            
                when 'TYPL_2' then 'logement_type_appartement'
            
                when 'TYPL_3' then 'logement_type_appartement_foyer'
            
                when 'TYPL_4' then 'logement_type_chambre_hotel'
            
                when 'TYPL_5' then 'logement_type_habitation_fortune'
            
                when 'TYPL_6' then 'logement_type_piece_independante'
            
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
      when champs_valeur_renomme = 'logement_type_maison'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_maison"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_appartement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_appartement"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_appartement_foyer'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_appartement_foyer"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_chambre_hotel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_chambre_hotel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_habitation_fortune'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_habitation_fortune"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_type_piece_independante'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_type_piece_independante"
      
    
    
  

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

      cast('WC' as TEXT) as "champs",
      cast(  
           "WC"
             
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
        
            'WC_1'  , 
        
            'WC_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'WC_1' then 'dom__logements_avec_wc_interieurs'
            
                when 'WC_2' then 'dom__logements_sans_wc_interieurs'
            
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
      when champs_valeur_renomme = 'dom__logements_avec_wc_interieurs'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__logements_avec_wc_interieurs"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__logements_sans_wc_interieurs'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__logements_sans_wc_interieurs"
      
    
    
  

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

      cast('SANI' as TEXT) as "champs",
      cast(  
           "SANI"
             
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
        
            'SANI_0'  , 
        
            'SANI_1'  , 
        
            'SANI_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'SANI_0' then 'metro__logement_ni_baignoire_ni_douche'
            
                when 'SANI_1' then 'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'
            
                when 'SANI_2' then 'metro__logement_salle_de_bain'
            
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
      when champs_valeur_renomme = 'metro__logement_ni_baignoire_ni_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__logement_ni_baignoire_ni_douche"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__logement_avec_baignoire_ou_douche_hors_piece_reservee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'metro__logement_salle_de_bain'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "metro__logement_salle_de_bain"
      
    
    
  

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

      cast('BAIN' as TEXT) as "champs",
      cast(  
           "BAIN"
             
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
        
            'BAIN_1'  , 
        
            'BAIN_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'BAIN_1' then 'dom__beignoire_ou_douche'
            
                when 'BAIN_2' then 'dom__ni_beignoire_ni_douche'
            
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
      when champs_valeur_renomme = 'dom__beignoire_ou_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__beignoire_ou_douche"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__ni_beignoire_ni_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__ni_beignoire_ni_douche"
      
    
    
  

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

      cast('ASCEN' as TEXT) as "champs",
      cast(  
           "ASCEN"
             
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
        
            'ASCEN_1'  , 
        
            'ASCEN_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'ASCEN_1' then 'avec_ascensseur'
            
                when 'ASCEN_2' then 'sans_ascensseur'
            
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
      when champs_valeur_renomme = 'avec_ascensseur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "avec_ascensseur"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'sans_ascensseur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "sans_ascensseur"
      
    
    
  

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

      cast('CATL' as TEXT) as "champs",
      cast(  
           "CATL"
             
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
        
            'CATL_1'  , 
        
            'CATL_2'  , 
        
            'CATL_3'  , 
        
            'CATL_4' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'CATL_1' then 'residence_principale'
            
                when 'CATL_2' then 'logement_occasionnel'
            
                when 'CATL_3' then 'residence_secondaire'
            
                when 'CATL_4' then 'logement_vacant'
            
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
      when champs_valeur_renomme = 'residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_occasionnel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_occasionnel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'residence_secondaire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "residence_secondaire"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'logement_vacant'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_vacant"
      
    
    
  

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

      cast('ELEC' as TEXT) as "champs",
      cast(  
           "ELEC"
             
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
        
            'ELEC_1'  , 
        
            'ELEC_2' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'ELEC_1' then 'dom__avec_electricite'
            
                when 'ELEC_2' then 'dom__sans_electricite'
            
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
      when champs_valeur_renomme = 'dom__avec_electricite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__avec_electricite"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__sans_electricite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__sans_electricite"
      
    
    
  

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

      cast('EAU' as TEXT) as "champs",
      cast(  
           "EAU"
             
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
        
            'EAU_1'  , 
        
            'EAU_2'  , 
        
            'EAU_3' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'EAU_1' then 'dom__eau_froide_seulement'
            
                when 'EAU_2' then 'dom__eau_froide_et_chaude'
            
                when 'EAU_3' then 'dom__aucun_point_eau'
            
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
      when champs_valeur_renomme = 'dom__eau_froide_seulement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__eau_froide_seulement"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__eau_froide_et_chaude'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__eau_froide_et_chaude"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'dom__aucun_point_eau'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "dom__aucun_point_eau"
      
    
    
  

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