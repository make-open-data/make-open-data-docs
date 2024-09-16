

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.




with communes as (
    SELECT 
      code_commune_insee,
      CAST( SUM(CAST(poids_du_logement AS numeric)) AS INT) AS nombre_de_logements
    FROM 
      "defaultdb"."intermediaires"."decoder_habitat"
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('mode_evacuation_eaux_usagers__dom' as TEXT) as champs,
      cast(  
           mode_evacuation_eaux_usagers__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'mode_evacuation_eaux_usagers__dom__evacuation_des_eaux_usees_a_meme_le_sol'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "mode_evacuation_eaux_usagers__dom__evacuation_des_eaux_usees_a_meme_le_sol"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_evacuation_eaux_usagers__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "mode_evacuation_eaux_usagers__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_evacuation_eaux_usagers__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "mode_evacuation_eaux_usagers__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_evacuation_eaux_usagers__dom__raccordement_a_fosse_septique'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "mode_evacuation_eaux_usagers__dom__raccordement_a_fosse_septique"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_evacuation_eaux_usagers__dom__raccordement_a_puisard'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "mode_evacuation_eaux_usagers__dom__raccordement_a_puisard"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_evacuation_eaux_usagers__dom__raccordement_au_reseau_d_egouts'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "mode_evacuation_eaux_usagers__dom__raccordement_au_reseau_d_egouts"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('annee_regroupe_ammenagement_logement' as TEXT) as champs,
      cast(  
           annee_regroupe_ammenagement_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_apres_1999'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_apres_1999"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1900_et_1919'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1900_et_1919"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1920_et_1939'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1920_et_1939"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1940_et_1959'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1940_et_1959"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1960_et_1969'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1960_et_1969"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1970_et_1979'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1970_et_1979"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1980_et_1989'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1980_et_1989"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__emmenagement_entre_1990_et_1999'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__emmenagement_entre_1990_et_1999"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'annee_regroupe_ammenagement_logement__logement_ordinaire_inoccupe'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "annee_regroupe_ammenagement_logement__logement_ordinaire_inoccupe"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('installation_sanitaires__dom' as TEXT) as champs,
      cast(  
           installation_sanitaires__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__dom__avec_baignoire_ou_douche_et_avec_wc_a_l_interieur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__dom__avec_baignoire_ou_douche_et_avec_wc_a_l_interieur"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__dom__avec_baignoire_ou_douche_mais_sans_wc_a_l_interieur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__dom__avec_baignoire_ou_douche_mais_sans_wc_a_l_interieur"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__dom__sans_baignoire_ni_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__dom__sans_baignoire_ni_douche"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__dom__sans_baignoire_ni_douche_mais_avec_wc_a_l_interieur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__dom__sans_baignoire_ni_douche_mais_avec_wc_a_l_interieur"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('combustible_principal_logement__metro' as TEXT) as champs,
      cast(  
           combustible_principal_logement__metro
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__autre'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__autre"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__chauffage_urbain'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__chauffage_urbain"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__electricite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__electricite"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__fioul_mazout'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__fioul_mazout"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__gaz_en_bouteilles_ou_en_citerne'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__gaz_en_bouteilles_ou_en_citerne"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__gaz_ville_ou_reseau'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__gaz_ville_ou_reseau"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'combustible_principal_logement__metro__logement_ordinaire_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "combustible_principal_logement__metro__logement_ordinaire_dom"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('nombre_pieces_logement' as TEXT) as champs,
      cast(  
           nombre_pieces_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__01'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__01"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__02'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__02"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__03'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__03"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__04'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__04"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__05'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__05"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__06'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__06"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__07'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__07"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__08'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__08"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__09'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__09"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__10'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__11'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__12'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__13'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__14'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__15'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__16'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__17'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__18'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__19'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__20'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_pieces_logement__YY'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_pieces_logement__YY"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('superficie_logement' as TEXT) as champs,
      cast(  
           superficie_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__120_ou_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__120_ou_plus"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__de_100_a_moins_120'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__de_100_a_moins_120"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__de_30_a_moins_40'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__de_30_a_moins_40"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__de_40_a_moins_60'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__de_40_a_moins_60"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__de_60_a_moins_80'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__de_60_a_moins_80"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__de_80_a_moins_100'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__de_80_a_moins_100"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'superficie_logement__moins_30'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "superficie_logement__moins_30"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('nombre_deux_roues_menage__dom' as TEXT) as champs,
      cast(  
           nombre_deux_roues_menage__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'nombre_deux_roues_menage__dom__aucdeux_roues_a_moteur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_deux_roues_menage__dom__aucdeux_roues_a_moteur"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_deux_roues_menage__dom__deux_deux_roues_a_moteur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_deux_roues_menage__dom__deux_deux_roues_a_moteur"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_deux_roues_menage__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_deux_roues_menage__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_deux_roues_menage__dom__logement_ordinaire_inoccupe_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_deux_roues_menage__dom__logement_ordinaire_inoccupe_dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_deux_roues_menage__dom__trois_deux_roues_a_moteur_ou_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_deux_roues_menage__dom__trois_deux_roues_a_moteur_ou_plus"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_deux_roues_menage__dom__un_seul_deux_roues_a_moteur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_deux_roues_menage__dom__un_seul_deux_roues_a_moteur"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('baignoire_ou_douche__dom' as TEXT) as champs,
      cast(  
           baignoire_ou_douche__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'baignoire_ou_douche__dom__avec_baignoire_ou_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "baignoire_ou_douche__dom__avec_baignoire_ou_douche"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'baignoire_ou_douche__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "baignoire_ou_douche__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'baignoire_ou_douche__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "baignoire_ou_douche__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'baignoire_ou_douche__dom__sans_baignoire_ni_douche'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "baignoire_ou_douche__dom__sans_baignoire_ni_douche"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('presence_wc_interieur_logement__dom' as TEXT) as champs,
      cast(  
           presence_wc_interieur_logement__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'presence_wc_interieur_logement__dom__avec_w._c._a_l_interieur_logement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "presence_wc_interieur_logement__dom__avec_w._c._a_l_interieur_logement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'presence_wc_interieur_logement__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "presence_wc_interieur_logement__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'presence_wc_interieur_logement__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "presence_wc_interieur_logement__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'presence_wc_interieur_logement__dom__sans_w._c._a_l_interieur_logement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "presence_wc_interieur_logement__dom__sans_w._c._a_l_interieur_logement"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('chauffage_central_logement__metro' as TEXT) as champs,
      cast(  
           chauffage_central_logement__metro
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'chauffage_central_logement__metro__autre_moyen_chauffage'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_central_logement__metro__autre_moyen_chauffage"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_central_logement__metro__chauffage_central_collectif_y_compris_chauffage_urbain'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_central_logement__metro__chauffage_central_collectif_y_compris_chauffage_urbain"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_central_logement__metro__chauffage_central_individuel_avec_chaudiere_propre_au_logement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_central_logement__metro__chauffage_central_individuel_avec_chaudiere_propre_au_logement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_central_logement__metro__chauffage_tout_electrique'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_central_logement__metro__chauffage_tout_electrique"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_central_logement__metro__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_central_logement__metro__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_central_logement__metro__logement_ordinaire_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_central_logement__metro__logement_ordinaire_dom"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('electricite_logement__dom' as TEXT) as champs,
      cast(  
           electricite_logement__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'electricite_logement__dom__avec_electricite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "electricite_logement__dom__avec_electricite"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'electricite_logement__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "electricite_logement__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'electricite_logement__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "electricite_logement__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'electricite_logement__dom__sans_electricite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "electricite_logement__dom__sans_electricite"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('emplacement_stationnement_reserve' as TEXT) as champs,
      cast(  
           emplacement_stationnement_reserve
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'emplacement_stationnement_reserve__avec_emplacement_s_stationnement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emplacement_stationnement_reserve__avec_emplacement_s_stationnement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'emplacement_stationnement_reserve__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emplacement_stationnement_reserve__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'emplacement_stationnement_reserve__sans_emplacement_stationnement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "emplacement_stationnement_reserve__sans_emplacement_stationnement"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('desserte_ascensseur' as TEXT) as champs,
      cast(  
           desserte_ascensseur
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'desserte_ascensseur__avec_ascenseur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "desserte_ascensseur__avec_ascenseur"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'desserte_ascensseur__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "desserte_ascensseur__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'desserte_ascensseur__sans_ascenseur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "desserte_ascensseur__sans_ascenseur"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('aspect_bati__dom' as TEXT) as champs,
      cast(  
           aspect_bati__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'aspect_bati__dom__cases_traditionnelles'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aspect_bati__dom__cases_traditionnelles"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aspect_bati__dom__habitations_fortune'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aspect_bati__dom__habitations_fortune"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aspect_bati__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aspect_bati__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aspect_bati__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aspect_bati__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aspect_bati__dom__maisons_ou_immeubles_en_bois'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aspect_bati__dom__maisons_ou_immeubles_en_bois"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aspect_bati__dom__maisons_ou_immeubles_en_dur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aspect_bati__dom__maisons_ou_immeubles_en_dur"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('type_logement' as TEXT) as champs,
      cast(  
           type_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'type_logement__appartement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_logement__appartement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_logement__chambre_d_hotel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_logement__chambre_d_hotel"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_logement__habitation_fortune'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_logement__habitation_fortune"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_logement__logement_foyer'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_logement__logement_foyer"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_logement__maison'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_logement__maison"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_logement__piece_independante_ayant_sa_propre_entree'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_logement__piece_independante_ayant_sa_propre_entree"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('cuisine_interieur_avec_evier__dom' as TEXT) as champs,
      cast(  
           cuisine_interieur_avec_evier__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'cuisine_interieur_avec_evier__dom__absence_cuisine_interieure_avec_evier'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "cuisine_interieur_avec_evier__dom__absence_cuisine_interieure_avec_evier"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'cuisine_interieur_avec_evier__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "cuisine_interieur_avec_evier__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'cuisine_interieur_avec_evier__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "cuisine_interieur_avec_evier__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'cuisine_interieur_avec_evier__dom__presence_cuisine_interieure_avec_evier'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "cuisine_interieur_avec_evier__dom__presence_cuisine_interieure_avec_evier"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('type_construction' as TEXT) as champs,
      cast(  
           type_construction
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'type_construction__batiment_a_usage_autre_qu_habitation'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_construction__batiment_a_usage_autre_qu_habitation"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_construction__batiment_d_habitation_2_logements_ou_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_construction__batiment_d_habitation_2_logements_ou_plus"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_construction__batiment_d_habitation_seul_logement_isole'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_construction__batiment_d_habitation_seul_logement_isole"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_construction__batiment_d_habitation_seul_logement_jumele_ou_groupe_toute_autre_facon'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_construction__batiment_d_habitation_seul_logement_jumele_ou_groupe_toute_autre_facon"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_construction__construction_provisoire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_construction__construction_provisoire"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_construction__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "type_construction__hors_residence_principale"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('min_une_piece_climatise__dom' as TEXT) as champs,
      cast(  
           min_une_piece_climatise__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'min_une_piece_climatise__dom__avec_piece_climatisee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "min_une_piece_climatise__dom__avec_piece_climatisee"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'min_une_piece_climatise__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "min_une_piece_climatise__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'min_une_piece_climatise__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "min_une_piece_climatise__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'min_une_piece_climatise__dom__sans_piece_climatisee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "min_une_piece_climatise__dom__sans_piece_climatisee"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('eau_potabke_interieur_logement__dom' as TEXT) as champs,
      cast(  
           eau_potabke_interieur_logement__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'eau_potabke_interieur_logement__dom__aucpoint_d_eau_a_l_interieur_logement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "eau_potabke_interieur_logement__dom__aucpoint_d_eau_a_l_interieur_logement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'eau_potabke_interieur_logement__dom__eau_froiet_chaude'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "eau_potabke_interieur_logement__dom__eau_froiet_chaude"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'eau_potabke_interieur_logement__dom__eau_froiseulement'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "eau_potabke_interieur_logement__dom__eau_froiseulement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'eau_potabke_interieur_logement__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "eau_potabke_interieur_logement__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'eau_potabke_interieur_logement__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "eau_potabke_interieur_logement__dom__logement_ordinaire_france_metropolitaine"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('achevement_construction_logement' as TEXT) as champs,
      cast(  
           achevement_construction_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__avant_1919'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__avant_1919"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__de_1919_a_1945'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__de_1919_a_1945"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__de_1946_a_1970'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__de_1946_a_1970"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__de_1971_a_1990'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__de_1971_a_1990"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__de_1991_a_2005'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__de_1991_a_2005"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2006'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2006"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2007'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2007"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2008'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2008"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2009'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2009"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2010'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2010"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2011'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2011"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2012'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2012"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2013'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2013"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2014'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2014"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2015'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2015"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2016'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2016"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2017'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2017"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2018_partiel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2018_partiel"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2019_partiel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2019_partiel"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2020_partiel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2020_partiel"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2021_partiel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2021_partiel"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'achevement_construction_logement__en_2022_partiel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "achevement_construction_logement__en_2022_partiel"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('chauffe_eau_solaire__dom' as TEXT) as champs,
      cast(  
           chauffe_eau_solaire__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'chauffe_eau_solaire__dom__absence_chauffe_eau_solaire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffe_eau_solaire__dom__absence_chauffe_eau_solaire"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffe_eau_solaire__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffe_eau_solaire__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffe_eau_solaire__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffe_eau_solaire__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffe_eau_solaire__dom__presence_chauffe_eau_solaire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffe_eau_solaire__dom__presence_chauffe_eau_solaire"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('categorie_logement' as TEXT) as champs,
      cast(  
           categorie_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'categorie_logement__logements_occasionnels'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "categorie_logement__logements_occasionnels"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'categorie_logement__logements_vacants'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "categorie_logement__logements_vacants"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'categorie_logement__residences_principales'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "categorie_logement__residences_principales"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'categorie_logement__residences_secondaires'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "categorie_logement__residences_secondaires"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('logement_hml' as TEXT) as champs,
      cast(  
           logement_hml
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'logement_hml__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_hml__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'logement_hml__logement_appartenant_a_organisme_hlm'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_hml__logement_appartenant_a_organisme_hlm"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'logement_hml__logement_n_appartenant_pas_a_organisme_hlm'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "logement_hml__logement_n_appartenant_pas_a_organisme_hlm"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('aciennete_regroupe_ammenagement_logement' as TEXT) as champs,
      cast(  
           aciennete_regroupe_ammenagement_logement
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__70_ans_ou_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__70_ans_ou_plus"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_10_a_19_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_10_a_19_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_20_a_29_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_20_a_29_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_2_a_4_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_2_a_4_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_30_a_39_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_30_a_39_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_40_a_49_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_40_a_49_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_50_a_59_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_50_a_59_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_5_a_9_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_5_a_9_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__de_60_a_69_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__de_60_a_69_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__logement_ordinaire_inoccupe'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__logement_ordinaire_inoccupe"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'aciennete_regroupe_ammenagement_logement__moins_2_ans'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "aciennete_regroupe_ammenagement_logement__moins_2_ans"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('residence_metropole_ou_dom' as TEXT) as champs,
      cast(  
           residence_metropole_ou_dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'residence_metropole_ou_dom__dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "residence_metropole_ou_dom__dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_metropole_ou_dom__france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "residence_metropole_ou_dom__france_metropolitaine"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('chauffage_logement__dom' as TEXT) as champs,
      cast(  
           chauffage_logement__dom
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'chauffage_logement__dom__absence_moyen_chauffage'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_logement__dom__absence_moyen_chauffage"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_logement__dom__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_logement__dom__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_logement__dom__logement_ordinaire_france_metropolitaine'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_logement__dom__logement_ordinaire_france_metropolitaine"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'chauffage_logement__dom__presence_moyen_chauffage'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "chauffage_logement__dom__presence_moyen_chauffage"
      
    
    
  

    from 
        deduplicated
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
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('code_iris_incomplet' as TEXT) as champs,
      cast(  
           code_iris_incomplet
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    union all
    select
        code_commune_insee,
        poids_du_logement,
        code_iris,

      cast('installation_sanitaires__metro' as TEXT) as champs,
      cast(  
           installation_sanitaires__metro
             
           as varchar) as valeur

    from "defaultdb"."intermediaires"."decoder_habitat"

    

), 
concatenated as (
        
    SELECT
        code_commune_insee,
        poids_du_logement,
        champs,
        valeur,
        champs || '__' || valeur AS champs__valeur
    FROM
        unpivoted

),
deduplicated as (
        
    SELECT
        code_commune_insee,
        champs,
        champs__valeur,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        concatenated
    GROUP BY
        code_commune_insee,
        champs__valeur,
        champs

),
pivoted as (
        





    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__metro__baignoire_ou_douche_hors_piece_reservee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__metro__baignoire_ou_douche_hors_piece_reservee"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__metro__hors_residence_principale'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__metro__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__metro__logement_ordinaire_dom'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__metro__logement_ordinaire_dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__metro__ni_baignoire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__metro__ni_baignoire"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'installation_sanitaires__metro__salle_s_bains_avec_douche_ou_baignoire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "installation_sanitaires__metro__salle_s_bains_avec_douche_ou_baignoire"
      
    
    
  

    from 
        deduplicated
    group by
        code_commune_insee


)

select * from pivoted
 )
      USING (code_commune_insee)

    

  ),
  aggregated_with_cog as (
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
    aggregated_with_cog