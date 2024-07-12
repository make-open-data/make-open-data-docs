

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.




with communes as (
    SELECT 
      code_commune_insee,
      CAST( SUM(CAST(poids_du_logement AS numeric)) AS INT) AS nombre_de_logements
    FROM 
      "defaultdb"."prep"."decoder_demographie"
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

      cast('personnes_moins_3_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_3_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_3_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_3_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_3_ans_menage__Y"
      
    
    
  

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

      cast('personnes_moins_11_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_11_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_11_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_11_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_11_ans_menage__Y"
      
    
    
  

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

      cast('residence_annee_avant_pr' as TEXT) as champs,
      cast(  
           residence_annee_avant_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'residence_annee_avant_pr__a_l_etranger_hors_union_europeenne'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__a_l_etranger_hors_union_europeenne"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__a_l_etranger_l_union_europeenne_28_pays_membres'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__a_l_etranger_l_union_europeenne_28_pays_membres"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__dans_autre_commdepartement'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__dans_autre_commdepartement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__dans_autre_departement_region'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__dans_autre_departement_region"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__dans_autre_logement_meme_commune'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__dans_autre_logement_meme_commune"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__dans_le_meme_logement'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__dans_le_meme_logement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__hors_region_residence_actuelle_dom'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__hors_region_residence_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__hors_region_residence_actuelle_en_metropole'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__hors_region_residence_actuelle_en_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__hors_region_residence_actuelle_tom_com'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__hors_region_residence_actuelle_tom_com"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'residence_annee_avant_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "residence_annee_avant_pr__hors_residence_principale"
      
    
    
  

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

      cast('status_conjugal_pr' as TEXT) as champs,
      cast(  
           status_conjugal_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'status_conjugal_pr__celibataire'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__celibataire"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status_conjugal_pr__divorce_e'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__divorce_e"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status_conjugal_pr__en_concubinage_ou_union_libre'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__en_concubinage_ou_union_libre"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status_conjugal_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status_conjugal_pr__marie_e'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__marie_e"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status_conjugal_pr__pacse_e'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__pacse_e"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status_conjugal_pr__veuf_veuve'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status_conjugal_pr__veuf_veuve"
      
    
    
  

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

      cast('sexe_pr' as TEXT) as champs,
      cast(  
           sexe_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'sexe_pr__femmes'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "sexe_pr__femmes"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'sexe_pr__hommes'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "sexe_pr__hommes"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'sexe_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "sexe_pr__hors_residence_principale"
      
    
    
  

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

      cast('nombre_voitures_menage' as TEXT) as champs,
      cast(  
           nombre_voitures_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'nombre_voitures_menage__aucvoiture'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_voitures_menage__aucvoiture"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_voitures_menage__deux_voitures'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_voitures_menage__deux_voitures"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_voitures_menage__logement_ordinaire_inoccupe'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_voitures_menage__logement_ordinaire_inoccupe"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_voitures_menage__trois_voitures_ou_plus'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_voitures_menage__trois_voitures_ou_plus"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'nombre_voitures_menage__une_seule_voiture'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "nombre_voitures_menage__une_seule_voiture"
      
    
    
  

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

      cast('personnes_moins_17_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_17_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_17_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__24'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__24"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_17_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_17_ans_menage__Y"
      
    
    
  

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

      cast('personnes_actives_ayant_emploi_menage' as TEXT) as champs,
      cast(  
           personnes_actives_ayant_emploi_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__26'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__26"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__41'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__41"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_ayant_emploi_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_ayant_emploi_menage__Y"
      
    
    
  

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

      cast('anciennete_recherche_emploi_pr' as TEXT) as champs,
      cast(  
           anciennete_recherche_emploi_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'anciennete_recherche_emploi_pr__cherche_emploi_depuis_moins_an'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "anciennete_recherche_emploi_pr__cherche_emploi_depuis_moins_an"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'anciennete_recherche_emploi_pr__cherche_emploi_depuis_plus_an'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "anciennete_recherche_emploi_pr__cherche_emploi_depuis_plus_an"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'anciennete_recherche_emploi_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "anciennete_recherche_emploi_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'anciennete_recherche_emploi_pr__ne_recherche_pas_d_emploi'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "anciennete_recherche_emploi_pr__ne_recherche_pas_d_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'anciennete_recherche_emploi_pr__non_declare_inactif'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "anciennete_recherche_emploi_pr__non_declare_inactif"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'anciennete_recherche_emploi_pr__sans_objet_en_emploi'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "anciennete_recherche_emploi_pr__sans_objet_en_emploi"
      
    
    
  

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

      cast('personnes_masculin_menage' as TEXT) as champs,
      cast(  
           personnes_masculin_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_masculin_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__22'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__22"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__26'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__26"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__38'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__38"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_masculin_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_masculin_menage__Y"
      
    
    
  

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

      cast('personnes_moins_24_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_24_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_24_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__21'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__21"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__25'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__25"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_24_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_24_ans_menage__Y"
      
    
    
  

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

      cast('personnes_scolarisees_menage' as TEXT) as champs,
      cast(  
           personnes_scolarisees_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_scolarisees_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__25'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__25"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarisees_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarisees_menage__Y"
      
    
    
  

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

      cast('status' as TEXT) as champs,
      cast(  
           status
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'status__locataire_ou_sous_locataire_logement_loue_meuble_ou_d_chambre_d_hotel'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status__locataire_ou_sous_locataire_logement_loue_meuble_ou_d_chambre_d_hotel"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status__locataire_ou_sous_locataire_logement_loue_vihlm'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status__locataire_ou_sous_locataire_logement_loue_vihlm"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status__locataire_ou_sous_locataire_logement_loue_vinon_hlm'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status__locataire_ou_sous_locataire_logement_loue_vinon_hlm"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status__loge_gratuitement'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status__loge_gratuitement"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status__logement_ordinaire_inoccupe'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status__logement_ordinaire_inoccupe"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'status__proprietaire'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "status__proprietaire"
      
    
    
  

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

      cast('personnes_plus_75_ans_menage' as TEXT) as champs,
      cast(  
           personnes_plus_75_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_plus_75_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__48'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__48"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_75_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_75_ans_menage__Y"
      
    
    
  

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

      cast('personnes_plus_60_ans_menage' as TEXT) as champs,
      cast(  
           personnes_plus_60_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_plus_60_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__55'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__55"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_60_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_60_ans_menage__Y"
      
    
    
  

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

      cast('personnes_moins_19_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_19_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_19_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__21'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__21"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__22'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__22"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__23'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__23"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__25'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__25"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__41'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__41"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__55'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__55"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_19_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_19_ans_menage__Y"
      
    
    
  

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

      cast('condition_empoi_pr' as TEXT) as champs,
      cast(  
           condition_empoi_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'condition_empoi_pr__autres_emplois_a_duree_limitee'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__autres_emplois_a_duree_limitee"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__emplois_aides_contrat_unique_d_insertion'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__emplois_aides_contrat_unique_d_insertion"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__emplois_sans_limite_duree'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__emplois_sans_limite_duree"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__en_contrat_d_apprentissage_ou_professionnalisation'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__en_contrat_d_apprentissage_ou_professionnalisation"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__non_salaries_aides_familiaux'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__non_salaries_aides_familiaux"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__non_salaries_employeurs'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__non_salaries_employeurs"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__non_salaries_independants'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__non_salaries_independants"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__places_par_agence_d_interim'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__places_par_agence_d_interim"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__sans_objet_sans_emploi'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__sans_objet_sans_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'condition_empoi_pr__stagiaires_remuneres_en_entreprise'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "condition_empoi_pr__stagiaires_remuneres_en_entreprise"
      
    
    
  

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

      cast('personnes_moins_5_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_5_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_5_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_5_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_5_ans_menage__Y"
      
    
    
  

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

      cast('personnes_scolarise_plus_14_menage' as TEXT) as champs,
      cast(  
           personnes_scolarise_plus_14_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_scolarise_plus_14_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_scolarise_plus_14_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_scolarise_plus_14_menage__Y"
      
    
    
  

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

      cast('personnes_actives_menage' as TEXT) as champs,
      cast(  
           personnes_actives_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_actives_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__26'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__26"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__41'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__41"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_actives_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_actives_menage__Y"
      
    
    
  

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

      cast('mode_transport_plus_utilise_travail_pr' as TEXT) as champs,
      cast(  
           mode_transport_plus_utilise_travail_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__deux_roues_motorise'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__deux_roues_motorise"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__marche_a_pied_ou_rollers_patinette'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__marche_a_pied_ou_rollers_patinette"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__pas_transport'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__pas_transport"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__sans_objet'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__sans_objet"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__transports_en_commun'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__transports_en_commun"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__velo_y_compris_a_assistance_electrique'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__velo_y_compris_a_assistance_electrique"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'mode_transport_plus_utilise_travail_pr__voiture_camion_fourgonnette'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "mode_transport_plus_utilise_travail_pr__voiture_camion_fourgonnette"
      
    
    
  

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

      cast('age_regroupe_pr_menage' as TEXT) as champs,
      cast(  
           age_regroupe_pr_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'age_regroupe_pr_menage__15_a_19_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__15_a_19_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__20_a_24_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__20_a_24_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__25_a_39_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__25_a_39_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__40_a_54_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__40_a_54_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__55_a_64_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__55_a_64_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__65_a_79_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__65_a_79_ans"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__80_ans_ou_plus'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__80_ans_ou_plus"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'age_regroupe_pr_menage__moins_15_ans'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "age_regroupe_pr_menage__moins_15_ans"
      
    
    
  

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

      cast('type_activite_pr' as TEXT) as champs,
      cast(  
           type_activite_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'type_activite_pr__actifs_ayant_emploi'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__actifs_ayant_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_activite_pr__autres_inactifs'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__autres_inactifs"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_activite_pr__chomeurs'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__chomeurs"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_activite_pr__eleves'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__eleves"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_activite_pr__femmes_ou_hommes_au_foyer'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__femmes_ou_hommes_au_foyer"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_activite_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'type_activite_pr__retraites_ou_preretraites'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "type_activite_pr__retraites_ou_preretraites"
      
    
    
  

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

      cast('personnes_moins_15_ans_menage' as TEXT) as champs,
      cast(  
           personnes_moins_15_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_moins_15_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_moins_15_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_moins_15_ans_menage__Y"
      
    
    
  

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

      cast('situation_immigration_pr' as TEXT) as champs,
      cast(  
           situation_immigration_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'situation_immigration_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "situation_immigration_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'situation_immigration_pr__immigres'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "situation_immigration_pr__immigres"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'situation_immigration_pr__non_immigres'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "situation_immigration_pr__non_immigres"
      
    
    
  

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

      cast('personnes_feminin_menage' as TEXT) as champs,
      cast(  
           personnes_feminin_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_feminin_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__39'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__39"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_feminin_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_feminin_menage__Y"
      
    
    
  

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

      cast('diplome_pr' as TEXT) as champs,
      cast(  
           diplome_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'diplome_pr__aucdiplome_et_scolarite_interrompue_a_fin_primaire_ou_avant_fin_college'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__aucdiplome_et_scolarite_interrompue_a_fin_primaire_ou_avant_fin_college"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__aucdiplome_et_scolarite_jusqu_a_fin_college_ou_au_dela'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__aucdiplome_et_scolarite_jusqu_a_fin_college_ou_au_dela"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__baccalaureat_general_ou_technologique_brevet_superieur_capacite_en_droit_daeu_eseu'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__baccalaureat_general_ou_technologique_brevet_superieur_capacite_en_droit_daeu_eseu"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__baccalaureat_professionnel_brevet_professionnel_technicien_ou_d_enseignement_diplome_equivalent'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__baccalaureat_professionnel_brevet_professionnel_technicien_ou_d_enseignement_diplome_equivalent"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__bepc_brevet_elementaire_brevet_des_colleges_dnb'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__bepc_brevet_elementaire_brevet_des_colleges_dnb"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__bts_dut_deug_deust_diplome_sante_ou_social_niveau_bac+2_diplome_equivalent'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__bts_dut_deug_deust_diplome_sante_ou_social_niveau_bac+2_diplome_equivalent"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__cap_bep_ou_diplome_niveau_equivalent'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__cap_bep_ou_diplome_niveau_equivalent"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__cep_certificat_d_etudes_primaires'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__cep_certificat_d_etudes_primaires"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__doctorat_recherche_hors_sante'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__doctorat_recherche_hors_sante"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__licence_licence_pro_maitrise_diplome_equivalent_niveau_bac+3_ou_bac+4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__licence_licence_pro_maitrise_diplome_equivalent_niveau_bac+3_ou_bac+4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__master_dea_dess_diplome_granecole_niveau_bac+5_doctorat_sante'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__master_dea_dess_diplome_granecole_niveau_bac+5_doctorat_sante"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'diplome_pr__pas_scolarite_ou_arret_avant_fin_primaire'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "diplome_pr__pas_scolarite_ou_arret_avant_fin_primaire"
      
    
    
  

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

      cast('lieu_etudes_pr' as TEXT) as champs,
      cast(  
           lieu_etudes_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'lieu_etudes_pr__a_l_etranger'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__a_l_etranger"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__dans_autre_commdepartement_residence'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__dans_autre_commdepartement_residence"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__dans_autre_departement_region_residence'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__dans_autre_departement_region_residence"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__dans_commresidence_actuelle'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__dans_commresidence_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__hors_region_residence_actuelle_com'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__hors_region_residence_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__hors_region_residence_actuelle_dom'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__hors_region_residence_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__hors_region_residence_actuelle_en_metropole'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__hors_region_residence_actuelle_en_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_etudes_pr__sans_objet_pas_d_inscription_etablissement_d_enseignement'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_etudes_pr__sans_objet_pas_d_inscription_etablissement_d_enseignement"
      
    
    
  

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

      cast('lieu_naissance_pr' as TEXT) as champs,
      cast(  
           lieu_naissance_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'lieu_naissance_pr__a_l_etranger'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__a_l_etranger"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_naissance_pr__dans_autre_departement_region_residence_actuelle'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__dans_autre_departement_region_residence_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_naissance_pr__dans_le_departement_residence_actuelle'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__dans_le_departement_residence_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_naissance_pr__hors_region_residence_actuelle_dom'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__hors_region_residence_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_naissance_pr__hors_region_residence_actuelle_en_metropole'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__hors_region_residence_actuelle_en_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_naissance_pr__hors_region_residence_actuelle_tom_com'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__hors_region_residence_actuelle_tom_com"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_naissance_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_naissance_pr__hors_residence_principale"
      
    
    
  

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

      cast('lieu_travail_pr' as TEXT) as champs,
      cast(  
           lieu_travail_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'lieu_travail_pr__a_l_etranger'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__a_l_etranger"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__dans_autre_commdepartement_residence'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__dans_autre_commdepartement_residence"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__dans_autre_departement_region_residence'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__dans_autre_departement_region_residence"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__dans_commresidence_actuelle'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__dans_commresidence_actuelle"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__hors_region_residence_actuelle_com'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__hors_region_residence_actuelle_com"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__hors_region_residence_actuelle_dom'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__hors_region_residence_actuelle_dom"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__hors_region_residence_actuelle_en_metropole'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__hors_region_residence_actuelle_en_metropole"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'lieu_travail_pr__sans_objet_sans_emploi'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "lieu_travail_pr__sans_objet_sans_emploi"
      
    
    
  

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

      cast('personnes_plus_65_ans_menage' as TEXT) as champs,
      cast(  
           personnes_plus_65_ans_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_plus_65_ans_menage__0'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__0"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__55'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__55"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_plus_65_ans_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_plus_65_ans_menage__Y"
      
    
    
  

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

      cast('temps_travail_pr' as TEXT) as champs,
      cast(  
           temps_travail_pr
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'temps_travail_pr__hors_residence_principale'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "temps_travail_pr__hors_residence_principale"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'temps_travail_pr__sans_objet_sans_emploi'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "temps_travail_pr__sans_objet_sans_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'temps_travail_pr__temps_complet'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "temps_travail_pr__temps_complet"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'temps_travail_pr__temps_partiel'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "temps_travail_pr__temps_partiel"
      
    
    
  

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

      cast('personnes_menage' as TEXT) as champs,
      cast(  
           personnes_menage
             
           as varchar) as valeur

    from "defaultdb"."prep"."decoder_demographie"

    

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
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_commune_champs_valeur
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
      when champs__valeur = 'personnes_menage__1'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__1"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__10'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__10"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__11'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__11"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__12'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__12"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__13'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__13"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__14'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__14"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__15'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__15"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__16'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__16"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__17'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__17"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__18'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__18"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__19'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__19"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__2'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__2"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__20'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__20"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__21'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__21"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__22'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__22"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__23'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__23"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__24'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__24"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__25'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__25"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__26'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__26"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__28'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__28"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__3'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__3"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__34'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__34"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__4'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__4"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__41'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__41"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__5'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__5"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__55'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__55"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__6'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__6"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__7'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__7"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__8'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__8"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__9'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__9"
      
    
    ,
  
    sum(
      
      case
      when champs__valeur = 'personnes_menage__Y'
        then population_par_commune_champs_valeur
      else 0
      end
    )
    
      
            as "personnes_menage__Y"
      
    
    
  

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
	    "defaultdb"."prep"."geo_communes" as cog
    ON
      aggregated.code_commune_insee = cog.code_commune
  )

SELECT 
    *  
FROM
    aggregated_with_cog