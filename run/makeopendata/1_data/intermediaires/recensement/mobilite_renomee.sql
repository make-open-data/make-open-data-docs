
  create view "defaultdb"."intermediaires"."mobilite_renomee__dbt_tmp"
    
    
  as (
    

WITH logement AS (
    select * 
    
        from "defaultdb"."sources"."logement_2020"
    
),
logement_renomee AS (
    (     

    

    
    
    


    select 

        "COMMUNE" as code_commune_insee,
        CASE 
		    WHEN "IRIS" = 'ZZZZZZZZZ' THEN CONCAT("COMMUNE", '0000')
		    ELSE "IRIS"
	    END as code_iris,
        
            CASE "INAIM"
                
                    when '4' then 'pr_naissance_hors_region_actuelle_dom'
                
                    when '5' then 'pr_naissance_hors_region_actuelle_com'
                
                    when '2' then 'pr_naissance_department_region_actuelle'
                
                    when '3' then 'pr_naissance_hors_region_actuelle_metropole'
                
                    when '1' then 'pr_naissance_department_actuelle'
                
                    when '6' then 'pr_naissance_etranger'
                
            END AS "INAIM",
        
            CASE "IRANM"
                
                    when '8' then 'menages_avec_pr_annee_avant_union_europeenne'
                
                    when '7' then 'menages_avec_pr_annee_avant_autre_region_com'
                
                    when '4' then 'menages_avec_pr_annee_avant_meme_region'
                
                    when '5' then 'menages_avec_pr_annee_avant_autre_region_metro'
                
                    when '6' then 'menages_avec_pr_annee_avant_autre_region_dom'
                
                    when '1' then 'menages_avec_pr_annee_avant_meme_logement'
                
                    when '9' then 'menages_avec_pr_annee_avant_etranger'
                
                    when '2' then 'menages_avec_pr_annee_avant_meme_commune'
                
                    when '3' then 'menages_avec_pr_annee_avant_meme_departement'
                
            END AS "IRANM",
        
            CASE "IMMIM"
                
                    when '2' then 'pr_immigration_non_immigre'
                
                    when '1' then 'pr_immigration_immigre'
                
            END AS "IMMIM",
        
            CASE "ILETUDM"
                
                    when '3' then 'pr_etudes_department_region_actuel'
                
                    when '4' then 'pr_etudes_hors_region_actuelle_metropole'
                
                    when '2' then 'pr_etudes_commune_departement_actuel'
                
                    when 'Z' then 'pr_etudes_sans_objet_non_inscrit'
                
                    when '6' then 'pr_etudes_hors_region_actuelle_com'
                
                    when '1' then 'pr_etudes_commune_actuelle'
                
                    when '5' then 'pr_etudes_hors_region_actuelle_dom'
                
                    when '7' then 'pr_etudes_etranger'
                
            END AS "ILETUDM",
        
            CASE "VOIT"
                
                    when '3' then 'menages_3_et_plus_voitures'
                
                    when '0' then 'menages_0_voiture'
                
                    when '2' then 'menages_2_voitures'
                
                    when '1' then 'menages_1_voiture'
                
            END AS "VOIT",
        
            CASE "ILTM"
                
                    when '4' then 'pr_travail_hors_region_actuelle_metropole'
                
                    when 'Z' then 'pr_travail_sans_objet_sans_emploi'
                
                    when '5' then 'pr_travail_hors_region_actuelle_dom'
                
                    when '7' then 'pr_travail_etranger'
                
                    when '6' then 'pr_travail_hors_region_actuelle_com'
                
                    when '2' then 'pr_travail_commune_departement_actuel'
                
                    when '3' then 'pr_travail_department_region_actuel'
                
                    when '1' then 'pr_travail_commune_actuelle'
                
            END AS "ILTM",
        
            CASE "TRANSM"
                
                    when '4' then 'menages_pr_transport_travail_deux_roues'
                
                    when '5' then 'menages_pr_transport_travail_voiture'
                
                    when '6' then 'menages_pr_transport_travail_transport_commune'
                
                    when '3' then 'menages_pr_transport_travail_velo'
                
                    when '2' then 'menages_pr_transport_travail_pieds'
                
            END AS "TRANSM",
        
            CASE "DEROU"
                
                    when '3' then 'dom__trois_ou_plus_deux_roues'
                
                    when '1' then 'dom__un_deux_roues'
                
                    when '2' then 'dom__deux_deux_roues'
                
                    when '0' then 'dom__aucun_deux_roues'
                
            END AS "DEROU",
        
        CAST(CAST("IPONDL" AS NUMERIC) AS INT) AS poids_du_logement
    
    FROM 
        logement

 )
)

SELECT 
    *
FROM 
    logement_renomee
  );