
  create view "defaultdb"."intermediaires"."mobilite_renomee__dbt_tmp"
    
    
  as (
    

WITH logement AS (
    select * 
    
        from "defaultdb"."sources"."logement_2020"
    
),
logement_renomee AS (
    (     

    

    
    
    


    select 
        CASE
            WHEN "ARM" != 'ZZZZZ' THEN "ARM"
            ELSE "COMMUNE" 
        END AS code_commune_insee,
        "CATL",
        CASE 
		    WHEN "IRIS" = 'ZZZZZZZZZ' THEN CONCAT("COMMUNE", '0000')
		    ELSE "IRIS"
	    END as code_iris,
        
            CASE
                
                    when LPAD(CAST("DEROU" AS TEXT), 3, '0') = '000' then 'dom__aucun_deux_roues'
                
                    when LPAD(CAST("DEROU" AS TEXT), 3, '0') = '001' then 'dom__un_deux_roues'
                
                    when LPAD(CAST("DEROU" AS TEXT), 3, '0') = '002' then 'dom__deux_deux_roues'
                
                    when LPAD(CAST("DEROU" AS TEXT), 3, '0') = '003' then 'dom__trois_ou_plus_deux_roues'
                
            END AS "DEROU",
        
            CASE
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '001' then 'pr_etudes_commune_actuelle'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '002' then 'pr_etudes_commune_departement_actuel'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '003' then 'pr_etudes_department_region_actuel'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '004' then 'pr_etudes_hors_region_actuelle_metropole'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '005' then 'pr_etudes_hors_region_actuelle_dom'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '006' then 'pr_etudes_hors_region_actuelle_com'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '007' then 'pr_etudes_etranger'
                
                    when LPAD(CAST("ILETUDM" AS TEXT), 3, '0') = '00Z' then 'pr_etudes_sans_objet_non_inscrit'
                
            END AS "ILETUDM",
        
            CASE
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '001' then 'pr_travail_commune_actuelle'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '002' then 'pr_travail_commune_departement_actuel'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '003' then 'pr_travail_department_region_actuel'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '004' then 'pr_travail_hors_region_actuelle_metropole'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '005' then 'pr_travail_hors_region_actuelle_dom'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '006' then 'pr_travail_hors_region_actuelle_com'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '007' then 'pr_travail_etranger'
                
                    when LPAD(CAST("ILTM" AS TEXT), 3, '0') = '00Z' then 'pr_travail_sans_objet_sans_emploi'
                
            END AS "ILTM",
        
            CASE
                
                    when LPAD(CAST("IMMIM" AS TEXT), 3, '0') = '001' then 'pr_immigration_immigre'
                
                    when LPAD(CAST("IMMIM" AS TEXT), 3, '0') = '002' then 'pr_immigration_non_immigre'
                
            END AS "IMMIM",
        
            CASE
                
                    when LPAD(CAST("INAIM" AS TEXT), 3, '0') = '001' then 'pr_naissance_department_actuelle'
                
                    when LPAD(CAST("INAIM" AS TEXT), 3, '0') = '002' then 'pr_naissance_department_region_actuelle'
                
                    when LPAD(CAST("INAIM" AS TEXT), 3, '0') = '003' then 'pr_naissance_hors_region_actuelle_metropole'
                
                    when LPAD(CAST("INAIM" AS TEXT), 3, '0') = '004' then 'pr_naissance_hors_region_actuelle_dom'
                
                    when LPAD(CAST("INAIM" AS TEXT), 3, '0') = '005' then 'pr_naissance_hors_region_actuelle_com'
                
                    when LPAD(CAST("INAIM" AS TEXT), 3, '0') = '006' then 'pr_naissance_etranger'
                
            END AS "INAIM",
        
            CASE
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '001' then 'menages_avec_pr_annee_avant_meme_logement'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '002' then 'menages_avec_pr_annee_avant_meme_commune'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '003' then 'menages_avec_pr_annee_avant_meme_departement'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '004' then 'menages_avec_pr_annee_avant_meme_region'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '005' then 'menages_avec_pr_annee_avant_autre_region_metro'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '006' then 'menages_avec_pr_annee_avant_autre_region_dom'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '007' then 'menages_avec_pr_annee_avant_autre_region_com'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '008' then 'menages_avec_pr_annee_avant_union_europeenne'
                
                    when LPAD(CAST("IRANM" AS TEXT), 3, '0') = '009' then 'menages_avec_pr_annee_avant_etranger'
                
            END AS "IRANM",
        
            CASE
                
                    when LPAD(CAST("TRANSM" AS TEXT), 3, '0') = '002' then 'menages_pr_transport_travail_pieds'
                
                    when LPAD(CAST("TRANSM" AS TEXT), 3, '0') = '003' then 'menages_pr_transport_travail_velo'
                
                    when LPAD(CAST("TRANSM" AS TEXT), 3, '0') = '004' then 'menages_pr_transport_travail_deux_roues'
                
                    when LPAD(CAST("TRANSM" AS TEXT), 3, '0') = '005' then 'menages_pr_transport_travail_voiture'
                
                    when LPAD(CAST("TRANSM" AS TEXT), 3, '0') = '006' then 'menages_pr_transport_travail_transport_commune'
                
            END AS "TRANSM",
        
            CASE
                
                    when LPAD(CAST("VOIT" AS TEXT), 3, '0') = '000' then 'menages_0_voiture'
                
                    when LPAD(CAST("VOIT" AS TEXT), 3, '0') = '001' then 'menages_1_voiture'
                
                    when LPAD(CAST("VOIT" AS TEXT), 3, '0') = '002' then 'menages_2_voitures'
                
                    when LPAD(CAST("VOIT" AS TEXT), 3, '0') = '003' then 'menages_3_et_plus_voitures'
                
            END AS "VOIT",
        
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