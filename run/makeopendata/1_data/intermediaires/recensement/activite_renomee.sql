
  create view "defaultdb"."intermediaires"."activite_renomee__dbt_tmp"
    
    
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
        
            CASE "DIPLM"
                
                    when '1' then 'pr_scolarite_avant_primaire'
                
                    when '11' then 'pr_scolarite_CEP'
                
                    when '12' then 'pr_scolarite_BEPC'
                
                    when '13' then 'pr_scolarite_CAP_BEP'
                
                    when '14' then 'pr_scolarite_bac_general_techno'
                
                    when '15' then 'pr_scolarite_bar_pr'
                
                    when '16' then 'pr_scolarite_bac_plus_2'
                
                    when '17' then 'pr_scolarite_bac_plus_3_ou_4'
                
                    when '18' then 'pr_scolarite_bac_plus_5'
                
                    when '19' then 'pr_scolarite_bac_plus_8'
                
                    when '2' then 'pr_scolarite_avant_college'
                
                    when '3' then 'pr_scolarite_fin_college'
                
            END AS "DIPLM",
        
            CASE "EMPLM"
                
                    when '11' then 'pr_emploi_apprentissage'
                
                    when '12' then 'pr_emploi_interim'
                
                    when '13' then 'pr_emploi_contrat_aide'
                
                    when '14' then 'pr_emploi_stage_entreprise'
                
                    when '15' then 'pr_emploi_duree_limite'
                
                    when '16' then 'pr_emploi_sans_duree_limite'
                
                    when '21' then 'pr_emploi_independant'
                
                    when '22' then 'pr_emploi_employeur'
                
                    when '23' then 'pr_emploi_aides_familiaux'
                
                    when 'ZZ' then 'pr_emploi_sans_objet'
                
            END AS "EMPLM",
        
            CASE "INEEM"
                
                    when '0' then 'menages_avec_0_eleve_etudiant_14_ans_et_plus'
                
                    when '1' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '10' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '11' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '12' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '2' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '3' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '4' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '5' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '6' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '7' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '8' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when '9' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
            END AS "INEEM",
        
            CASE "INPAM"
                
                    when '0' then 'menages_avec_0_personne_active'
                
                    when '1' then 'menages_avec_1_personne_active'
                
                    when '10' then 'menages_avec_1_personne_active'
                
                    when '11' then 'menages_avec_1_personne_active'
                
                    when '12' then 'menages_avec_1_personne_active'
                
                    when '13' then 'menages_avec_1_personne_active'
                
                    when '14' then 'menages_avec_1_personne_active'
                
                    when '15' then 'menages_avec_1_personne_active'
                
                    when '16' then 'menages_avec_1_personne_active'
                
                    when '17' then 'menages_avec_1_personne_active'
                
                    when '18' then 'menages_avec_1_personne_active'
                
                    when '19' then 'menages_avec_1_personne_active'
                
                    when '2' then 'menages_avec_2_et_plus_personnes_actives'
                
                    when '20' then 'menages_avec_1_personne_active'
                
                    when '26' then 'menages_avec_1_personne_active'
                
                    when '3' then 'menages_avec_1_personne_active'
                
                    when '4' then 'menages_avec_1_personne_active'
                
                    when '41' then 'menages_avec_1_personne_active'
                
                    when '5' then 'menages_avec_1_personne_active'
                
                    when '6' then 'menages_avec_1_personne_active'
                
                    when '7' then 'menages_avec_1_personne_active'
                
                    when '8' then 'menages_avec_1_personne_active'
                
                    when '9' then 'menages_avec_1_personne_active'
                
            END AS "INPAM",
        
            CASE "INPOM"
                
                    when '0' then 'menages_avec_0_personne_active_avec_emploi'
                
                    when '1' then 'menages_avec_1_personne_active_avec_emploi'
                
                    when '10' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '11' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '12' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '13' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '14' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '15' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '16' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '17' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '18' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '19' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '2' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '20' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '26' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '3' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '4' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '41' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '5' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '6' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '7' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '8' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when '9' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
            END AS "INPOM",
        
            CASE "INPSM"
                
                    when '0' then 'menages_avec_0_personne_scolarisee'
                
                    when '1' then 'menages_avec_1_personne_scolarisee'
                
                    when '10' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '11' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '12' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '13' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '14' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '15' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '16' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '2' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '25' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '3' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '4' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '5' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '6' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '7' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '8' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when '9' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
            END AS "INPSM",
        
            CASE "RECHM"
                
                    when '0' then 'menages_pr_pas_de_recherche_emploi'
                
                    when '1' then 'menages_pr_recherche_emploi_moins_un_an'
                
                    when '2' then 'menages_pr_recherche_emploi_plus_un_an'
                
                    when '9' then 'menages_pr_recherche_emploi_non_declaree'
                
                    when 'Z' then 'menages_pr_recherche_emploi_sans_objet_en_emploi'
                
            END AS "RECHM",
        
            CASE "TACTM"
                
                    when '11' then 'menages_pr_activite_emploi'
                
                    when '12' then 'menages_pr_activite_chomeurs'
                
                    when '21' then 'menages_pr_activite_retraite_pre_retraite'
                
                    when '22' then 'menages_pr_activite_eleve'
                
                    when '24' then 'menages_pr_activite_au_foyer'
                
                    when '25' then 'menages_pr_activite_autre'
                
            END AS "TACTM",
        
            CASE "TPM"
                
                    when '1' then 'menages_pr_travail_temps_complet'
                
                    when '2' then 'menages_pr_travail_temps_partiel'
                
                    when 'Z' then 'menages_pr_travail_temps_sans_objet_sans_emploi'
                
            END AS "TPM",
        
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