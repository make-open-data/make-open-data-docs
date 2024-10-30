
  create view "defaultdb"."intermediaires"."activite_renomee__dbt_tmp"
    
    
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
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '001' then 'pr_scolarite_avant_primaire'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '002' then 'pr_scolarite_avant_college'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '003' then 'pr_scolarite_fin_college'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '011' then 'pr_scolarite_CEP'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '012' then 'pr_scolarite_BEPC'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '013' then 'pr_scolarite_CAP_BEP'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '014' then 'pr_scolarite_bac_general_techno'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '015' then 'pr_scolarite_bar_pr'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '016' then 'pr_scolarite_bac_plus_2'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '017' then 'pr_scolarite_bac_plus_3_ou_4'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '018' then 'pr_scolarite_bac_plus_5'
                
                    when LPAD(CAST("DIPLM" AS TEXT), 3, '0') = '019' then 'pr_scolarite_bac_plus_8'
                
            END AS "DIPLM",
        
            CASE
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '011' then 'pr_emploi_apprentissage'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '012' then 'pr_emploi_interim'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '013' then 'pr_emploi_contrat_aide'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '014' then 'pr_emploi_stage_entreprise'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '015' then 'pr_emploi_duree_limite'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '016' then 'pr_emploi_sans_duree_limite'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '021' then 'pr_emploi_independant'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '022' then 'pr_emploi_employeur'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '023' then 'pr_emploi_aides_familiaux'
                
                    when LPAD(CAST("EMPLM" AS TEXT), 3, '0') = '0ZZ' then 'pr_emploi_sans_objet'
                
            END AS "EMPLM",
        
            CASE
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '000' then 'menages_avec_0_eleve_etudiant_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
                    when LPAD(CAST("INEEM" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
                
            END AS "INEEM",
        
            CASE
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '001' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '002' then 'menages_avec_2_et_plus_personnes_actives'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '003' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '004' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '005' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '006' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '007' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '008' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '009' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '010' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '011' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '012' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '013' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '014' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '015' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '016' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '017' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '018' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '019' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '020' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '026' then 'menages_avec_1_personne_active'
                
                    when LPAD(CAST("INPAM" AS TEXT), 3, '0') = '041' then 'menages_avec_1_personne_active'
                
            END AS "INPAM",
        
            CASE
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_active_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '001' then 'menages_avec_1_personne_active_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '002' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '003' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '004' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '005' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '006' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '007' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '008' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '009' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '010' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '011' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '012' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '013' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '014' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '015' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '016' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '017' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '018' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '019' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '020' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '026' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
                    when LPAD(CAST("INPOM" AS TEXT), 3, '0') = '041' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
                
            END AS "INPOM",
        
            CASE
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_scolarisee'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '001' then 'menages_avec_1_personne_scolarisee'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '002' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '003' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '004' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '005' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '006' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '007' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '008' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '009' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '010' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '011' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '012' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '013' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '014' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '015' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '016' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
                    when LPAD(CAST("INPSM" AS TEXT), 3, '0') = '025' then 'menages_avec_2_et_plus_personnes_scolarisees'
                
            END AS "INPSM",
        
            CASE
                
                    when LPAD(CAST("RECHM" AS TEXT), 3, '0') = '000' then 'menages_pr_pas_de_recherche_emploi'
                
                    when LPAD(CAST("RECHM" AS TEXT), 3, '0') = '001' then 'menages_pr_recherche_emploi_moins_un_an'
                
                    when LPAD(CAST("RECHM" AS TEXT), 3, '0') = '002' then 'menages_pr_recherche_emploi_plus_un_an'
                
                    when LPAD(CAST("RECHM" AS TEXT), 3, '0') = '009' then 'menages_pr_recherche_emploi_non_declaree'
                
                    when LPAD(CAST("RECHM" AS TEXT), 3, '0') = '00Z' then 'menages_pr_recherche_emploi_sans_objet_en_emploi'
                
            END AS "RECHM",
        
            CASE
                
                    when LPAD(CAST("TACTM" AS TEXT), 3, '0') = '011' then 'menages_pr_activite_emploi'
                
                    when LPAD(CAST("TACTM" AS TEXT), 3, '0') = '012' then 'menages_pr_activite_chomeurs'
                
                    when LPAD(CAST("TACTM" AS TEXT), 3, '0') = '021' then 'menages_pr_activite_retraite_pre_retraite'
                
                    when LPAD(CAST("TACTM" AS TEXT), 3, '0') = '022' then 'menages_pr_activite_eleve'
                
                    when LPAD(CAST("TACTM" AS TEXT), 3, '0') = '024' then 'menages_pr_activite_au_foyer'
                
                    when LPAD(CAST("TACTM" AS TEXT), 3, '0') = '025' then 'menages_pr_activite_autre'
                
            END AS "TACTM",
        
            CASE
                
                    when LPAD(CAST("TPM" AS TEXT), 3, '0') = '001' then 'menages_pr_travail_temps_complet'
                
                    when LPAD(CAST("TPM" AS TEXT), 3, '0') = '002' then 'menages_pr_travail_temps_partiel'
                
                    when LPAD(CAST("TPM" AS TEXT), 3, '0') = '00Z' then 'menages_pr_travail_temps_sans_objet_sans_emploi'
                
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