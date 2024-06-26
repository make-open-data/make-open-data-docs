
  create view "defaultdb"."prep"."decoder_demographie__dbt_tmp"
    
    
  as (
    

WITH logement AS (
    select * from "defaultdb"."sources"."logement_2020"
),
decode_logement AS (
    
  
  

  

  

  

  SELECT
    
      
      
      
      
            
      
        "INP65M" as "personnes_plus_65_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP24M" as "personnes_moins_24_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TPM"
          
            WHEN '1' THEN 'temps_complet'
          
            WHEN '2' THEN 'temps_partiel'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'sans_objet_sans_emploi'
          
        END as "temps_travail_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TACTM"
          
            WHEN '11' THEN 'actifs_ayant_emploi'
          
            WHEN '12' THEN 'chomeurs'
          
            WHEN '21' THEN 'retraites_ou_preretraites'
          
            WHEN '22' THEN 'eleves'
          
            WHEN '24' THEN 'femmes_ou_hommes_au_foyer'
          
            WHEN '25' THEN 'autres_inactifs'
          
            WHEN 'YY' THEN 'hors_residence_principale'
          
        END as "type_activite_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "DIPLM"
          
            WHEN '01' THEN 'pas_scolarite_ou_arret_avant_fin_primaire'
          
            WHEN '02' THEN 'aucdiplome_et_scolarite_interrompue_a_fin_primaire_ou_avant_fin_college'
          
            WHEN '03' THEN 'aucdiplome_et_scolarite_jusqu_a_fin_college_ou_au_dela'
          
            WHEN '11' THEN 'cep_certificat_d_etudes_primaires'
          
            WHEN '12' THEN 'bepc_brevet_elementaire_brevet_des_colleges_dnb'
          
            WHEN '13' THEN 'cap_bep_ou_diplome_niveau_equivalent'
          
            WHEN '14' THEN 'baccalaureat_general_ou_technologique_brevet_superieur_capacite_en_droit_daeu_eseu'
          
            WHEN '15' THEN 'baccalaureat_professionnel_brevet_professionnel_technicien_ou_d_enseignement_diplome_equivalent'
          
            WHEN '16' THEN 'bts_dut_deug_deust_diplome_sante_ou_social_niveau_bac+2_diplome_equivalent'
          
            WHEN '17' THEN 'licence_licence_pro_maitrise_diplome_equivalent_niveau_bac+3_ou_bac+4'
          
            WHEN '18' THEN 'master_dea_dess_diplome_granecole_niveau_bac+5_doctorat_sante'
          
            WHEN '19' THEN 'doctorat_recherche_hors_sante'
          
            WHEN 'YY' THEN 'hors_residence_principale'
          
        END as "diplome_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "INAIM"
          
            WHEN '1' THEN 'dans_le_departement_residence_actuelle'
          
            WHEN '2' THEN 'dans_autre_departement_region_residence_actuelle'
          
            WHEN '3' THEN 'hors_region_residence_actuelle_en_metropole'
          
            WHEN '4' THEN 'hors_region_residence_actuelle_dom'
          
            WHEN '5' THEN 'hors_region_residence_actuelle_tom_com'
          
            WHEN '6' THEN 'a_l_etranger'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "lieu_naissance_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SEXEM"
          
            WHEN '1' THEN 'hommes'
          
            WHEN '2' THEN 'femmes'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "sexe_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPER2" as "personnes_feminin_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP19M" as "personnes_moins_19_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "EMPLM"
          
            WHEN '11' THEN 'en_contrat_d_apprentissage_ou_professionnalisation'
          
            WHEN '12' THEN 'places_par_agence_d_interim'
          
            WHEN '13' THEN 'emplois_aides_contrat_unique_d_insertion'
          
            WHEN '14' THEN 'stagiaires_remuneres_en_entreprise'
          
            WHEN '15' THEN 'autres_emplois_a_duree_limitee'
          
            WHEN '16' THEN 'emplois_sans_limite_duree'
          
            WHEN '21' THEN 'non_salaries_independants'
          
            WHEN '22' THEN 'non_salaries_employeurs'
          
            WHEN '23' THEN 'non_salaries_aides_familiaux'
          
            WHEN 'YY' THEN 'hors_residence_principale'
          
            WHEN 'ZZ' THEN 'sans_objet_sans_emploi'
          
        END as "condition_empoi_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "RECHM"
          
            WHEN '0' THEN 'ne_recherche_pas_d_emploi'
          
            WHEN '1' THEN 'cherche_emploi_depuis_moins_an'
          
            WHEN '2' THEN 'cherche_emploi_depuis_plus_an'
          
            WHEN '9' THEN 'non_declare_inactif'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'sans_objet_en_emploi'
          
        END as "anciennete_recherche_emploi_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TRANSM"
          
            WHEN '1' THEN 'pas_transport'
          
            WHEN '2' THEN 'marche_a_pied_ou_rollers_patinette'
          
            WHEN '3' THEN 'velo_y_compris_a_assistance_electrique'
          
            WHEN '4' THEN 'deux_roues_motorise'
          
            WHEN '5' THEN 'voiture_camion_fourgonnette'
          
            WHEN '6' THEN 'transports_en_commun'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'sans_objet'
          
        END as "mode_transport_plus_utilise_travail_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "IPONDL" as "poids_du_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPER1" as "personnes_masculin_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "STAT_CONJM"
          
            WHEN '1' THEN 'marie_e'
          
            WHEN '2' THEN 'pacse_e'
          
            WHEN '3' THEN 'en_concubinage_ou_union_libre'
          
            WHEN '4' THEN 'veuf_veuve'
          
            WHEN '5' THEN 'divorce_e'
          
            WHEN '6' THEN 'celibataire'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "status_conjugal_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP60M" as "personnes_plus_60_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP75M" as "personnes_plus_75_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ILETUDM"
          
            WHEN '1' THEN 'dans_commresidence_actuelle'
          
            WHEN '2' THEN 'dans_autre_commdepartement_residence'
          
            WHEN '3' THEN 'dans_autre_departement_region_residence'
          
            WHEN '4' THEN 'hors_region_residence_actuelle_en_metropole'
          
            WHEN '5' THEN 'hors_region_residence_actuelle_dom'
          
            WHEN '6' THEN 'hors_region_residence_actuelle_com'
          
            WHEN '7' THEN 'a_l_etranger'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'sans_objet_pas_d_inscription_etablissement_d_enseignement'
          
        END as "lieu_etudes_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPER" as "personnes_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "VOIT"
          
            WHEN '0' THEN 'aucvoiture'
          
            WHEN '1' THEN 'une_seule_voiture'
          
            WHEN '2' THEN 'deux_voitures'
          
            WHEN '3' THEN 'trois_voitures_ou_plus'
          
            WHEN 'X' THEN 'logement_ordinaire_inoccupe'
          
        END as "nombre_voitures_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "COMMUNE" as "code_commune_insee"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPAM" as "personnes_actives_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "STOCD"
          
            WHEN '00' THEN 'logement_ordinaire_inoccupe'
          
            WHEN '10' THEN 'proprietaire'
          
            WHEN '21' THEN 'locataire_ou_sous_locataire_logement_loue_vinon_hlm'
          
            WHEN '22' THEN 'locataire_ou_sous_locataire_logement_loue_vihlm'
          
            WHEN '23' THEN 'locataire_ou_sous_locataire_logement_loue_meuble_ou_d_chambre_d_hotel'
          
            WHEN '30' THEN 'loge_gratuitement'
          
        END as "status"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "IMMIM"
          
            WHEN '1' THEN 'immigres'
          
            WHEN '2' THEN 'non_immigres'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "situation_immigration_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "AGEMEN8"
          
            WHEN '00' THEN 'moins_15_ans'
          
            WHEN '15' THEN '15_a_19_ans'
          
            WHEN '20' THEN '20_a_24_ans'
          
            WHEN '25' THEN '25_a_39_ans'
          
            WHEN '40' THEN '40_a_54_ans'
          
            WHEN '55' THEN '55_a_64_ans'
          
            WHEN '65' THEN '65_a_79_ans'
          
            WHEN '80' THEN '80_ans_ou_plus'
          
            WHEN 'YY' THEN 'hors_residence_principale'
          
        END as "age_regroupe_pr_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP3M" as "personnes_moins_3_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "REGION"
          
            WHEN '01' THEN 'guadeloupe'
          
            WHEN '02' THEN 'martinique'
          
            WHEN '03' THEN 'guyane'
          
            WHEN '04' THEN 'la_reunion'
          
            WHEN '11' THEN 'ile_france'
          
            WHEN '24' THEN 'centre_val_loire'
          
            WHEN '27' THEN 'bourgogne_franche_comte'
          
            WHEN '28' THEN 'normandie'
          
            WHEN '32' THEN 'hauts_france'
          
            WHEN '44' THEN 'grand_est'
          
            WHEN '52' THEN 'pays_loire'
          
            WHEN '53' THEN 'bretagne'
          
            WHEN '75' THEN 'nouvelle_aquitaine'
          
            WHEN '76' THEN 'occitanie'
          
            WHEN '84' THEN 'auvergne_rhone_alpes'
          
            WHEN '93' THEN 'provence_alpes_cote_d_azur'
          
            WHEN '94' THEN 'corse'
          
        END as "region_residence"
      
      
      
      , 
    
      
      
      
      
            
      
        "INEEM" as "personnes_scolarise_plus_14_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP17M" as "personnes_moins_17_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP5M" as "personnes_moins_5_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP15M" as "personnes_moins_15_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP11M" as "personnes_moins_11_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPOM" as "personnes_actives_ayant_emploi_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ILTM"
          
            WHEN '1' THEN 'dans_commresidence_actuelle'
          
            WHEN '2' THEN 'dans_autre_commdepartement_residence'
          
            WHEN '3' THEN 'dans_autre_departement_region_residence'
          
            WHEN '4' THEN 'hors_region_residence_actuelle_en_metropole'
          
            WHEN '5' THEN 'hors_region_residence_actuelle_dom'
          
            WHEN '6' THEN 'hors_region_residence_actuelle_com'
          
            WHEN '7' THEN 'a_l_etranger'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'sans_objet_sans_emploi'
          
        END as "lieu_travail_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "IRANM"
          
            WHEN '1' THEN 'dans_le_meme_logement'
          
            WHEN '2' THEN 'dans_autre_logement_meme_commune'
          
            WHEN '3' THEN 'dans_autre_commdepartement'
          
            WHEN '4' THEN 'dans_autre_departement_region'
          
            WHEN '5' THEN 'hors_region_residence_actuelle_en_metropole'
          
            WHEN '6' THEN 'hors_region_residence_actuelle_dom'
          
            WHEN '7' THEN 'hors_region_residence_actuelle_tom_com'
          
            WHEN '8' THEN 'a_l_etranger_l_union_europeenne_28_pays_membres'
          
            WHEN '9' THEN 'a_l_etranger_hors_union_europeenne'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "residence_annee_avant_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPSM" as "personnes_scolarisees_menage"
      
      
      
      
    
  FROM logement

)

SELECT 
    *
FROM 
    decode_logement
  );