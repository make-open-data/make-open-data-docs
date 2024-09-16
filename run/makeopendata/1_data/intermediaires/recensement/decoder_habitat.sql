
  create view "defaultdb"."intermediaires"."decoder_habitat__dbt_tmp"
    
    
  as (
    

WITH logement AS (
    select * from "defaultdb"."sources"."logement_2020"
),
decode_logement AS (
    
  
  

  

  

  

  SELECT
    
      
      
      
      
            
      
        CASE "CMBL"
          
            WHEN '1' THEN 'chauffage_urbain'
          
            WHEN '2' THEN 'gaz_ville_ou_reseau'
          
            WHEN '3' THEN 'fioul_mazout'
          
            WHEN '4' THEN 'electricite'
          
            WHEN '5' THEN 'gaz_en_bouteilles_ou_en_citerne'
          
            WHEN '6' THEN 'autre'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_dom'
          
        END as "combustible_principal_logement__metro"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ASCEN"
          
            WHEN '1' THEN 'avec_ascenseur'
          
            WHEN '2' THEN 'sans_ascenseur'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "desserte_ascensseur"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "GARL"
          
            WHEN '1' THEN 'avec_emplacement_s_stationnement'
          
            WHEN '2' THEN 'sans_emplacement_stationnement'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "emplacement_stationnement_reserve"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CHFL"
          
            WHEN '1' THEN 'chauffage_central_collectif_y_compris_chauffage_urbain'
          
            WHEN '2' THEN 'chauffage_central_individuel_avec_chaudiere_propre_au_logement'
          
            WHEN '3' THEN 'chauffage_tout_electrique'
          
            WHEN '4' THEN 'autre_moyen_chauffage'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_dom'
          
        END as "chauffage_central_logement__metro"
      
      
      
      , 
    
      
      
      
      
            
      
        "IRIS" as "code_iris_incomplet"
      
      
      
      , 
    
      
      
      
      
            
      
        "IPONDL" as "poids_du_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "HLML"
          
            WHEN '1' THEN 'logement_appartenant_a_organisme_hlm'
          
            WHEN '2' THEN 'logement_n_appartenant_pas_a_organisme_hlm'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "logement_hml"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "EGOUL"
          
            WHEN '1' THEN 'raccordement_au_reseau_d_egouts'
          
            WHEN '2' THEN 'raccordement_a_fosse_septique'
          
            WHEN '3' THEN 'raccordement_a_puisard'
          
            WHEN '4' THEN 'evacuation_des_eaux_usees_a_meme_le_sol'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "mode_evacuation_eaux_usagers__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CATL"
          
            WHEN '1' THEN 'residences_principales'
          
            WHEN '2' THEN 'logements_occasionnels'
          
            WHEN '3' THEN 'residences_secondaires'
          
            WHEN '4' THEN 'logements_vacants'
          
        END as "categorie_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ANEMR"
          
            WHEN '00' THEN 'moins_2_ans'
          
            WHEN '01' THEN 'de_2_a_4_ans'
          
            WHEN '02' THEN 'de_5_a_9_ans'
          
            WHEN '03' THEN 'de_10_a_19_ans'
          
            WHEN '04' THEN 'de_20_a_29_ans'
          
            WHEN '05' THEN 'de_30_a_39_ans'
          
            WHEN '06' THEN 'de_40_a_49_ans'
          
            WHEN '07' THEN 'de_50_a_59_ans'
          
            WHEN '08' THEN 'de_60_a_69_ans'
          
            WHEN '09' THEN '70_ans_ou_plus'
          
            WHEN '99' THEN 'logement_ordinaire_inoccupe'
          
        END as "aciennete_regroupe_ammenagement_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "AEMMR"
          
            WHEN '0' THEN 'logement_ordinaire_inoccupe'
          
            WHEN '2' THEN 'emmenagement_entre_1900_et_1919'
          
            WHEN '3' THEN 'emmenagement_entre_1920_et_1939'
          
            WHEN '4' THEN 'emmenagement_entre_1940_et_1959'
          
            WHEN '5' THEN 'emmenagement_entre_1960_et_1969'
          
            WHEN '6' THEN 'emmenagement_entre_1970_et_1979'
          
            WHEN '7' THEN 'emmenagement_entre_1980_et_1989'
          
            WHEN '8' THEN 'emmenagement_entre_1990_et_1999'
          
            WHEN '9' THEN 'emmenagement_apres_1999'
          
        END as "annee_regroupe_ammenagement_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        "COMMUNE" as "code_commune_insee"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CHAU"
          
            WHEN '1' THEN 'presence_moyen_chauffage'
          
            WHEN '2' THEN 'absence_moyen_chauffage'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "chauffage_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SURF"
          
            WHEN '1' THEN 'moins_30'
          
            WHEN '2' THEN 'de_30_a_moins_40'
          
            WHEN '3' THEN 'de_40_a_moins_60'
          
            WHEN '4' THEN 'de_60_a_moins_80'
          
            WHEN '5' THEN 'de_80_a_moins_100'
          
            WHEN '6' THEN 'de_100_a_moins_120'
          
            WHEN '7' THEN '120_ou_plus'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "superficie_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "EAU"
          
            WHEN '1' THEN 'eau_froiseulement'
          
            WHEN '2' THEN 'eau_froiet_chaude'
          
            WHEN '3' THEN 'aucpoint_d_eau_a_l_interieur_logement'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "eau_potabke_interieur_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "METRODOM"
          
            WHEN 'D' THEN 'dom'
          
            WHEN 'M' THEN 'france_metropolitaine'
          
        END as "residence_metropole_ou_dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ELEC"
          
            WHEN '1' THEN 'avec_electricite'
          
            WHEN '2' THEN 'sans_electricite'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "electricite_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CLIM"
          
            WHEN '1' THEN 'avec_piece_climatisee'
          
            WHEN '2' THEN 'sans_piece_climatisee'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "min_une_piece_climatise__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "NBPI" as "nombre_pieces_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "BAIN"
          
            WHEN '1' THEN 'avec_baignoire_ou_douche'
          
            WHEN '2' THEN 'sans_baignoire_ni_douche'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "baignoire_ou_douche__dom"
      
      
      
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
    
      
      
      
      
            
      
        CASE "CHOS"
          
            WHEN '1' THEN 'presence_chauffe_eau_solaire'
          
            WHEN '2' THEN 'absence_chauffe_eau_solaire'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "chauffe_eau_solaire__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CUIS"
          
            WHEN '1' THEN 'presence_cuisine_interieure_avec_evier'
          
            WHEN '2' THEN 'absence_cuisine_interieure_avec_evier'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "cuisine_interieur_avec_evier__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SANI"
          
            WHEN '0' THEN 'ni_baignoire'
          
            WHEN '1' THEN 'baignoire_ou_douche_hors_piece_reservee'
          
            WHEN '2' THEN 'salle_s_bains_avec_douche_ou_baignoire'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_dom'
          
        END as "installation_sanitaires__metro"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "DEROU"
          
            WHEN '0' THEN 'aucdeux_roues_a_moteur'
          
            WHEN '1' THEN 'un_seul_deux_roues_a_moteur'
          
            WHEN '2' THEN 'deux_deux_roues_a_moteur'
          
            WHEN '3' THEN 'trois_deux_roues_a_moteur_ou_plus'
          
            WHEN 'X' THEN 'logement_ordinaire_inoccupe_dom'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "nombre_deux_roues_menage__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "BATI"
          
            WHEN '1' THEN 'habitations_fortune'
          
            WHEN '2' THEN 'cases_traditionnelles'
          
            WHEN '3' THEN 'maisons_ou_immeubles_en_bois'
          
            WHEN '4' THEN 'maisons_ou_immeubles_en_dur'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "aspect_bati__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SANIDOM"
          
            WHEN '11' THEN 'avec_baignoire_ou_douche_et_avec_wc_a_l_interieur'
          
            WHEN '12' THEN 'avec_baignoire_ou_douche_mais_sans_wc_a_l_interieur'
          
            WHEN '21' THEN 'sans_baignoire_ni_douche_mais_avec_wc_a_l_interieur'
          
            WHEN '22' THEN 'sans_baignoire_ni_douche'
          
            WHEN 'YY' THEN 'hors_residence_principale'
          
            WHEN 'ZZ' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "installation_sanitaires__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "WC"
          
            WHEN '1' THEN 'avec_w._c._a_l_interieur_logement'
          
            WHEN '2' THEN 'sans_w._c._a_l_interieur_logement'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
            WHEN 'Z' THEN 'logement_ordinaire_france_metropolitaine'
          
        END as "presence_wc_interieur_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TYPC"
          
            WHEN '1' THEN 'batiment_d_habitation_seul_logement_isole'
          
            WHEN '2' THEN 'batiment_d_habitation_seul_logement_jumele_ou_groupe_toute_autre_facon'
          
            WHEN '3' THEN 'batiment_d_habitation_2_logements_ou_plus'
          
            WHEN '4' THEN 'batiment_a_usage_autre_qu_habitation'
          
            WHEN '5' THEN 'construction_provisoire'
          
            WHEN 'Y' THEN 'hors_residence_principale'
          
        END as "type_construction"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ACHL"
          
            WHEN 'A11' THEN 'avant_1919'
          
            WHEN 'A12' THEN 'de_1919_a_1945'
          
            WHEN 'B11' THEN 'de_1946_a_1970'
          
            WHEN 'B12' THEN 'de_1971_a_1990'
          
            WHEN 'C100' THEN 'de_1991_a_2005'
          
            WHEN 'C106' THEN 'en_2006'
          
            WHEN 'C107' THEN 'en_2007'
          
            WHEN 'C108' THEN 'en_2008'
          
            WHEN 'C109' THEN 'en_2009'
          
            WHEN 'C110' THEN 'en_2010'
          
            WHEN 'C111' THEN 'en_2011'
          
            WHEN 'C112' THEN 'en_2012'
          
            WHEN 'C113' THEN 'en_2013'
          
            WHEN 'C114' THEN 'en_2014'
          
            WHEN 'C115' THEN 'en_2015'
          
            WHEN 'C116' THEN 'en_2016'
          
            WHEN 'C117' THEN 'en_2017'
          
            WHEN 'C2018' THEN 'en_2018_partiel'
          
            WHEN 'C2019' THEN 'en_2019_partiel'
          
            WHEN 'C2020' THEN 'en_2020_partiel'
          
            WHEN 'C2021' THEN 'en_2021_partiel'
          
            WHEN 'C2022' THEN 'en_2022_partiel'
          
        END as "achevement_construction_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TYPL"
          
            WHEN '1' THEN 'maison'
          
            WHEN '2' THEN 'appartement'
          
            WHEN '3' THEN 'logement_foyer'
          
            WHEN '4' THEN 'chambre_d_hotel'
          
            WHEN '5' THEN 'habitation_fortune'
          
            WHEN '6' THEN 'piece_independante_ayant_sa_propre_entree'
          
        END as "type_logement"
      
      
      
      
    
  FROM logement

),
decode_logement_iris AS(
    select 
        *,
        CASE 
		    WHEN code_iris_incomplet = 'ZZZZZZZZZ' THEN CONCAT(code_commune_insee, '0000')
		    ELSE code_iris_incomplet
	    END as code_iris 
    from decode_logement
)

SELECT 
    *
FROM 
    decode_logement_iris
  );