
  create view "defaultdb"."intermediaires"."habitat_renomee__dbt_tmp"
    
    
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
        
            CASE "TYPC"
                
                    when '4' then 'logement_type_contruction_autre_logement'
                
                    when '2' then 'logement_type_contruction_un_logement_groupe'
                
                    when '5' then 'logement_type_construction_provisoire'
                
                    when '3' then 'logement_type_contruction_plusieurs_logements'
                
                    when '1' then 'logement_type_contruction_un_logement_isole'
                
            END AS "TYPC",
        
            CASE "ANEMR"
                
                    when '"09"' then 'emmenagement_plus_70_ans'
                
                    when '"07"' then 'emmenagement_50_59_ans'
                
                    when '"05"' then 'emmenagement_30_39_ans'
                
                    when '"06"' then 'emmenagement_40_49_ans'
                
                    when '"02"' then 'emmenagement_5_9_ans'
                
                    when '"03"' then 'emmenagement_10_19_ans'
                
                    when '"08"' then 'emmenagement_60_69_ans'
                
                    when '"04"' then 'emmenagement_20_29_ans'
                
                    when '"00"' then 'emmenagement_moins_2_ans'
                
                    when '"01"' then 'emmenagement_2_4_ans'
                
            END AS "ANEMR",
        
            CASE "HLML"
                
                    when '1' then 'appartient_hlm'
                
                    when '2' then 'appartient_pas_hlm'
                
            END AS "HLML",
        
            CASE "ACHL"
                
                    when 'C117' then 'construction_2017'
                
                    when 'C109' then 'construction_2009'
                
                    when 'C112' then 'construction_2012'
                
                    when 'C106' then 'construction_2006'
                
                    when 'A11' then 'construction_avant_1919'
                
                    when 'C116' then 'construction_2016'
                
                    when 'C2021' then 'construction_2021'
                
                    when 'C2018' then 'construction_2018'
                
                    when 'C107' then 'construction_2007'
                
                    when 'C113' then 'construction_2013'
                
                    when 'C2020' then 'construction_2020'
                
                    when 'B11' then 'construction_1916_1970'
                
                    when 'C100' then 'construction_1991_2025'
                
                    when 'C110' then 'construction_2010'
                
                    when 'C114' then 'construction_2014'
                
                    when 'C111' then 'construction_2011'
                
                    when 'A12' then 'construction_1919_1945'
                
                    when 'B12' then 'construction_1971_1990'
                
                    when 'C115' then 'construction_2015'
                
                    when 'C108' then 'construction_2008'
                
                    when 'C2022' then 'construction_2022'
                
                    when 'C2019' then 'construction_2019'
                
            END AS "ACHL",
        
            CASE "NBPI"
                
                    when '20' then 'logements_avec_6_et_plus_pieces'
                
                    when '8' then 'logements_avec_6_et_plus_pieces'
                
                    when '6' then 'logements_avec_6_et_plus_pieces'
                
                    when '4' then 'logements_avec_4_pieces'
                
                    when '11' then 'logements_avec_6_et_plus_pieces'
                
                    when '7' then 'logements_avec_6_et_plus_pieces'
                
                    when '10' then 'logements_avec_6_et_plus_pieces'
                
                    when '19' then 'logements_avec_6_et_plus_pieces'
                
                    when '1' then 'logements_avec_1_piece'
                
                    when '12' then 'logements_avec_6_et_plus_pieces'
                
                    when '16' then 'logements_avec_6_et_plus_pieces'
                
                    when '9' then 'logements_avec_6_et_plus_pieces'
                
                    when '15' then 'logements_avec_6_et_plus_pieces'
                
                    when '3' then 'logements_avec_3_pieces'
                
                    when '13' then 'logements_avec_6_et_plus_pieces'
                
                    when '17' then 'logements_avec_6_et_plus_pieces'
                
                    when '2' then 'logements_avec_2_pieces'
                
                    when '14' then 'logements_avec_6_et_plus_pieces'
                
                    when '18' then 'logements_avec_6_et_plus_pieces'
                
                    when '5' then 'logements_avec_5_pieces'
                
            END AS "NBPI",
        
            CASE "ELEC"
                
                    when '2' then 'dom__sans_electricite'
                
                    when '1' then 'dom__avec_electricite'
                
            END AS "ELEC",
        
            CASE "AGEMEN8"
                
                    when '80' then 'pr_plus_80'
                
                    when '55' then 'pr_55_64'
                
                    when '65' then 'pr_64_79'
                
                    when '15' then 'pr_15_19'
                
                    when '25' then 'pr_25_39'
                
                    when '0' then 'pr_moins_15'
                
                    when '40' then 'pr_40_54'
                
                    when '20' then 'pr_20_24'
                
            END AS "AGEMEN8",
        
            CASE "SANIDOM"
                
                    when '11' then 'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'
                
                    when '12' then 'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'
                
                    when '22' then 'dom__logement_ni_baignoire_ni_douche'
                
                    when '21' then 'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'
                
            END AS "SANIDOM",
        
            CASE "CLIM"
                
                    when '2' then 'dom__sans_piece_climatisee'
                
                    when '1' then 'dom__avec_piece_climatisee'
                
            END AS "CLIM",
        
            CASE "AEMMR"
                
                    when '4' then 'emmenagement_1940_1959'
                
                    when '7' then 'emmenagement_1980_1989'
                
                    when '8' then 'emmenagement_1990_1999'
                
                    when '2' then 'emmenagement_1900_1919'
                
                    when '6' then 'emmenagement_1970_1979'
                
                    when '9' then 'emmenagement_apres_1999'
                
                    when '5' then 'emmenagement_1940_1969'
                
                    when '3' then 'emmenagement_1920_1939'
                
            END AS "AEMMR",
        
            CASE "BAIN"
                
                    when '1' then 'dom__beignoire_ou_douche'
                
                    when '2' then 'dom__ni_beignoire_ni_douche'
                
            END AS "BAIN",
        
            CASE "SURF"
                
                    when '2' then 'logements_30_40_mc'
                
                    when '5' then 'logements_80_100_mc'
                
                    when '6' then 'logements_100_120_mc'
                
                    when '4' then 'logements_60_80_mc'
                
                    when '1' then 'logements_moins_30_mc'
                
                    when '7' then 'logements_plus_120_mc'
                
                    when '3' then 'logements_40_60_mc'
                
            END AS "SURF",
        
            CASE "CMBL"
                
                    when '2' then 'metro__combustible_gaz_de_ville_ou_reseau'
                
                    when '6' then 'metro__combustible_autre'
                
                    when '3' then 'metro__combustible_fioul'
                
                    when '4' then 'metro__combustible_electricte'
                
                    when '5' then 'metro__combustible_gaz_bouteille_ou_citerne'
                
                    when '1' then 'metro__combustible_chauffage_urbain'
                
            END AS "CMBL",
        
            CASE "EAU"
                
                    when '2' then 'dom__eau_froide_et_chaude'
                
                    when '3' then 'dom__aucun_point_eau'
                
                    when '1' then 'dom__eau_froide_seulement'
                
            END AS "EAU",
        
            CASE "CATL"
                
                    when '3' then 'residence_secondaire'
                
                    when '1' then 'residence_principale'
                
                    when '2' then 'logement_occasionnel'
                
                    when '4' then 'logement_vacant'
                
            END AS "CATL",
        
            CASE "WC"
                
                    when '2' then 'dom__logements_sans_wc_interieurs'
                
                    when '1' then 'dom__logements_avec_wc_interieurs'
                
            END AS "WC",
        
            CASE "TYPL"
                
                    when '4' then 'logement_type_chambre_hotel'
                
                    when '1' then 'logement_type_maison'
                
                    when '6' then 'logement_type_piece_independante'
                
                    when '5' then 'logement_type_habitation_fortune'
                
                    when '3' then 'logement_type_appartement_foyer'
                
                    when '2' then 'logement_type_appartement'
                
            END AS "TYPL",
        
            CASE "EGOUL"
                
                    when '1' then 'dom__raccordement_egouts'
                
                    when '3' then 'dom__raccordement_puisard'
                
                    when '4' then 'dom__evacuation_sol'
                
                    when '2' then 'dom__raccordement_fosse_septique'
                
            END AS "EGOUL",
        
            CASE "GARL"
                
                    when '1' then 'avec_emplacement_stationnement'
                
                    when '2' then 'sans_emplacement_stationnement'
                
            END AS "GARL",
        
            CASE "SANI"
                
                    when '2' then 'metro__logement_salle_de_bain'
                
                    when '1' then 'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'
                
                    when '0' then 'metro__logement_ni_baignoire_ni_douche'
                
            END AS "SANI",
        
            CASE "STOCD"
                
                    when '10' then 'logements_occupes_par_proprietaires'
                
                    when '23' then 'logements_occupes_par_locataire_meuble_hotel'
                
                    when '30' then 'logements_occupes_par_loge_gratuitemenent'
                
                    when '22' then 'logements_occupes_par_locataire_sous_locataire_vide_hlm'
                
                    when '21' then 'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'
                
            END AS "STOCD",
        
            CASE "ASCEN"
                
                    when '2' then 'sans_ascensseur'
                
                    when '1' then 'avec_ascensseur'
                
            END AS "ASCEN",
        
            CASE "CUIS"
                
                    when '1' then 'dom__avec_cuisine_interieure_evier'
                
                    when '2' then 'dom__sans_cuisine_interieure_evier'
                
            END AS "CUIS",
        
            CASE "CHAU"
                
                    when '1' then 'dom__presence_chauffage'
                
                    when '2' then 'dom__abscence_chauffage'
                
            END AS "CHAU",
        
            CASE "BATI"
                
                    when '3' then 'dom__habitation_en_bois'
                
                    when '1' then 'dom__habitation_de_fortune'
                
                    when '2' then 'dom__cases_traditionnelles'
                
                    when '4' then 'dom__habitation_en_dur'
                
            END AS "BATI",
        
            CASE "CHOS"
                
                    when '1' then 'dom__presence_chauffe_eau_solaire'
                
                    when '2' then 'dom__abscence_chauffe_eau_solaire'
                
            END AS "CHOS",
        
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