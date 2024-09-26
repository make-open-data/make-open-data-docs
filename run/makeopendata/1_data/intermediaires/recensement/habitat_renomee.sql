
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
        
            CASE "ACHL"
                
                    when 'A11' then 'construction_avant_1919'
                
                    when 'A12' then 'construction_1919_1945'
                
                    when 'B11' then 'construction_1916_1970'
                
                    when 'B12' then 'construction_1971_1990'
                
                    when 'C100' then 'construction_1991_2025'
                
                    when 'C106' then 'construction_2006'
                
                    when 'C107' then 'construction_2007'
                
                    when 'C108' then 'construction_2008'
                
                    when 'C109' then 'construction_2009'
                
                    when 'C110' then 'construction_2010'
                
                    when 'C111' then 'construction_2011'
                
                    when 'C112' then 'construction_2012'
                
                    when 'C113' then 'construction_2013'
                
                    when 'C114' then 'construction_2014'
                
                    when 'C115' then 'construction_2015'
                
                    when 'C116' then 'construction_2016'
                
                    when 'C117' then 'construction_2017'
                
                    when 'C2018' then 'construction_2018'
                
                    when 'C2019' then 'construction_2019'
                
                    when 'C2020' then 'construction_2020'
                
                    when 'C2021' then 'construction_2021'
                
                    when 'C2022' then 'construction_2022'
                
            END AS "ACHL",
        
            CASE "AEMMR"
                
                    when '2' then 'emmenagement_1900_1919'
                
                    when '3' then 'emmenagement_1920_1939'
                
                    when '4' then 'emmenagement_1940_1959'
                
                    when '5' then 'emmenagement_1940_1969'
                
                    when '6' then 'emmenagement_1970_1979'
                
                    when '7' then 'emmenagement_1980_1989'
                
                    when '8' then 'emmenagement_1990_1999'
                
                    when '9' then 'emmenagement_apres_1999'
                
            END AS "AEMMR",
        
            CASE "AGEMEN8"
                
                    when '0' then 'pr_moins_15'
                
                    when '15' then 'pr_15_19'
                
                    when '20' then 'pr_20_24'
                
                    when '25' then 'pr_25_39'
                
                    when '40' then 'pr_40_54'
                
                    when '55' then 'pr_55_64'
                
                    when '65' then 'pr_64_79'
                
                    when '80' then 'pr_plus_80'
                
            END AS "AGEMEN8",
        
            CASE "ANEMR"
                
                    when '"00"' then 'emmenagement_moins_2_ans'
                
                    when '"01"' then 'emmenagement_2_4_ans'
                
                    when '"02"' then 'emmenagement_5_9_ans'
                
                    when '"03"' then 'emmenagement_10_19_ans'
                
                    when '"04"' then 'emmenagement_20_29_ans'
                
                    when '"05"' then 'emmenagement_30_39_ans'
                
                    when '"06"' then 'emmenagement_40_49_ans'
                
                    when '"07"' then 'emmenagement_50_59_ans'
                
                    when '"08"' then 'emmenagement_60_69_ans'
                
                    when '"09"' then 'emmenagement_plus_70_ans'
                
            END AS "ANEMR",
        
            CASE "ASCEN"
                
                    when '1' then 'avec_ascensseur'
                
                    when '2' then 'sans_ascensseur'
                
            END AS "ASCEN",
        
            CASE "BAIN"
                
                    when '1' then 'dom__beignoire_ou_douche'
                
                    when '2' then 'dom__ni_beignoire_ni_douche'
                
            END AS "BAIN",
        
            CASE "BATI"
                
                    when '1' then 'dom__habitation_de_fortune'
                
                    when '2' then 'dom__cases_traditionnelles'
                
                    when '3' then 'dom__habitation_en_bois'
                
                    when '4' then 'dom__habitation_en_dur'
                
            END AS "BATI",
        
            CASE "CATL"
                
                    when '1' then 'residence_principale'
                
                    when '2' then 'logement_occasionnel'
                
                    when '3' then 'residence_secondaire'
                
                    when '4' then 'logement_vacant'
                
            END AS "CATL",
        
            CASE "CHAU"
                
                    when '1' then 'dom__presence_chauffage'
                
                    when '2' then 'dom__abscence_chauffage'
                
            END AS "CHAU",
        
            CASE "CHOS"
                
                    when '1' then 'dom__presence_chauffe_eau_solaire'
                
                    when '2' then 'dom__abscence_chauffe_eau_solaire'
                
            END AS "CHOS",
        
            CASE "CLIM"
                
                    when '1' then 'dom__avec_piece_climatisee'
                
                    when '2' then 'dom__sans_piece_climatisee'
                
            END AS "CLIM",
        
            CASE "CMBL"
                
                    when '1' then 'metro__combustible_chauffage_urbain'
                
                    when '2' then 'metro__combustible_gaz_de_ville_ou_reseau'
                
                    when '3' then 'metro__combustible_fioul'
                
                    when '4' then 'metro__combustible_electricte'
                
                    when '5' then 'metro__combustible_gaz_bouteille_ou_citerne'
                
                    when '6' then 'metro__combustible_autre'
                
            END AS "CMBL",
        
            CASE "CUIS"
                
                    when '1' then 'dom__avec_cuisine_interieure_evier'
                
                    when '2' then 'dom__sans_cuisine_interieure_evier'
                
            END AS "CUIS",
        
            CASE "EAU"
                
                    when '1' then 'dom__eau_froide_seulement'
                
                    when '2' then 'dom__eau_froide_et_chaude'
                
                    when '3' then 'dom__aucun_point_eau'
                
            END AS "EAU",
        
            CASE "EGOUL"
                
                    when '1' then 'dom__raccordement_egouts'
                
                    when '2' then 'dom__raccordement_fosse_septique'
                
                    when '3' then 'dom__raccordement_puisard'
                
                    when '4' then 'dom__evacuation_sol'
                
            END AS "EGOUL",
        
            CASE "ELEC"
                
                    when '1' then 'dom__avec_electricite'
                
                    when '2' then 'dom__sans_electricite'
                
            END AS "ELEC",
        
            CASE "GARL"
                
                    when '1' then 'avec_emplacement_stationnement'
                
                    when '2' then 'sans_emplacement_stationnement'
                
            END AS "GARL",
        
            CASE "HLML"
                
                    when '1' then 'appartient_hlm'
                
                    when '2' then 'appartient_pas_hlm'
                
            END AS "HLML",
        
            CASE "NBPI"
                
                    when '1' then 'logements_avec_1_piece'
                
                    when '10' then 'logements_avec_6_et_plus_pieces'
                
                    when '11' then 'logements_avec_6_et_plus_pieces'
                
                    when '12' then 'logements_avec_6_et_plus_pieces'
                
                    when '13' then 'logements_avec_6_et_plus_pieces'
                
                    when '14' then 'logements_avec_6_et_plus_pieces'
                
                    when '15' then 'logements_avec_6_et_plus_pieces'
                
                    when '16' then 'logements_avec_6_et_plus_pieces'
                
                    when '17' then 'logements_avec_6_et_plus_pieces'
                
                    when '18' then 'logements_avec_6_et_plus_pieces'
                
                    when '19' then 'logements_avec_6_et_plus_pieces'
                
                    when '2' then 'logements_avec_2_pieces'
                
                    when '20' then 'logements_avec_6_et_plus_pieces'
                
                    when '3' then 'logements_avec_3_pieces'
                
                    when '4' then 'logements_avec_4_pieces'
                
                    when '5' then 'logements_avec_5_pieces'
                
                    when '6' then 'logements_avec_6_et_plus_pieces'
                
                    when '7' then 'logements_avec_6_et_plus_pieces'
                
                    when '8' then 'logements_avec_6_et_plus_pieces'
                
                    when '9' then 'logements_avec_6_et_plus_pieces'
                
            END AS "NBPI",
        
            CASE "SANI"
                
                    when '0' then 'metro__logement_ni_baignoire_ni_douche'
                
                    when '1' then 'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'
                
                    when '2' then 'metro__logement_salle_de_bain'
                
            END AS "SANI",
        
            CASE "SANIDOM"
                
                    when '11' then 'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'
                
                    when '12' then 'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'
                
                    when '21' then 'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'
                
                    when '22' then 'dom__logement_ni_baignoire_ni_douche'
                
            END AS "SANIDOM",
        
            CASE "STOCD"
                
                    when '10' then 'logements_occupes_par_proprietaires'
                
                    when '21' then 'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'
                
                    when '22' then 'logements_occupes_par_locataire_sous_locataire_vide_hlm'
                
                    when '23' then 'logements_occupes_par_locataire_meuble_hotel'
                
                    when '30' then 'logements_occupes_par_loge_gratuitemenent'
                
            END AS "STOCD",
        
            CASE "SURF"
                
                    when '1' then 'logements_moins_30_mc'
                
                    when '2' then 'logements_30_40_mc'
                
                    when '3' then 'logements_40_60_mc'
                
                    when '4' then 'logements_60_80_mc'
                
                    when '5' then 'logements_80_100_mc'
                
                    when '6' then 'logements_100_120_mc'
                
                    when '7' then 'logements_plus_120_mc'
                
            END AS "SURF",
        
            CASE "TYPC"
                
                    when '1' then 'logement_type_contruction_un_logement_isole'
                
                    when '2' then 'logement_type_contruction_un_logement_groupe'
                
                    when '3' then 'logement_type_contruction_plusieurs_logements'
                
                    when '4' then 'logement_type_contruction_autre_logement'
                
                    when '5' then 'logement_type_construction_provisoire'
                
            END AS "TYPC",
        
            CASE "TYPL"
                
                    when '1' then 'logement_type_maison'
                
                    when '2' then 'logement_type_appartement'
                
                    when '3' then 'logement_type_appartement_foyer'
                
                    when '4' then 'logement_type_chambre_hotel'
                
                    when '5' then 'logement_type_habitation_fortune'
                
                    when '6' then 'logement_type_piece_independante'
                
            END AS "TYPL",
        
            CASE "WC"
                
                    when '1' then 'dom__logements_avec_wc_interieurs'
                
                    when '2' then 'dom__logements_sans_wc_interieurs'
                
            END AS "WC",
        
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