
  create view "defaultdb"."intermediaires"."habitat_renomee__dbt_tmp"
    
    
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
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'A11' then 'construction_avant_1919'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'A12' then 'construction_1919_1945'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'B11' then 'construction_1916_1970'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'B12' then 'construction_1971_1990'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C10' then 'construction_1991_2025'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C10' then 'construction_2006'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C10' then 'construction_2007'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C10' then 'construction_2008'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C10' then 'construction_2009'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2010'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2011'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2012'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2013'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2014'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2015'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2016'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C11' then 'construction_2017'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C20' then 'construction_2018'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C20' then 'construction_2019'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C20' then 'construction_2020'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C20' then 'construction_2021'
                
                    when LPAD(CAST("ACHL" AS TEXT), 3, '0') = 'C20' then 'construction_2022'
                
            END AS "ACHL",
        
            CASE
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '002' then 'emmenagement_1900_1919'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '003' then 'emmenagement_1920_1939'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '004' then 'emmenagement_1940_1959'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '005' then 'emmenagement_1940_1969'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '006' then 'emmenagement_1970_1979'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '007' then 'emmenagement_1980_1989'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '008' then 'emmenagement_1990_1999'
                
                    when LPAD(CAST("AEMMR" AS TEXT), 3, '0') = '009' then 'emmenagement_apres_1999'
                
            END AS "AEMMR",
        
            CASE
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '000' then 'emmenagement_moins_2_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '001' then 'emmenagement_2_4_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '002' then 'emmenagement_5_9_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '003' then 'emmenagement_10_19_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '004' then 'emmenagement_20_29_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '005' then 'emmenagement_30_39_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '006' then 'emmenagement_40_49_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '007' then 'emmenagement_50_59_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '008' then 'emmenagement_60_69_ans'
                
                    when LPAD(CAST("ANEMR" AS TEXT), 3, '0') = '009' then 'emmenagement_plus_70_ans'
                
            END AS "ANEMR",
        
            CASE
                
                    when LPAD(CAST("ASCEN" AS TEXT), 3, '0') = '001' then 'avec_ascensseur'
                
                    when LPAD(CAST("ASCEN" AS TEXT), 3, '0') = '002' then 'sans_ascensseur'
                
            END AS "ASCEN",
        
            CASE
                
                    when LPAD(CAST("BAIN" AS TEXT), 3, '0') = '001' then 'dom__beignoire_ou_douche'
                
                    when LPAD(CAST("BAIN" AS TEXT), 3, '0') = '002' then 'dom__ni_beignoire_ni_douche'
                
            END AS "BAIN",
        
            CASE
                
                    when LPAD(CAST("BATI" AS TEXT), 3, '0') = '001' then 'dom__habitation_de_fortune'
                
                    when LPAD(CAST("BATI" AS TEXT), 3, '0') = '002' then 'dom__cases_traditionnelles'
                
                    when LPAD(CAST("BATI" AS TEXT), 3, '0') = '003' then 'dom__habitation_en_bois'
                
                    when LPAD(CAST("BATI" AS TEXT), 3, '0') = '004' then 'dom__habitation_en_dur'
                
            END AS "BATI",
        
            CASE
                
                    when LPAD(CAST("CHAU" AS TEXT), 3, '0') = '001' then 'dom__presence_chauffage'
                
                    when LPAD(CAST("CHAU" AS TEXT), 3, '0') = '002' then 'dom__abscence_chauffage'
                
            END AS "CHAU",
        
            CASE
                
                    when LPAD(CAST("CHOS" AS TEXT), 3, '0') = '001' then 'dom__presence_chauffe_eau_solaire'
                
                    when LPAD(CAST("CHOS" AS TEXT), 3, '0') = '002' then 'dom__abscence_chauffe_eau_solaire'
                
            END AS "CHOS",
        
            CASE
                
                    when LPAD(CAST("CLIM" AS TEXT), 3, '0') = '001' then 'dom__avec_piece_climatisee'
                
                    when LPAD(CAST("CLIM" AS TEXT), 3, '0') = '002' then 'dom__sans_piece_climatisee'
                
            END AS "CLIM",
        
            CASE
                
                    when LPAD(CAST("CMBL" AS TEXT), 3, '0') = '001' then 'metro__combustible_chauffage_urbain'
                
                    when LPAD(CAST("CMBL" AS TEXT), 3, '0') = '002' then 'metro__combustible_gaz_de_ville_ou_reseau'
                
                    when LPAD(CAST("CMBL" AS TEXT), 3, '0') = '003' then 'metro__combustible_fioul'
                
                    when LPAD(CAST("CMBL" AS TEXT), 3, '0') = '004' then 'metro__combustible_electricte'
                
                    when LPAD(CAST("CMBL" AS TEXT), 3, '0') = '005' then 'metro__combustible_gaz_bouteille_ou_citerne'
                
                    when LPAD(CAST("CMBL" AS TEXT), 3, '0') = '006' then 'metro__combustible_autre'
                
            END AS "CMBL",
        
            CASE
                
                    when LPAD(CAST("CUIS" AS TEXT), 3, '0') = '001' then 'dom__avec_cuisine_interieure_evier'
                
                    when LPAD(CAST("CUIS" AS TEXT), 3, '0') = '002' then 'dom__sans_cuisine_interieure_evier'
                
            END AS "CUIS",
        
            CASE
                
                    when LPAD(CAST("EAU" AS TEXT), 3, '0') = '001' then 'dom__eau_froide_seulement'
                
                    when LPAD(CAST("EAU" AS TEXT), 3, '0') = '002' then 'dom__eau_froide_et_chaude'
                
                    when LPAD(CAST("EAU" AS TEXT), 3, '0') = '003' then 'dom__aucun_point_eau'
                
            END AS "EAU",
        
            CASE
                
                    when LPAD(CAST("EGOUL" AS TEXT), 3, '0') = '001' then 'dom__raccordement_egouts'
                
                    when LPAD(CAST("EGOUL" AS TEXT), 3, '0') = '002' then 'dom__raccordement_fosse_septique'
                
                    when LPAD(CAST("EGOUL" AS TEXT), 3, '0') = '003' then 'dom__raccordement_puisard'
                
                    when LPAD(CAST("EGOUL" AS TEXT), 3, '0') = '004' then 'dom__evacuation_sol'
                
            END AS "EGOUL",
        
            CASE
                
                    when LPAD(CAST("ELEC" AS TEXT), 3, '0') = '001' then 'dom__avec_electricite'
                
                    when LPAD(CAST("ELEC" AS TEXT), 3, '0') = '002' then 'dom__sans_electricite'
                
            END AS "ELEC",
        
            CASE
                
                    when LPAD(CAST("GARL" AS TEXT), 3, '0') = '001' then 'avec_emplacement_stationnement'
                
                    when LPAD(CAST("GARL" AS TEXT), 3, '0') = '002' then 'sans_emplacement_stationnement'
                
            END AS "GARL",
        
            CASE
                
                    when LPAD(CAST("HLML" AS TEXT), 3, '0') = '001' then 'appartient_hlm'
                
                    when LPAD(CAST("HLML" AS TEXT), 3, '0') = '002' then 'appartient_pas_hlm'
                
            END AS "HLML",
        
            CASE
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '001' then 'logements_avec_1_piece'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '002' then 'logements_avec_2_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '003' then 'logements_avec_3_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '004' then 'logements_avec_4_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '005' then 'logements_avec_5_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '006' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '007' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '008' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '009' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '010' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '011' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '012' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '013' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '014' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '015' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '016' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '017' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '018' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '019' then 'logements_avec_6_et_plus_pieces'
                
                    when LPAD(CAST("NBPI" AS TEXT), 3, '0') = '020' then 'logements_avec_6_et_plus_pieces'
                
            END AS "NBPI",
        
            CASE
                
                    when LPAD(CAST("SANI" AS TEXT), 3, '0') = '000' then 'metro__logement_ni_baignoire_ni_douche'
                
                    when LPAD(CAST("SANI" AS TEXT), 3, '0') = '001' then 'metro__logement_avec_baignoire_ou_douche_hors_piece_reservee'
                
                    when LPAD(CAST("SANI" AS TEXT), 3, '0') = '002' then 'metro__logement_salle_de_bain'
                
            END AS "SANI",
        
            CASE
                
                    when LPAD(CAST("SANIDOM" AS TEXT), 3, '0') = '011' then 'dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures'
                
                    when LPAD(CAST("SANIDOM" AS TEXT), 3, '0') = '012' then 'dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures'
                
                    when LPAD(CAST("SANIDOM" AS TEXT), 3, '0') = '021' then 'dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures'
                
                    when LPAD(CAST("SANIDOM" AS TEXT), 3, '0') = '022' then 'dom__logement_ni_baignoire_ni_douche'
                
            END AS "SANIDOM",
        
            CASE
                
                    when LPAD(CAST("STOCD" AS TEXT), 3, '0') = '010' then 'logements_occupes_par_proprietaires'
                
                    when LPAD(CAST("STOCD" AS TEXT), 3, '0') = '021' then 'logements_occupes_par_locataire_sous_locataire_vide_non_hlm'
                
                    when LPAD(CAST("STOCD" AS TEXT), 3, '0') = '022' then 'logements_occupes_par_locataire_sous_locataire_vide_hlm'
                
                    when LPAD(CAST("STOCD" AS TEXT), 3, '0') = '023' then 'logements_occupes_par_locataire_meuble_hotel'
                
                    when LPAD(CAST("STOCD" AS TEXT), 3, '0') = '030' then 'logements_occupes_par_loge_gratuitemenent'
                
            END AS "STOCD",
        
            CASE
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '001' then 'logements_moins_30_mc'
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '002' then 'logements_30_40_mc'
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '003' then 'logements_40_60_mc'
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '004' then 'logements_60_80_mc'
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '005' then 'logements_80_100_mc'
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '006' then 'logements_100_120_mc'
                
                    when LPAD(CAST("SURF" AS TEXT), 3, '0') = '007' then 'logements_plus_120_mc'
                
            END AS "SURF",
        
            CASE
                
                    when LPAD(CAST("TYPC" AS TEXT), 3, '0') = '001' then 'logement_type_contruction_un_logement_isole'
                
                    when LPAD(CAST("TYPC" AS TEXT), 3, '0') = '002' then 'logement_type_contruction_un_logement_groupe'
                
                    when LPAD(CAST("TYPC" AS TEXT), 3, '0') = '003' then 'logement_type_contruction_plusieurs_logements'
                
                    when LPAD(CAST("TYPC" AS TEXT), 3, '0') = '004' then 'logement_type_contruction_autre_logement'
                
                    when LPAD(CAST("TYPC" AS TEXT), 3, '0') = '005' then 'logement_type_construction_provisoire'
                
            END AS "TYPC",
        
            CASE
                
                    when LPAD(CAST("TYPL" AS TEXT), 3, '0') = '001' then 'logement_type_maison'
                
                    when LPAD(CAST("TYPL" AS TEXT), 3, '0') = '002' then 'logement_type_appartement'
                
                    when LPAD(CAST("TYPL" AS TEXT), 3, '0') = '003' then 'logement_type_appartement_foyer'
                
                    when LPAD(CAST("TYPL" AS TEXT), 3, '0') = '004' then 'logement_type_chambre_hotel'
                
                    when LPAD(CAST("TYPL" AS TEXT), 3, '0') = '005' then 'logement_type_habitation_fortune'
                
                    when LPAD(CAST("TYPL" AS TEXT), 3, '0') = '006' then 'logement_type_piece_independante'
                
            END AS "TYPL",
        
            CASE
                
                    when LPAD(CAST("WC" AS TEXT), 3, '0') = '001' then 'dom__logements_avec_wc_interieurs'
                
                    when LPAD(CAST("WC" AS TEXT), 3, '0') = '002' then 'dom__logements_sans_wc_interieurs'
                
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