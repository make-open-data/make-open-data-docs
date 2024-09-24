

WITH logement AS (
    select * 
    
        from "defaultdb"."sources"."logement_2020_dev"
    
),
logement_renomee AS (
    (     

    

    
    
    


    select 

        "COMMUNE" as code_commune_insee,
        CASE 
		    WHEN "IRIS" = 'ZZZZZZZZZ' THEN CONCAT("COMMUNE", '0000')
		    ELSE "IRIS"
	    END as code_iris,
        
            CASE "INPER2"
                
                    when '6' then 'menages_avec_1_personne_feminin'
                
                    when '15' then 'menages_avec_1_personne_feminin'
                
                    when '39' then 'menages_avec_1_personne_feminin'
                
                    when '5' then 'menages_avec_1_personne_feminin'
                
                    when '13' then 'menages_avec_1_personne_feminin'
                
                    when '11' then 'menages_avec_1_personne_feminin'
                
                    when '0' then 'menages_avec_0_personne_feminin'
                
                    when '14' then 'menages_avec_1_personne_feminin'
                
                    when '7' then 'menages_avec_1_personne_feminin'
                
                    when '3' then 'menages_avec_1_personne_feminin'
                
                    when '8' then 'menages_avec_1_personne_feminin'
                
                    when '9' then 'menages_avec_1_personne_feminin'
                
                    when '12' then 'menages_avec_1_personne_feminin'
                
                    when '19' then 'menages_avec_1_personne_feminin'
                
                    when '2' then 'menages_avec_1_personne_feminin'
                
                    when '4' then 'menages_avec_1_personne_feminin'
                
                    when '16' then 'menages_avec_1_personne_feminin'
                
                    when '20' then 'menages_avec_1_personne_feminin'
                
                    when '1' then 'menages_avec_1_personne_feminin'
                
                    when '17' then 'menages_avec_1_personne_feminin'
                
                    when '10' then 'menages_avec_1_personne_feminin'
                
            END AS "INPER2",
        
            CASE "INPER1"
                
                    when '16' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '1' then 'menages_avec_1_personne_masculin'
                
                    when '3' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '12' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '9' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '0' then 'menages_avec_0_personne_masculin'
                
                    when '11' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '22' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '5' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '7' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '6' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '26' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '19' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '13' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '38' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '20' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '10' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '18' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '8' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '2' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '15' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '14' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '17' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when '4' then 'menages_avec_2_et_plus_personnes_masculin'
                
            END AS "INPER1",
        
            CASE "INP15M"
                
                    when '14' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '13' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '0' then 'menages_avec_0_personne_15_ans_et_moins'
                
                    when '15' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
            END AS "INP15M",
        
            CASE "INP19M"
                
                    when '13' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '25' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '23' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '15' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '19' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '20' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '0' then 'menages_avec_0_personne_19_ans_et_moins'
                
                    when '16' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '18' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '17' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '21' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '41' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '55' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '22' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '14' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
            END AS "INP19M",
        
            CASE "INP11M"
                
                    when '14' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '13' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '0' then 'menages_avec_0_personne_11_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
            END AS "INP11M",
        
            CASE "INP60M"
                
                    when '1' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '19' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '0' then 'menages_avec_0_personne_60_ans_et_plus'
                
                    when '55' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '17' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
            END AS "INP60M",
        
            CASE "INPER"
                
                    when '22' then 'menages_avec_6_et_plus_personnes'
                
                    when '25' then 'menages_avec_6_et_plus_personnes'
                
                    when '9' then 'menages_avec_6_et_plus_personnes'
                
                    when '1' then 'menages_avec_1_personne'
                
                    when '21' then 'menages_avec_6_et_plus_personnes'
                
                    when '8' then 'menages_avec_6_et_plus_personnes'
                
                    when '17' then 'menages_avec_6_et_plus_personnes'
                
                    when '5' then 'menages_avec_5_personnes'
                
                    when '28' then 'menages_avec_6_et_plus_personnes'
                
                    when '6' then 'menages_avec_6_et_plus_personnes'
                
                    when '3' then 'menages_avec_3_personnes'
                
                    when '26' then 'menages_avec_6_et_plus_personnes'
                
                    when '19' then 'menages_avec_6_et_plus_personnes'
                
                    when '13' then 'menages_avec_6_et_plus_personnes'
                
                    when '16' then 'menages_avec_6_et_plus_personnes'
                
                    when '2' then 'menages_avec_2_personnes'
                
                    when '18' then 'menages_avec_6_et_plus_personnes'
                
                    when '23' then 'menages_avec_6_et_plus_personnes'
                
                    when '4' then 'menages_avec_4_personnes'
                
                    when '14' then 'menages_avec_6_et_plus_personnes'
                
                    when '11' then 'menages_avec_6_et_plus_personnes'
                
                    when '55' then 'menages_avec_6_et_plus_personnes'
                
                    when '34' then 'menages_avec_6_et_plus_personnes'
                
                    when '24' then 'menages_avec_6_et_plus_personnes'
                
                    when '15' then 'menages_avec_6_et_plus_personnes'
                
                    when '10' then 'menages_avec_6_et_plus_personnes'
                
                    when '7' then 'menages_avec_6_et_plus_personnes'
                
                    when '20' then 'menages_avec_6_et_plus_personnes'
                
                    when '12' then 'menages_avec_6_et_plus_personnes'
                
                    when '41' then 'menages_avec_6_et_plus_personnes'
                
            END AS "INPER",
        
            CASE "INP65M"
                
                    when '16' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '55' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '0' then 'menages_avec_0_personne_65_ans_et_plus'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '18' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
            END AS "INP65M",
        
            CASE "INP5M"
                
                    when '4' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when '0' then 'menages_avec_0_personne_5_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
            END AS "INP5M",
        
            CASE "INP24M"
                
                    when '9' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '14' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '0' then 'menages_avec_0_personne_24_ans_et_moins'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '25' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '19' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '20' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '18' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '15' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '21' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '17' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '13' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '16' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
            END AS "INP24M",
        
            CASE "STAT_CONJM"
                
                    when '3' then 'menages_pr_concubinage_union_libre'
                
                    when '5' then 'menages_pr_divorcee'
                
                    when '1' then 'menages_pr_mariee'
                
                    when '6' then 'menages_pr_celibataire'
                
                    when '4' then 'menages_pr_veuve'
                
                    when '2' then 'menages_pr_pacsee'
                
            END AS "STAT_CONJM",
        
            CASE "INP17M"
                
                    when '6' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '12' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '24' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '13' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '0' then 'menages_avec_0_personne_17_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '14' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '17' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '10' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '15' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when '16' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
            END AS "INP17M",
        
            CASE "INP3M"
                
                    when '0' then 'menages_avec_0_personne_3_ans_et_moins'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '5' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
            END AS "INP3M",
        
            CASE "INP75M"
                
                    when '5' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '0' then 'menages_avec_0_personne_75_ans_et_plus'
                
                    when '3' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '11' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '18' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '2' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '48' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '8' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '9' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '6' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '7' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '1' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when '4' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
            END AS "INP75M",
        
            CASE "SEXEM"
                
                    when '2' then 'menages_pr_femmes'
                
                    when '1' then 'menages_pr_homme'
                
            END AS "SEXEM",
        
        CAST(CAST("IPONDL" AS NUMERIC) AS INT) AS poids_du_logement
    
    FROM 
        logement

 )
)

SELECT 
    *
FROM 
    logement_renomee