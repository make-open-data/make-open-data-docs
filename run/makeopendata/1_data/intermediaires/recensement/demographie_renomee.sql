
  create view "defaultdb"."intermediaires"."demographie_renomee__dbt_tmp"
    
    
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
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '020' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '016' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '015' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '025' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '014' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '013' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '019' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '018' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '021' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
                    when LPAD(CAST("INP24M" AS TEXT), 3, '0') = '017' then 'menages_avec_1_et_plus_personnes_24_ans_et_moins'
                
            END AS "INP24M",
        
            CASE
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '020' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '007' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '010' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '004' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '015' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '022' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '006' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '011' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '017' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '013' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '003' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '012' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '019' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '008' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '038' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '001' then 'menages_avec_1_personne_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '005' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '018' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '002' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '016' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '026' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '014' then 'menages_avec_2_et_plus_personnes_masculin'
                
                    when LPAD(CAST("INPER1" AS TEXT), 3, '0') = '009' then 'menages_avec_2_et_plus_personnes_masculin'
                
            END AS "INPER1",
        
            CASE
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_3_ans_et_moins'
                
                    when LPAD(CAST("INP3M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_3_ans_et_moins'
                
            END AS "INP3M",
        
            CASE
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '015' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '014' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '013' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
                    when LPAD(CAST("INP15M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_15_ans_et_moins'
                
            END AS "INP15M",
        
            CASE
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '016' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '041' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '014' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '020' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '055' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '025' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '019' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '022' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '015' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '023' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '013' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '018' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '017' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '021' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
                    when LPAD(CAST("INP19M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_19_ans_et_moins'
                
            END AS "INP19M",
        
            CASE
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '012' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '001' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '008' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '007' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '011' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '003' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '014' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '039' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '015' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '004' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '010' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '017' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '013' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '016' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '020' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '006' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '005' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '009' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '019' then 'menages_avec_1_personne_feminin'
                
                    when LPAD(CAST("INPER2" AS TEXT), 3, '0') = '002' then 'menages_avec_1_personne_feminin'
                
            END AS "INPER2",
        
            CASE
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
                    when LPAD(CAST("INP5M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_5_ans_et_moins'
                
            END AS "INP5M",
        
            CASE
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '013' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '014' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_11_ans_et_moins'
                
                    when LPAD(CAST("INP11M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_11_ans_et_moins'
                
            END AS "INP11M",
        
            CASE
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '015' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '025' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '014' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '016' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '055' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '023' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '010' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '021' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '017' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '013' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '003' then 'menages_avec_3_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '020' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '005' then 'menages_avec_5_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '008' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '001' then 'menages_avec_1_personne'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '018' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '028' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '002' then 'menages_avec_2_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '041' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '006' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '034' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '019' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '004' then 'menages_avec_4_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '009' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '022' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '026' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '011' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '007' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '012' then 'menages_avec_6_et_plus_personnes'
                
                    when LPAD(CAST("INPER" AS TEXT), 3, '0') = '024' then 'menages_avec_6_et_plus_personnes'
                
            END AS "INPER",
        
            CASE
                
                    when LPAD(CAST("STAT_CONJM" AS TEXT), 3, '0') = '004' then 'menages_pr_veuve'
                
                    when LPAD(CAST("STAT_CONJM" AS TEXT), 3, '0') = '003' then 'menages_pr_concubinage_union_libre'
                
                    when LPAD(CAST("STAT_CONJM" AS TEXT), 3, '0') = '002' then 'menages_pr_pacsee'
                
                    when LPAD(CAST("STAT_CONJM" AS TEXT), 3, '0') = '005' then 'menages_pr_divorcee'
                
                    when LPAD(CAST("STAT_CONJM" AS TEXT), 3, '0') = '006' then 'menages_pr_celibataire'
                
                    when LPAD(CAST("STAT_CONJM" AS TEXT), 3, '0') = '001' then 'menages_pr_mariee'
                
            END AS "STAT_CONJM",
        
            CASE
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '017' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '019' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '055' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
                    when LPAD(CAST("INP60M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_60_ans_et_plus'
                
            END AS "INP60M",
        
            CASE
                
                    when LPAD(CAST("SEXEM" AS TEXT), 3, '0') = '002' then 'menages_pr_femmes'
                
                    when LPAD(CAST("SEXEM" AS TEXT), 3, '0') = '001' then 'menages_pr_homme'
                
            END AS "SEXEM",
        
            CASE
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '048' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '018' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
                    when LPAD(CAST("INP75M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_75_ans_et_plus'
                
            END AS "INP75M",
        
            CASE
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '024' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '015' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '014' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '017' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '013' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '010' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '016' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
                    when LPAD(CAST("INP17M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_17_ans_et_moins'
                
            END AS "INP17M",
        
            CASE
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '004' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '018' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '001' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '011' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '007' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '055' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '009' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '008' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '006' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '002' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '003' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '012' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '000' then 'menages_avec_0_personne_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '016' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
                    when LPAD(CAST("INP65M" AS TEXT), 3, '0') = '005' then 'menages_avec_1_et_plus_personnes_65_ans_et_plus'
                
            END AS "INP65M",
        
            CASE
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '015' then 'pr_age_15_19'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '080' then 'pr_age_plus_80'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '020' then 'pr_age_20_24'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '040' then 'pr_age_40_54'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '000' then 'pr_age_moins_15'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '055' then 'pr_age_55_64'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '025' then 'pr_age_25_39'
                
                    when LPAD(CAST("AGEMEN8" AS TEXT), 3, '0') = '065' then 'pr_age_64_79'
                
            END AS "AGEMEN8",
        
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