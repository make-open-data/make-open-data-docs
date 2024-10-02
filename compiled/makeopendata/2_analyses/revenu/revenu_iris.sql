-- L'insee ne fournit que les infos sur les iris des communes irisées
-- Ajouter les autres communes est impossible car la table commune ne reporte pas les mêmes champs
-- Aussi, revenu par isis utilise la géographie 2022 qu'il convient de transposer avec la géographie 2024
-- 

with filosofi_iris as (
    select * 
    from "defaultdb"."sources"."filosofi_iris_2021"

),
renamed_columns as (
    select
        "IRIS" as code_iris_2022_filosofi,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PIMP21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_menages_fiscaux_imposes_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_TP6021", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_bas_revenus_declares_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_INCERT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as incertitude,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_Q121", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as quartile_1_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_MED21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as mediane_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_Q321", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as quartile_3_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_EQ21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as ecart_interquartile_rapporte_mediane,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D121", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_1_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D221", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_2_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D321", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_3_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D421", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_4_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D621", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_6_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D721", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_7_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D821", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_8_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_D921", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_9_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_RD21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as rapport_interdecile_d9_d1,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_S80S2021", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as s80_s20,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_GI21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as indice_gini,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PACT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_revenus_activite_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PTSA21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_salaires_traitements_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PCHO21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_indemnites_chomage_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PBEN21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_revenus_activites_non_salariees_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PPEN21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_pensions_retraites_rentes_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("DEC_PAUT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_autres_revenus_pourcentage
    from filosofi_iris
), 
aggregated_with_infos_iris as (
    SELECT
      *
    FROM
      renamed_columns
    JOIN
	    "defaultdb"."prepare"."infos_iris" as infos_iris
    ON
      renamed_columns.code_iris_2024_filosofi = infos_iris.code_iris_2024
  )

select * from aggregated_with_infos_iris