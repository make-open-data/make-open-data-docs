

with filosofi_commune as (
    select * 
    from "defaultdb"."sources"."filosofi_commune_2021"

),
renamed_columns as (
    select
        "CODGEO" as code_commune_2024,
        
        cast(nullif(nullif(nullif(nullif(replace("NBMENFISC21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as nombre_menages_fiscaux,
        
        cast(nullif(nullif(nullif(nullif(replace("NBPERSMENFISC21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as nombre_personnes,
        
        cast(nullif(nullif(nullif(nullif(replace("MED21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as mediane_niveau_vie_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("PIMP21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_menages_fiscaux_imposes_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP6021", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_ensemble_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60AGE121", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_moins_30_ans_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60AGE221", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_30_39_ans_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60AGE321", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_40_49_ans_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60AGE421", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_50_59_ans_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60AGE521", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_60_74_ans_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60AGE621", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_75_ans_ou_plus_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60TOL121", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_proprietaires_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("TP60TOL221", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as taux_pauvrete_locataires_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PACT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_revenus_activite_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PTSA21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_salaires_traitements_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PCHO21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_indemnites_chomage_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PBEN21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_revenus_activites_non_salariees_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PPEN21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_pensions,
        
        cast(nullif(nullif(nullif(nullif(replace("PPAT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_revenus_patrimoine_autres_revenus_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PPSOC21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_ensemble_prestations_sociales_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PPFAM21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_prestations_familiales_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PPMINI21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_minima_sociaux_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PPLOGT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_prestations_logement_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("PIMPOT21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as part_des_impots_pourcentage,
        
        cast(nullif(nullif(nullif(nullif(replace("D121", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_1_niveau_vie_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("D921", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as decile_9_niveau_vie_euro,
        
        cast(nullif(nullif(nullif(nullif(replace("RD21", ',', '.'), 'ns'), 'so'), 's'), 'nd') as NUMERIC)
 as rapport_interdecile_9_1
    from filosofi_commune
),
aggregated_with_infos_communes as (
    SELECT
      *
    FROM
      renamed_columns
    JOIN
	    "defaultdb"."prepare"."infos_communes" as infos_communes
    ON
      renamed_columns.code_commune_2024 = infos_communes.code_commune
  )

select * from aggregated_with_infos_communes