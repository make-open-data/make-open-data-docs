
  
    

  create  table "defaultdb"."prepare"."activite_communes__dbt_tmp"
  
  
    as
  
  (
    --- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.












with communes as (
    SELECT 
      "COMMUNE" as code_commune_insee,
      CAST( SUM(CAST("IPONDL" AS numeric)) AS INT) AS nombre_de_logements
    FROM 
      "defaultdb"."sources"."logement_2020"
    GROUP BY
      code_commune_insee
  ),
  aggregated as (

    SELECT * 

    FROM communes

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('INEEM' as TEXT) as "champs",
      cast(  
           "INEEM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'INEEM_0'  , 
        
            'INEEM_1'  , 
        
            'INEEM_10'  , 
        
            'INEEM_11'  , 
        
            'INEEM_12'  , 
        
            'INEEM_2'  , 
        
            'INEEM_3'  , 
        
            'INEEM_4'  , 
        
            'INEEM_5'  , 
        
            'INEEM_6'  , 
        
            'INEEM_7'  , 
        
            'INEEM_8'  , 
        
            'INEEM_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INEEM_0' then 'menages_avec_0_eleve_etudiant_14_ans_et_plus'
            
                when 'INEEM_1' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_10' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_11' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_12' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_2' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_3' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_4' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_5' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_6' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_7' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_8' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
                when 'INEEM_9' then 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_0_eleve_etudiant_14_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_eleve_etudiant_14_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('TPM' as TEXT) as "champs",
      cast(  
           "TPM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'TPM_1'  , 
        
            'TPM_2'  , 
        
            'TPM_Z' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'TPM_1' then 'menages_pr_travail_temps_complet'
            
                when 'TPM_2' then 'menages_pr_travail_temps_partiel'
            
                when 'TPM_Z' then 'menages_pr_travail_temps_sans_objet_sans_emploi'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_travail_temps_complet'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_travail_temps_complet"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_travail_temps_partiel'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_travail_temps_partiel"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_travail_temps_sans_objet_sans_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_travail_temps_sans_objet_sans_emploi"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('INPOM' as TEXT) as "champs",
      cast(  
           "INPOM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'INPOM_0'  , 
        
            'INPOM_1'  , 
        
            'INPOM_10'  , 
        
            'INPOM_11'  , 
        
            'INPOM_12'  , 
        
            'INPOM_13'  , 
        
            'INPOM_14'  , 
        
            'INPOM_15'  , 
        
            'INPOM_16'  , 
        
            'INPOM_17'  , 
        
            'INPOM_18'  , 
        
            'INPOM_19'  , 
        
            'INPOM_2'  , 
        
            'INPOM_20'  , 
        
            'INPOM_26'  , 
        
            'INPOM_3'  , 
        
            'INPOM_4'  , 
        
            'INPOM_41'  , 
        
            'INPOM_5'  , 
        
            'INPOM_6'  , 
        
            'INPOM_7'  , 
        
            'INPOM_8'  , 
        
            'INPOM_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INPOM_0' then 'menages_avec_0_personne_active_avec_emploi'
            
                when 'INPOM_1' then 'menages_avec_1_personne_active_avec_emploi'
            
                when 'INPOM_10' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_11' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_12' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_13' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_14' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_15' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_16' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_17' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_18' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_19' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_2' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_20' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_26' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_3' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_4' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_41' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_5' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_6' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_7' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_8' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
                when 'INPOM_9' then 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_0_personne_active_avec_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_active_avec_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_personne_active_avec_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_active_avec_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_actives_avec_emploi"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('DIPLM' as TEXT) as "champs",
      cast(  
           "DIPLM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'DIPLM_1'  , 
        
            'DIPLM_11'  , 
        
            'DIPLM_12'  , 
        
            'DIPLM_13'  , 
        
            'DIPLM_14'  , 
        
            'DIPLM_15'  , 
        
            'DIPLM_16'  , 
        
            'DIPLM_17'  , 
        
            'DIPLM_18'  , 
        
            'DIPLM_19'  , 
        
            'DIPLM_2'  , 
        
            'DIPLM_3' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'DIPLM_1' then 'pr_scolarite_avant_primaire'
            
                when 'DIPLM_11' then 'pr_scolarite_CEP'
            
                when 'DIPLM_12' then 'pr_scolarite_BEPC'
            
                when 'DIPLM_13' then 'pr_scolarite_CAP_BEP'
            
                when 'DIPLM_14' then 'pr_scolarite_bac_general_techno'
            
                when 'DIPLM_15' then 'pr_scolarite_bar_pr'
            
                when 'DIPLM_16' then 'pr_scolarite_bac_plus_2'
            
                when 'DIPLM_17' then 'pr_scolarite_bac_plus_3_ou_4'
            
                when 'DIPLM_18' then 'pr_scolarite_bac_plus_5'
            
                when 'DIPLM_19' then 'pr_scolarite_bac_plus_8'
            
                when 'DIPLM_2' then 'pr_scolarite_avant_college'
            
                when 'DIPLM_3' then 'pr_scolarite_fin_college'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_avant_primaire'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_avant_primaire"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_CEP'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_CEP"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_BEPC'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_BEPC"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_CAP_BEP'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_CAP_BEP"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_bac_general_techno'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_general_techno"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_bar_pr'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_bar_pr"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_bac_plus_2'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_2"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_bac_plus_3_ou_4'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_3_ou_4"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_bac_plus_5'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_5"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_bac_plus_8'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_8"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_avant_college'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_avant_college"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_scolarite_fin_college'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_scolarite_fin_college"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('INPAM' as TEXT) as "champs",
      cast(  
           "INPAM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'INPAM_0'  , 
        
            'INPAM_1'  , 
        
            'INPAM_10'  , 
        
            'INPAM_11'  , 
        
            'INPAM_12'  , 
        
            'INPAM_13'  , 
        
            'INPAM_14'  , 
        
            'INPAM_15'  , 
        
            'INPAM_16'  , 
        
            'INPAM_17'  , 
        
            'INPAM_18'  , 
        
            'INPAM_19'  , 
        
            'INPAM_2'  , 
        
            'INPAM_20'  , 
        
            'INPAM_26'  , 
        
            'INPAM_3'  , 
        
            'INPAM_4'  , 
        
            'INPAM_41'  , 
        
            'INPAM_5'  , 
        
            'INPAM_6'  , 
        
            'INPAM_7'  , 
        
            'INPAM_8'  , 
        
            'INPAM_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INPAM_0' then 'menages_avec_0_personne_active'
            
                when 'INPAM_1' then 'menages_avec_1_personne_active'
            
                when 'INPAM_10' then 'menages_avec_1_personne_active'
            
                when 'INPAM_11' then 'menages_avec_1_personne_active'
            
                when 'INPAM_12' then 'menages_avec_1_personne_active'
            
                when 'INPAM_13' then 'menages_avec_1_personne_active'
            
                when 'INPAM_14' then 'menages_avec_1_personne_active'
            
                when 'INPAM_15' then 'menages_avec_1_personne_active'
            
                when 'INPAM_16' then 'menages_avec_1_personne_active'
            
                when 'INPAM_17' then 'menages_avec_1_personne_active'
            
                when 'INPAM_18' then 'menages_avec_1_personne_active'
            
                when 'INPAM_19' then 'menages_avec_1_personne_active'
            
                when 'INPAM_2' then 'menages_avec_2_et_plus_personnes_actives'
            
                when 'INPAM_20' then 'menages_avec_1_personne_active'
            
                when 'INPAM_26' then 'menages_avec_1_personne_active'
            
                when 'INPAM_3' then 'menages_avec_1_personne_active'
            
                when 'INPAM_4' then 'menages_avec_1_personne_active'
            
                when 'INPAM_41' then 'menages_avec_1_personne_active'
            
                when 'INPAM_5' then 'menages_avec_1_personne_active'
            
                when 'INPAM_6' then 'menages_avec_1_personne_active'
            
                when 'INPAM_7' then 'menages_avec_1_personne_active'
            
                when 'INPAM_8' then 'menages_avec_1_personne_active'
            
                when 'INPAM_9' then 'menages_avec_1_personne_active'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_0_personne_active'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_active"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_personne_active'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_active"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_2_et_plus_personnes_actives'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_actives"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('EMPLM' as TEXT) as "champs",
      cast(  
           "EMPLM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'EMPLM_11'  , 
        
            'EMPLM_12'  , 
        
            'EMPLM_13'  , 
        
            'EMPLM_14'  , 
        
            'EMPLM_15'  , 
        
            'EMPLM_16'  , 
        
            'EMPLM_21'  , 
        
            'EMPLM_22'  , 
        
            'EMPLM_23'  , 
        
            'EMPLM_ZZ' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'EMPLM_11' then 'pr_emploi_apprentissage'
            
                when 'EMPLM_12' then 'pr_emploi_interim'
            
                when 'EMPLM_13' then 'pr_emploi_contrat_aide'
            
                when 'EMPLM_14' then 'pr_emploi_stage_entreprise'
            
                when 'EMPLM_15' then 'pr_emploi_duree_limite'
            
                when 'EMPLM_16' then 'pr_emploi_sans_duree_limite'
            
                when 'EMPLM_21' then 'pr_emploi_independant'
            
                when 'EMPLM_22' then 'pr_emploi_employeur'
            
                when 'EMPLM_23' then 'pr_emploi_aides_familiaux'
            
                when 'EMPLM_ZZ' then 'pr_emploi_sans_objet'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_apprentissage'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_apprentissage"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_interim'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_interim"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_contrat_aide'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_contrat_aide"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_stage_entreprise'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_stage_entreprise"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_duree_limite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_duree_limite"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_sans_duree_limite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_sans_duree_limite"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_independant'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_independant"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_employeur'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_employeur"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_aides_familiaux'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_aides_familiaux"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'pr_emploi_sans_objet'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "pr_emploi_sans_objet"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('RECHM' as TEXT) as "champs",
      cast(  
           "RECHM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'RECHM_0'  , 
        
            'RECHM_1'  , 
        
            'RECHM_2'  , 
        
            'RECHM_9'  , 
        
            'RECHM_Z' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'RECHM_0' then 'menages_pr_pas_de_recherche_emploi'
            
                when 'RECHM_1' then 'menages_pr_recherche_emploi_moins_un_an'
            
                when 'RECHM_2' then 'menages_pr_recherche_emploi_plus_un_an'
            
                when 'RECHM_9' then 'menages_pr_recherche_emploi_non_declaree'
            
                when 'RECHM_Z' then 'menages_pr_recherche_emploi_sans_objet_en_emploi'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_pas_de_recherche_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_pas_de_recherche_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_recherche_emploi_moins_un_an'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_moins_un_an"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_recherche_emploi_plus_un_an'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_plus_un_an"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_recherche_emploi_non_declaree'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_non_declaree"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_recherche_emploi_sans_objet_en_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_sans_objet_en_emploi"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('TACTM' as TEXT) as "champs",
      cast(  
           "TACTM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'TACTM_11'  , 
        
            'TACTM_12'  , 
        
            'TACTM_21'  , 
        
            'TACTM_22'  , 
        
            'TACTM_24'  , 
        
            'TACTM_25' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'TACTM_11' then 'menages_pr_activite_emploi'
            
                when 'TACTM_12' then 'menages_pr_activite_chomeurs'
            
                when 'TACTM_21' then 'menages_pr_activite_retraite_pre_retraite'
            
                when 'TACTM_22' then 'menages_pr_activite_eleve'
            
                when 'TACTM_24' then 'menages_pr_activite_au_foyer'
            
                when 'TACTM_25' then 'menages_pr_activite_autre'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_activite_emploi'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_activite_emploi"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_activite_chomeurs'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_activite_chomeurs"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_activite_retraite_pre_retraite'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_activite_retraite_pre_retraite"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_activite_eleve'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_activite_eleve"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_activite_au_foyer'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_activite_au_foyer"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_pr_activite_autre'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_pr_activite_autre"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

      LEFT JOIN ( 







with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  


    select
        "COMMUNE",
        "IPONDL",
        "IRIS",

      cast('INPSM' as TEXT) as "champs",
      cast(  
           "INPSM"
             
           as varchar) as "valeur"

    from "defaultdb"."sources"."logement_2020"

    

), 
unpivot_filtree as (
        

    SELECT
        "COMMUNE" as  code_commune_insee, 
        CAST("IPONDL" as NUMERIC) as poids_du_logement,
        champs || '_' || valeur AS champs_valeur
    FROM
        unpivoted
    WHERE
        champs || '_' || valeur in (
        
            'INPSM_0'  , 
        
            'INPSM_1'  , 
        
            'INPSM_10'  , 
        
            'INPSM_11'  , 
        
            'INPSM_12'  , 
        
            'INPSM_13'  , 
        
            'INPSM_14'  , 
        
            'INPSM_15'  , 
        
            'INPSM_16'  , 
        
            'INPSM_2'  , 
        
            'INPSM_25'  , 
        
            'INPSM_3'  , 
        
            'INPSM_4'  , 
        
            'INPSM_5'  , 
        
            'INPSM_6'  , 
        
            'INPSM_7'  , 
        
            'INPSM_8'  , 
        
            'INPSM_9' 
        )


),
renommee as (
        
    SELECT
        code_commune_insee,
        CASE champs_valeur
            
                when 'INPSM_0' then 'menages_avec_0_personne_scolarisee'
            
                when 'INPSM_1' then 'menages_avec_1_personne_scolarisee'
            
                when 'INPSM_10' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_11' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_12' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_13' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_14' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_15' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_16' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_2' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_25' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_3' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_4' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_5' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_6' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_7' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_8' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
                when 'INPSM_9' then 'menages_avec_2_et_plus_personnes_scolarisees'
            
        END AS champs_valeur_renomme,
        CAST(SUM(CAST(poids_du_logement as NUMERIC)) AS INT) as population_par_champs_valeur
    FROM
        unpivot_filtree
    GROUP BY
        code_commune_insee,
        champs_valeur_renomme

),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_commune_insee,
    
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_0_personne_scolarisee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_scolarisee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_1_personne_scolarisee'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_scolarisee"
      
    
    ,
  
    sum(
      
      case
      when champs_valeur_renomme = 'menages_avec_2_et_plus_personnes_scolarisees'
        then population_par_champs_valeur
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_scolarisees"
      
    
    
  

    from 
        renommee
    group by
        code_commune_insee


)

select * from pivoted
  )
      USING (code_commune_insee)

    

  ),
  aggregated_with_infos_communes as (
    SELECT
      *
    FROM
      aggregated
    JOIN
	    "defaultdb"."prepare"."infos_communes" as infos_communes
    ON
      aggregated.code_commune_insee = infos_communes.code_commune
  )

SELECT 
    *  
FROM
    aggregated_with_infos_communes
  );
  