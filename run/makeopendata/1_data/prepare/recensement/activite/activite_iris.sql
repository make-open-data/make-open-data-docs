
  
    

  create  table "defaultdb"."prepare"."activite_iris__dbt_tmp"
  
  
    as
  
  (
    --- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"

--- Agrège les données de la base logement par commune
--- Colonne par colonne pour ne pas saturer la mémoire
--- L'agrégration est faite par univot/pivot.



with aggregated as (
  





with poids_par_geo as (
    SELECT 
      code_iris,
      CAST( SUM(CAST(poids_du_logement AS numeric)) AS INT) AS nombre_de_logements
    FROM
      "defaultdb"."intermediaires"."activite_renomee"
    GROUP BY
      code_iris
  ),
  aggregated as (

    SELECT * 

    FROM poids_par_geo

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INEEM' as TEXT) as "champs",
      cast(  
           "INEEM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_avec_0_eleve_etudiant_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'  , 
        
            'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_eleve_etudiant_14_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_eleve_etudiant_14_ans_et_plus"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_INEEM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('TPM' as TEXT) as "champs",
      cast(  
           "TPM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_pr_travail_temps_complet'  , 
        
            'menages_pr_travail_temps_partiel'  , 
        
            'menages_pr_travail_temps_sans_objet_sans_emploi' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_pr_travail_temps_complet'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_travail_temps_complet"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_travail_temps_partiel'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_travail_temps_partiel"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_travail_temps_sans_objet_sans_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_travail_temps_sans_objet_sans_emploi"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_TPM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INPOM' as TEXT) as "champs",
      cast(  
           "INPOM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_avec_0_personne_active_avec_emploi'  , 
        
            'menages_avec_1_personne_active_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi'  , 
        
            'menages_avec_2_et_plus_personnes_actives_avec_emploi' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_active_avec_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_active_avec_emploi"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_personne_active_avec_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_active_avec_emploi"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_2_et_plus_personnes_actives_avec_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_actives_avec_emploi"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_INPOM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('DIPLM' as TEXT) as "champs",
      cast(  
           "DIPLM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_scolarite_avant_primaire'  , 
        
            'pr_scolarite_CEP'  , 
        
            'pr_scolarite_BEPC'  , 
        
            'pr_scolarite_CAP_BEP'  , 
        
            'pr_scolarite_bac_general_techno'  , 
        
            'pr_scolarite_bar_pr'  , 
        
            'pr_scolarite_bac_plus_2'  , 
        
            'pr_scolarite_bac_plus_3_ou_4'  , 
        
            'pr_scolarite_bac_plus_5'  , 
        
            'pr_scolarite_bac_plus_8'  , 
        
            'pr_scolarite_avant_college'  , 
        
            'pr_scolarite_fin_college' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'pr_scolarite_avant_primaire'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_avant_primaire"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_CEP'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_CEP"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_BEPC'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_BEPC"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_CAP_BEP'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_CAP_BEP"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_bac_general_techno'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_general_techno"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_bar_pr'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_bar_pr"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_bac_plus_2'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_2"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_bac_plus_3_ou_4'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_3_ou_4"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_bac_plus_5'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_5"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_bac_plus_8'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_bac_plus_8"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_avant_college'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_avant_college"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_scolarite_fin_college'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_scolarite_fin_college"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_DIPLM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INPAM' as TEXT) as "champs",
      cast(  
           "INPAM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_avec_0_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_2_et_plus_personnes_actives'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active'  , 
        
            'menages_avec_1_personne_active' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_active'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_active"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_personne_active'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_active"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_2_et_plus_personnes_actives'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_actives"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_INPAM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('EMPLM' as TEXT) as "champs",
      cast(  
           "EMPLM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'pr_emploi_apprentissage'  , 
        
            'pr_emploi_interim'  , 
        
            'pr_emploi_contrat_aide'  , 
        
            'pr_emploi_stage_entreprise'  , 
        
            'pr_emploi_duree_limite'  , 
        
            'pr_emploi_sans_duree_limite'  , 
        
            'pr_emploi_independant'  , 
        
            'pr_emploi_employeur'  , 
        
            'pr_emploi_aides_familiaux'  , 
        
            'pr_emploi_sans_objet' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'pr_emploi_apprentissage'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_apprentissage"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_interim'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_interim"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_contrat_aide'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_contrat_aide"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_stage_entreprise'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_stage_entreprise"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_duree_limite'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_duree_limite"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_sans_duree_limite'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_sans_duree_limite"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_independant'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_independant"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_employeur'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_employeur"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_aides_familiaux'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_aides_familiaux"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'pr_emploi_sans_objet'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "pr_emploi_sans_objet"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_EMPLM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('RECHM' as TEXT) as "champs",
      cast(  
           "RECHM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_pr_pas_de_recherche_emploi'  , 
        
            'menages_pr_recherche_emploi_moins_un_an'  , 
        
            'menages_pr_recherche_emploi_plus_un_an'  , 
        
            'menages_pr_recherche_emploi_non_declaree'  , 
        
            'menages_pr_recherche_emploi_sans_objet_en_emploi' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_pr_pas_de_recherche_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_pas_de_recherche_emploi"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_recherche_emploi_moins_un_an'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_moins_un_an"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_recherche_emploi_plus_un_an'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_plus_un_an"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_recherche_emploi_non_declaree'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_non_declaree"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_recherche_emploi_sans_objet_en_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_recherche_emploi_sans_objet_en_emploi"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_RECHM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  
    
      
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('TACTM' as TEXT) as "champs",
      cast(  
           "TACTM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_pr_activite_emploi'  , 
        
            'menages_pr_activite_chomeurs'  , 
        
            'menages_pr_activite_retraite_pre_retraite'  , 
        
            'menages_pr_activite_eleve'  , 
        
            'menages_pr_activite_au_foyer'  , 
        
            'menages_pr_activite_autre' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      
        
            
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_pr_activite_emploi'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_activite_emploi"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_activite_chomeurs'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_activite_chomeurs"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_activite_retraite_pre_retraite'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_activite_retraite_pre_retraite"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_activite_eleve'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_activite_eleve"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_activite_au_foyer'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_activite_au_foyer"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_pr_activite_autre'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_pr_activite_autre"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_TACTM_par_geo
      USING (code_iris)

    

      LEFT JOIN ( 






with unpivoted as (
      


-- Prends toutes les colonnes sauf la colonne à considérer, pour donne en paramètre à unpivot
  

  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
      
    
  
    
  

  select
        "poids_du_logement",
        "code_commune_insee",
        "code_iris",

      cast('INPSM' as TEXT) as "champs",
      cast(  
           "INPSM"
             
           as varchar) as "valeur"

    from "defaultdb"."intermediaires"."activite_renomee"

    



), 
unpivot_filtree as (
        

    SELECT
        code_iris, 
        poids_du_logement,
        valeur
    FROM
        unpivoted
    WHERE
        valeur in (
        
            'menages_avec_0_personne_scolarisee'  , 
        
            'menages_avec_1_personne_scolarisee'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees'  , 
        
            'menages_avec_2_et_plus_personnes_scolarisees' 
        )


),
pivoted as (
        

    
    
        
            
        
      
        
            
        
      
        
            
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      
        
      

    select 

    code_iris,
    
  
    sum(
      
      case
      when valeur = 'menages_avec_0_personne_scolarisee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_0_personne_scolarisee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_1_personne_scolarisee'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_1_personne_scolarisee"
      
    
    ,
  
    sum(
      
      case
      when valeur = 'menages_avec_2_et_plus_personnes_scolarisees'
        then poids_du_logement
      else 0
      end
    )
    
      
            as "menages_avec_2_et_plus_personnes_scolarisees"
      
    
    
  

    from 
        unpivot_filtree
    group by
        code_iris


)

select * from pivoted
  ) as alias_INPSM_par_geo
      USING (code_iris)

    

  )

select * from aggregated

)

SELECT 
    *  
FROM
    aggregated
  );
  