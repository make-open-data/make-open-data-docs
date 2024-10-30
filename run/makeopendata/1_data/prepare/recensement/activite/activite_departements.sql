
  
    

  create  table "defaultdb"."prepare"."activite_departements__dbt_tmp"
  
  
    as
  
  (
    --- depends_on: "defaultdb"."prepare"."logement_2020_valeurs"




select recensement_theme_departement.* ,
       infos_departements.nom_departement,
       infos_departements.code_region,
       infos_departements.nom_region,
       infos_departements.population_departement,
       infos_departements.contour_departement 
from (








    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    

    



select code_departement,

    SUM("nombre_de_menage_base_ou_logements_occupee") as "nombre_de_menage_base_ou_logements_occupee"
     , 

    SUM("nombre_de_logements_occasionnels") as "nombre_de_logements_occasionnels"
     , 

    SUM("nombre_de_logements_residences_secondaires") as "nombre_de_logements_residences_secondaires"
     , 

    SUM("nombre_de_logements_vacants") as "nombre_de_logements_vacants"
     , 

    SUM("nombre_de_logements_total_tous_status_occupation") as "nombre_de_logements_total_tous_status_occupation"
     , 

    SUM("menages_pr_travail_temps_partiel") as "menages_pr_travail_temps_partiel"
     , 

    SUM("menages_pr_activite_chomeurs") as "menages_pr_activite_chomeurs"
     , 

    SUM("pr_scolarite_avant_college") as "pr_scolarite_avant_college"
     , 

    SUM("menages_avec_2_et_plus_personnes_actives_avec_emploi") as "menages_avec_2_et_plus_personnes_actives_avec_emploi"
     , 

    SUM("pr_emploi_independant") as "pr_emploi_independant"
     , 

    SUM("menages_avec_0_personne_scolarisee") as "menages_avec_0_personne_scolarisee"
     , 

    SUM("pr_scolarite_bar_pr") as "pr_scolarite_bar_pr"
     , 

    SUM("menages_pr_travail_temps_sans_objet_sans_emploi") as "menages_pr_travail_temps_sans_objet_sans_emploi"
     , 

    SUM("pr_emploi_apprentissage") as "pr_emploi_apprentissage"
     , 

    SUM("menages_avec_0_personne_active_avec_emploi") as "menages_avec_0_personne_active_avec_emploi"
     , 

    SUM("menages_pr_recherche_emploi_non_declaree") as "menages_pr_recherche_emploi_non_declaree"
     , 

    SUM("pr_scolarite_avant_primaire") as "pr_scolarite_avant_primaire"
     , 

    SUM("pr_emploi_employeur") as "pr_emploi_employeur"
     , 

    SUM("pr_emploi_sans_objet") as "pr_emploi_sans_objet"
     , 

    SUM("menages_pr_activite_autre") as "menages_pr_activite_autre"
     , 

    SUM("pr_scolarite_CAP_BEP") as "pr_scolarite_CAP_BEP"
     , 

    SUM("menages_pr_activite_eleve") as "menages_pr_activite_eleve"
     , 

    SUM("pr_emploi_sans_duree_limite") as "pr_emploi_sans_duree_limite"
     , 

    SUM("menages_pr_recherche_emploi_moins_un_an") as "menages_pr_recherche_emploi_moins_un_an"
     , 

    SUM("menages_avec_2_et_plus_personnes_scolarisees") as "menages_avec_2_et_plus_personnes_scolarisees"
     , 

    SUM("pr_scolarite_bac_plus_5") as "pr_scolarite_bac_plus_5"
     , 

    SUM("pr_scolarite_bac_plus_2") as "pr_scolarite_bac_plus_2"
     , 

    SUM("pr_scolarite_bac_plus_8") as "pr_scolarite_bac_plus_8"
     , 

    SUM("pr_emploi_duree_limite") as "pr_emploi_duree_limite"
     , 

    SUM("menages_pr_pas_de_recherche_emploi") as "menages_pr_pas_de_recherche_emploi"
     , 

    SUM("menages_avec_0_eleve_etudiant_14_ans_et_plus") as "menages_avec_0_eleve_etudiant_14_ans_et_plus"
     , 

    SUM("menages_pr_travail_temps_complet") as "menages_pr_travail_temps_complet"
     , 

    SUM("pr_scolarite_fin_college") as "pr_scolarite_fin_college"
     , 

    SUM("menages_pr_activite_au_foyer") as "menages_pr_activite_au_foyer"
     , 

    SUM("menages_pr_activite_retraite_pre_retraite") as "menages_pr_activite_retraite_pre_retraite"
     , 

    SUM("menages_avec_2_et_plus_personnes_actives") as "menages_avec_2_et_plus_personnes_actives"
     , 

    SUM("menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus") as "menages_avec_1_et_plus_eleves_etudiants_14_ans_et_plus"
     , 

    SUM("pr_emploi_interim") as "pr_emploi_interim"
     , 

    SUM("pr_scolarite_BEPC") as "pr_scolarite_BEPC"
     , 

    SUM("pr_emploi_contrat_aide") as "pr_emploi_contrat_aide"
     , 

    SUM("menages_pr_activite_emploi") as "menages_pr_activite_emploi"
     , 

    SUM("pr_scolarite_CEP") as "pr_scolarite_CEP"
     , 

    SUM("menages_avec_1_personne_active") as "menages_avec_1_personne_active"
     , 

    SUM("menages_pr_recherche_emploi_plus_un_an") as "menages_pr_recherche_emploi_plus_un_an"
     , 

    SUM("menages_avec_0_personne_active") as "menages_avec_0_personne_active"
     , 

    SUM("pr_emploi_aides_familiaux") as "pr_emploi_aides_familiaux"
     , 

    SUM("pr_emploi_stage_entreprise") as "pr_emploi_stage_entreprise"
     , 

    SUM("menages_pr_recherche_emploi_sans_objet_en_emploi") as "menages_pr_recherche_emploi_sans_objet_en_emploi"
     , 

    SUM("menages_avec_1_personne_scolarisee") as "menages_avec_1_personne_scolarisee"
     , 

    SUM("pr_scolarite_bac_plus_3_ou_4") as "pr_scolarite_bac_plus_3_ou_4"
     , 

    SUM("pr_scolarite_bac_general_techno") as "pr_scolarite_bac_general_techno"
     , 

    SUM("menages_avec_1_personne_active_avec_emploi") as "menages_avec_1_personne_active_avec_emploi"
    

from "defaultdb"."prepare"."activite_communes"
where code_departement is not null --- A enlever après avoir bien milesimé les données recensement (i.e. pas de commune manquante)
group by code_departement

) recensement_theme_departement
left join "defaultdb"."prepare"."infos_departements" infos_departements 
using (code_departement)
  );
  