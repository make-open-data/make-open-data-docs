
  
    

  create  table "defaultdb"."prepare"."demographie_departements__dbt_tmp"
  
  
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

    SUM("menages_avec_0_personne_11_ans_et_moins") as "menages_avec_0_personne_11_ans_et_moins"
     , 

    SUM("pr_age_15_19") as "pr_age_15_19"
     , 

    SUM("menages_avec_1_et_plus_personnes_5_ans_et_moins") as "menages_avec_1_et_plus_personnes_5_ans_et_moins"
     , 

    SUM("menages_avec_1_et_plus_personnes_19_ans_et_moins") as "menages_avec_1_et_plus_personnes_19_ans_et_moins"
     , 

    SUM("pr_age_64_79") as "pr_age_64_79"
     , 

    SUM("pr_age_moins_15") as "pr_age_moins_15"
     , 

    SUM("menages_avec_1_et_plus_personnes_3_ans_et_moins") as "menages_avec_1_et_plus_personnes_3_ans_et_moins"
     , 

    SUM("menages_pr_divorcee") as "menages_pr_divorcee"
     , 

    SUM("menages_pr_concubinage_union_libre") as "menages_pr_concubinage_union_libre"
     , 

    SUM("menages_pr_veuve") as "menages_pr_veuve"
     , 

    SUM("menages_avec_1_et_plus_personnes_75_ans_et_plus") as "menages_avec_1_et_plus_personnes_75_ans_et_plus"
     , 

    SUM("menages_avec_3_personnes") as "menages_avec_3_personnes"
     , 

    SUM("menages_avec_4_personnes") as "menages_avec_4_personnes"
     , 

    SUM("menages_avec_1_personne_masculin") as "menages_avec_1_personne_masculin"
     , 

    SUM("menages_avec_0_personne_65_ans_et_plus") as "menages_avec_0_personne_65_ans_et_plus"
     , 

    SUM("menages_avec_1_et_plus_personnes_24_ans_et_moins") as "menages_avec_1_et_plus_personnes_24_ans_et_moins"
     , 

    SUM("menages_avec_1_et_plus_personnes_17_ans_et_moins") as "menages_avec_1_et_plus_personnes_17_ans_et_moins"
     , 

    SUM("pr_age_20_24") as "pr_age_20_24"
     , 

    SUM("menages_avec_1_et_plus_personnes_11_ans_et_moins") as "menages_avec_1_et_plus_personnes_11_ans_et_moins"
     , 

    SUM("menages_avec_0_personne_60_ans_et_plus") as "menages_avec_0_personne_60_ans_et_plus"
     , 

    SUM("menages_avec_2_et_plus_personnes_masculin") as "menages_avec_2_et_plus_personnes_masculin"
     , 

    SUM("menages_avec_0_personne_feminin") as "menages_avec_0_personne_feminin"
     , 

    SUM("menages_avec_0_personne_19_ans_et_moins") as "menages_avec_0_personne_19_ans_et_moins"
     , 

    SUM("pr_age_plus_80") as "pr_age_plus_80"
     , 

    SUM("pr_age_55_64") as "pr_age_55_64"
     , 

    SUM("menages_pr_mariee") as "menages_pr_mariee"
     , 

    SUM("menages_avec_5_personnes") as "menages_avec_5_personnes"
     , 

    SUM("menages_avec_1_et_plus_personnes_60_ans_et_plus") as "menages_avec_1_et_plus_personnes_60_ans_et_plus"
     , 

    SUM("menages_avec_1_personne") as "menages_avec_1_personne"
     , 

    SUM("menages_avec_2_personnes") as "menages_avec_2_personnes"
     , 

    SUM("menages_pr_celibataire") as "menages_pr_celibataire"
     , 

    SUM("menages_pr_femmes") as "menages_pr_femmes"
     , 

    SUM("menages_avec_0_personne_24_ans_et_moins") as "menages_avec_0_personne_24_ans_et_moins"
     , 

    SUM("menages_pr_homme") as "menages_pr_homme"
     , 

    SUM("menages_avec_1_et_plus_personnes_15_ans_et_moins") as "menages_avec_1_et_plus_personnes_15_ans_et_moins"
     , 

    SUM("menages_avec_0_personne_masculin") as "menages_avec_0_personne_masculin"
     , 

    SUM("menages_avec_6_et_plus_personnes") as "menages_avec_6_et_plus_personnes"
     , 

    SUM("menages_avec_0_personne_75_ans_et_plus") as "menages_avec_0_personne_75_ans_et_plus"
     , 

    SUM("menages_avec_0_personne_15_ans_et_moins") as "menages_avec_0_personne_15_ans_et_moins"
     , 

    SUM("menages_avec_0_personne_5_ans_et_moins") as "menages_avec_0_personne_5_ans_et_moins"
     , 

    SUM("menages_avec_0_personne_3_ans_et_moins") as "menages_avec_0_personne_3_ans_et_moins"
     , 

    SUM("menages_avec_0_personne_17_ans_et_moins") as "menages_avec_0_personne_17_ans_et_moins"
     , 

    SUM("menages_avec_1_et_plus_personnes_65_ans_et_plus") as "menages_avec_1_et_plus_personnes_65_ans_et_plus"
     , 

    SUM("menages_avec_1_personne_feminin") as "menages_avec_1_personne_feminin"
     , 

    SUM("pr_age_25_39") as "pr_age_25_39"
     , 

    SUM("menages_pr_pacsee") as "menages_pr_pacsee"
     , 

    SUM("pr_age_40_54") as "pr_age_40_54"
    

from "defaultdb"."prepare"."demographie_communes"
where code_departement is not null --- A enlever après avoir bien milesimé les données recensement (i.e. pas de commune manquante)
group by code_departement

) recensement_theme_departement
left join "defaultdb"."prepare"."infos_departements" infos_departements 
using (code_departement)
  );
  