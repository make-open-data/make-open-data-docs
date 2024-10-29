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

    SUM("pr_travail_hors_region_actuelle_metropole") as "pr_travail_hors_region_actuelle_metropole"
     , 

    SUM("menages_avec_pr_annee_avant_autre_region_com") as "menages_avec_pr_annee_avant_autre_region_com"
     , 

    SUM("menages_pr_transport_travail_transport_commune") as "menages_pr_transport_travail_transport_commune"
     , 

    SUM("pr_naissance_department_actuelle") as "pr_naissance_department_actuelle"
     , 

    SUM("pr_etudes_etranger") as "pr_etudes_etranger"
     , 

    SUM("pr_etudes_hors_region_actuelle_com") as "pr_etudes_hors_region_actuelle_com"
     , 

    SUM("menages_0_voiture") as "menages_0_voiture"
     , 

    SUM("menages_pr_transport_travail_deux_roues") as "menages_pr_transport_travail_deux_roues"
     , 

    SUM("menages_avec_pr_annee_avant_autre_region_metro") as "menages_avec_pr_annee_avant_autre_region_metro"
     , 

    SUM("pr_etudes_hors_region_actuelle_metropole") as "pr_etudes_hors_region_actuelle_metropole"
     , 

    SUM("pr_etudes_department_region_actuel") as "pr_etudes_department_region_actuel"
     , 

    SUM("menages_avec_pr_annee_avant_meme_commune") as "menages_avec_pr_annee_avant_meme_commune"
     , 

    SUM("pr_naissance_hors_region_actuelle_metropole") as "pr_naissance_hors_region_actuelle_metropole"
     , 

    SUM("pr_immigration_non_immigre") as "pr_immigration_non_immigre"
     , 

    SUM("menages_1_voiture") as "menages_1_voiture"
     , 

    SUM("dom__trois_ou_plus_deux_roues") as "dom__trois_ou_plus_deux_roues"
     , 

    SUM("menages_pr_transport_travail_velo") as "menages_pr_transport_travail_velo"
     , 

    SUM("pr_naissance_hors_region_actuelle_dom") as "pr_naissance_hors_region_actuelle_dom"
     , 

    SUM("pr_etudes_sans_objet_non_inscrit") as "pr_etudes_sans_objet_non_inscrit"
     , 

    SUM("menages_avec_pr_annee_avant_meme_region") as "menages_avec_pr_annee_avant_meme_region"
     , 

    SUM("pr_travail_hors_region_actuelle_com") as "pr_travail_hors_region_actuelle_com"
     , 

    SUM("menages_avec_pr_annee_avant_etranger") as "menages_avec_pr_annee_avant_etranger"
     , 

    SUM("pr_travail_commune_departement_actuel") as "pr_travail_commune_departement_actuel"
     , 

    SUM("pr_travail_department_region_actuel") as "pr_travail_department_region_actuel"
     , 

    SUM("pr_naissance_etranger") as "pr_naissance_etranger"
     , 

    SUM("pr_travail_sans_objet_sans_emploi") as "pr_travail_sans_objet_sans_emploi"
     , 

    SUM("menages_pr_transport_travail_pieds") as "menages_pr_transport_travail_pieds"
     , 

    SUM("menages_avec_pr_annee_avant_meme_logement") as "menages_avec_pr_annee_avant_meme_logement"
     , 

    SUM("pr_naissance_hors_region_actuelle_com") as "pr_naissance_hors_region_actuelle_com"
     , 

    SUM("menages_3_et_plus_voitures") as "menages_3_et_plus_voitures"
     , 

    SUM("menages_2_voitures") as "menages_2_voitures"
     , 

    SUM("dom__deux_deux_roues") as "dom__deux_deux_roues"
     , 

    SUM("pr_naissance_department_region_actuelle") as "pr_naissance_department_region_actuelle"
     , 

    SUM("pr_travail_hors_region_actuelle_dom") as "pr_travail_hors_region_actuelle_dom"
     , 

    SUM("pr_etudes_commune_actuelle") as "pr_etudes_commune_actuelle"
     , 

    SUM("dom__un_deux_roues") as "dom__un_deux_roues"
     , 

    SUM("pr_travail_etranger") as "pr_travail_etranger"
     , 

    SUM("dom__aucun_deux_roues") as "dom__aucun_deux_roues"
     , 

    SUM("menages_avec_pr_annee_avant_autre_region_dom") as "menages_avec_pr_annee_avant_autre_region_dom"
     , 

    SUM("pr_etudes_commune_departement_actuel") as "pr_etudes_commune_departement_actuel"
     , 

    SUM("menages_pr_transport_travail_voiture") as "menages_pr_transport_travail_voiture"
     , 

    SUM("menages_avec_pr_annee_avant_meme_departement") as "menages_avec_pr_annee_avant_meme_departement"
     , 

    SUM("pr_immigration_immigre") as "pr_immigration_immigre"
     , 

    SUM("menages_avec_pr_annee_avant_union_europeenne") as "menages_avec_pr_annee_avant_union_europeenne"
     , 

    SUM("pr_travail_commune_actuelle") as "pr_travail_commune_actuelle"
     , 

    SUM("pr_etudes_hors_region_actuelle_dom") as "pr_etudes_hors_region_actuelle_dom"
    

from "defaultdb"."prepare"."mobilite_communes"
where code_departement is not null --- A enlever après avoir bien milesimé les données recensement (i.e. pas de commune manquante)
group by code_departement

) recensement_theme_departement
left join "defaultdb"."prepare"."infos_departements" infos_departements 
using (code_departement)