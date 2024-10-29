
  
    

  create  table "defaultdb"."prepare"."habitat_departements__dbt_tmp"
  
  
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

    SUM("dom__raccordement_fosse_septique") as "dom__raccordement_fosse_septique"
     , 

    SUM("avec_emplacement_stationnement") as "avec_emplacement_stationnement"
     , 

    SUM("logements_occupes_par_locataire_sous_locataire_vide_non_hlm") as "logements_occupes_par_locataire_sous_locataire_vide_non_hlm"
     , 

    SUM("logement_type_habitation_fortune") as "logement_type_habitation_fortune"
     , 

    SUM("logements_80_100_mc") as "logements_80_100_mc"
     , 

    SUM("metro__combustible_gaz_bouteille_ou_citerne") as "metro__combustible_gaz_bouteille_ou_citerne"
     , 

    SUM("logement_type_contruction_plusieurs_logements") as "logement_type_contruction_plusieurs_logements"
     , 

    SUM("logements_100_120_mc") as "logements_100_120_mc"
     , 

    SUM("emmenagement_2_4_ans") as "emmenagement_2_4_ans"
     , 

    SUM("construction_2018") as "construction_2018"
     , 

    SUM("dom__evacuation_sol") as "dom__evacuation_sol"
     , 

    SUM("logement_type_construction_provisoire") as "logement_type_construction_provisoire"
     , 

    SUM("dom__avec_electricite") as "dom__avec_electricite"
     , 

    SUM("construction_2022") as "construction_2022"
     , 

    SUM("construction_2016") as "construction_2016"
     , 

    SUM("construction_2017") as "construction_2017"
     , 

    SUM("dom__sans_cuisine_interieure_evier") as "dom__sans_cuisine_interieure_evier"
     , 

    SUM("emmenagement_40_49_ans") as "emmenagement_40_49_ans"
     , 

    SUM("construction_2013") as "construction_2013"
     , 

    SUM("logements_avec_3_pieces") as "logements_avec_3_pieces"
     , 

    SUM("appartient_pas_hlm") as "appartient_pas_hlm"
     , 

    SUM("logements_avec_4_pieces") as "logements_avec_4_pieces"
     , 

    SUM("emmenagement_1900_1919") as "emmenagement_1900_1919"
     , 

    SUM("dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures") as "dom__logement_ni_baignoire_ni_douche_avec_toilettes_interieures"
     , 

    SUM("logement_type_appartement_foyer") as "logement_type_appartement_foyer"
     , 

    SUM("metro__combustible_electricte") as "metro__combustible_electricte"
     , 

    SUM("logement_type_maison") as "logement_type_maison"
     , 

    SUM("metro__logement_avec_baignoire_ou_douche_hors_piece_reservee") as "metro__logement_avec_baignoire_ou_douche_hors_piece_reservee"
     , 

    SUM("sans_ascensseur") as "sans_ascensseur"
     , 

    SUM("emmenagement_10_19_ans") as "emmenagement_10_19_ans"
     , 

    SUM("dom__avec_piece_climatisee") as "dom__avec_piece_climatisee"
     , 

    SUM("dom__presence_chauffage") as "dom__presence_chauffage"
     , 

    SUM("dom__habitation_en_dur") as "dom__habitation_en_dur"
     , 

    SUM("construction_2019") as "construction_2019"
     , 

    SUM("construction_2007") as "construction_2007"
     , 

    SUM("logement_type_chambre_hotel") as "logement_type_chambre_hotel"
     , 

    SUM("emmenagement_1920_1939") as "emmenagement_1920_1939"
     , 

    SUM("construction_2012") as "construction_2012"
     , 

    SUM("avec_ascensseur") as "avec_ascensseur"
     , 

    SUM("construction_2011") as "construction_2011"
     , 

    SUM("dom__habitation_de_fortune") as "dom__habitation_de_fortune"
     , 

    SUM("emmenagement_1990_1999") as "emmenagement_1990_1999"
     , 

    SUM("construction_avant_1919") as "construction_avant_1919"
     , 

    SUM("dom__logements_sans_wc_interieurs") as "dom__logements_sans_wc_interieurs"
     , 

    SUM("emmenagement_1940_1969") as "emmenagement_1940_1969"
     , 

    SUM("metro__logement_ni_baignoire_ni_douche") as "metro__logement_ni_baignoire_ni_douche"
     , 

    SUM("dom__sans_piece_climatisee") as "dom__sans_piece_climatisee"
     , 

    SUM("logements_occupes_par_loge_gratuitemenent") as "logements_occupes_par_loge_gratuitemenent"
     , 

    SUM("sans_emplacement_stationnement") as "sans_emplacement_stationnement"
     , 

    SUM("dom__raccordement_egouts") as "dom__raccordement_egouts"
     , 

    SUM("emmenagement_1970_1979") as "emmenagement_1970_1979"
     , 

    SUM("dom__ni_beignoire_ni_douche") as "dom__ni_beignoire_ni_douche"
     , 

    SUM("emmenagement_1940_1959") as "emmenagement_1940_1959"
     , 

    SUM("dom__raccordement_puisard") as "dom__raccordement_puisard"
     , 

    SUM("emmenagement_plus_70_ans") as "emmenagement_plus_70_ans"
     , 

    SUM("logements_avec_2_pieces") as "logements_avec_2_pieces"
     , 

    SUM("dom__eau_froide_et_chaude") as "dom__eau_froide_et_chaude"
     , 

    SUM("metro__combustible_autre") as "metro__combustible_autre"
     , 

    SUM("metro__combustible_chauffage_urbain") as "metro__combustible_chauffage_urbain"
     , 

    SUM("emmenagement_20_29_ans") as "emmenagement_20_29_ans"
     , 

    SUM("logements_occupes_par_locataire_sous_locataire_vide_hlm") as "logements_occupes_par_locataire_sous_locataire_vide_hlm"
     , 

    SUM("logements_avec_1_piece") as "logements_avec_1_piece"
     , 

    SUM("metro__combustible_fioul") as "metro__combustible_fioul"
     , 

    SUM("metro__combustible_gaz_de_ville_ou_reseau") as "metro__combustible_gaz_de_ville_ou_reseau"
     , 

    SUM("construction_2009") as "construction_2009"
     , 

    SUM("construction_2008") as "construction_2008"
     , 

    SUM("construction_2006") as "construction_2006"
     , 

    SUM("dom__cases_traditionnelles") as "dom__cases_traditionnelles"
     , 

    SUM("logements_40_60_mc") as "logements_40_60_mc"
     , 

    SUM("logement_type_contruction_autre_logement") as "logement_type_contruction_autre_logement"
     , 

    SUM("logement_type_contruction_un_logement_isole") as "logement_type_contruction_un_logement_isole"
     , 

    SUM("dom__habitation_en_bois") as "dom__habitation_en_bois"
     , 

    SUM("dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures") as "dom__logement_avec_baignoire_ou_douche_avec_toilettes_interieures"
     , 

    SUM("logements_occupes_par_locataire_meuble_hotel") as "logements_occupes_par_locataire_meuble_hotel"
     , 

    SUM("logements_avec_6_et_plus_pieces") as "logements_avec_6_et_plus_pieces"
     , 

    SUM("construction_2015") as "construction_2015"
     , 

    SUM("logement_type_piece_independante") as "logement_type_piece_independante"
     , 

    SUM("dom__presence_chauffe_eau_solaire") as "dom__presence_chauffe_eau_solaire"
     , 

    SUM("emmenagement_30_39_ans") as "emmenagement_30_39_ans"
     , 

    SUM("construction_2010") as "construction_2010"
     , 

    SUM("appartient_hlm") as "appartient_hlm"
     , 

    SUM("dom__aucun_point_eau") as "dom__aucun_point_eau"
     , 

    SUM("emmenagement_5_9_ans") as "emmenagement_5_9_ans"
     , 

    SUM("dom__logements_avec_wc_interieurs") as "dom__logements_avec_wc_interieurs"
     , 

    SUM("logements_moins_30_mc") as "logements_moins_30_mc"
     , 

    SUM("construction_2020") as "construction_2020"
     , 

    SUM("emmenagement_50_59_ans") as "emmenagement_50_59_ans"
     , 

    SUM("emmenagement_1980_1989") as "emmenagement_1980_1989"
     , 

    SUM("construction_2021") as "construction_2021"
     , 

    SUM("logements_occupes_par_proprietaires") as "logements_occupes_par_proprietaires"
     , 

    SUM("dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures") as "dom__logement_avec_baignoire_ou_douche_sans_toilettes_interieures"
     , 

    SUM("dom__beignoire_ou_douche") as "dom__beignoire_ou_douche"
     , 

    SUM("logements_plus_120_mc") as "logements_plus_120_mc"
     , 

    SUM("dom__logement_ni_baignoire_ni_douche") as "dom__logement_ni_baignoire_ni_douche"
     , 

    SUM("logements_30_40_mc") as "logements_30_40_mc"
     , 

    SUM("dom__eau_froide_seulement") as "dom__eau_froide_seulement"
     , 

    SUM("logements_60_80_mc") as "logements_60_80_mc"
     , 

    SUM("dom__sans_electricite") as "dom__sans_electricite"
     , 

    SUM("logement_type_contruction_un_logement_groupe") as "logement_type_contruction_un_logement_groupe"
     , 

    SUM("dom__abscence_chauffe_eau_solaire") as "dom__abscence_chauffe_eau_solaire"
     , 

    SUM("emmenagement_moins_2_ans") as "emmenagement_moins_2_ans"
     , 

    SUM("emmenagement_apres_1999") as "emmenagement_apres_1999"
     , 

    SUM("construction_1971_1990") as "construction_1971_1990"
     , 

    SUM("metro__logement_salle_de_bain") as "metro__logement_salle_de_bain"
     , 

    SUM("dom__avec_cuisine_interieure_evier") as "dom__avec_cuisine_interieure_evier"
     , 

    SUM("construction_1919_1945") as "construction_1919_1945"
     , 

    SUM("construction_2014") as "construction_2014"
     , 

    SUM("logement_type_appartement") as "logement_type_appartement"
     , 

    SUM("logements_avec_5_pieces") as "logements_avec_5_pieces"
     , 

    SUM("emmenagement_60_69_ans") as "emmenagement_60_69_ans"
     , 

    SUM("construction_1916_1970") as "construction_1916_1970"
     , 

    SUM("dom__abscence_chauffage") as "dom__abscence_chauffage"
     , 

    SUM("construction_1991_2025") as "construction_1991_2025"
    

from "defaultdb"."prepare"."habitat_communes"
where code_departement is not null --- A enlever après avoir bien milesimé les données recensement (i.e. pas de commune manquante)
group by code_departement

) recensement_theme_departement
left join "defaultdb"."prepare"."infos_departements" infos_departements 
using (code_departement)
  );
  