
  create view "make_open_data"."prep"."decoder_logement__dbt_tmp"
    
    
  as (
    

WITH logement AS (
    select * from "make_open_data"."sources"."logement_2020" as logement_2020
),
decode_logement AS (
    
  
  

  

  

  

  SELECT
    
      
      
      
      
            
      
        "INP65M" as "personnes_plus_65_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP24M" as "personnes_moins_24_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TPM"
          
            WHEN 'Z' THEN 'Sans objet  sans emploi '
          
            WHEN '2' THEN 'Temps partiel'
          
            WHEN '1' THEN 'Temps complet'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "temps_travail_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TACTM"
          
            WHEN '12' THEN 'Ch meurs'
          
            WHEN '22' THEN ' l ves'
          
            WHEN '11' THEN 'Actifs ayant un emploi'
          
            WHEN '24' THEN 'Femmes ou hommes au foyer'
          
            WHEN '25' THEN 'Autres inactifs'
          
            WHEN '21' THEN 'Retrait s ou pr retrait s'
          
            WHEN 'YY' THEN 'Hors r sidence principale'
          
        END as "type_activite_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CMBL"
          
            WHEN 'Z' THEN 'Logement ordinaire DOM'
          
            WHEN '3' THEN 'Fioul  mazout '
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '5' THEN 'Gaz en bouteilles ou en citerne'
          
            WHEN '6' THEN 'Autre'
          
            WHEN '1' THEN 'Chauffage urbain'
          
            WHEN '2' THEN 'Gaz de ville ou de r seau'
          
            WHEN '4' THEN ' lectricit '
          
        END as "combustible_principal_logement__metro"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "DIPLM"
          
            WHEN '15' THEN 'Baccalaur at professionnel  brevet professionnel  de technicien ou d enseignement  dipl me  quivalent'
          
            WHEN '14' THEN 'Baccalaur at g n ral ou technologique  brevet sup rieur  capacit  en droit  DAEU  ESEU'
          
            WHEN '12' THEN 'BEPC  brevet  l mentaire  brevet des coll ges  DNB'
          
            WHEN '03' THEN 'Aucun dipl me et scolarit  jusqu   la fin du coll ge ou au del '
          
            WHEN '13' THEN 'CAP  BEP ou dipl me de niveau  quivalent'
          
            WHEN '01' THEN 'Pas de scolarit  ou arr t avant la fin du primaire'
          
            WHEN '19' THEN 'Doctorat de recherche  hors sant  '
          
            WHEN 'YY' THEN 'Hors r sidence principale'
          
            WHEN '16' THEN 'BTS  DUT  Deug  Deust  dipl me de la sant  ou du social de niveau bac 2  dipl me  quivalent'
          
            WHEN '11' THEN 'CEP  certificat d  tudes primaires '
          
            WHEN '18' THEN 'Master  DEA  DESS  dipl me grande  cole niveau bac 5  doctorat de sant '
          
            WHEN '17' THEN 'Licence  licence pro  ma trise  dipl me  quivalent de niveau bac 3 ou bac 4'
          
            WHEN '02' THEN 'Aucun dipl me et scolarit  interrompue   la fin du primaire ou avant la fin du coll ge'
          
        END as "diplome_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "INAIM"
          
            WHEN '2' THEN 'Dans un autre d partement de la r gion de r sidence actuelle'
          
            WHEN '1' THEN 'Dans le d partement de r sidence actuelle'
          
            WHEN '6' THEN '  l  tranger'
          
            WHEN '4' THEN 'Hors de la r gion de r sidence actuelle   dans un DOM'
          
            WHEN '5' THEN 'Hors de la r gion de r sidence actuelle   dans un TOM COM'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '3' THEN 'Hors de la r gion de r sidence actuelle   en m tropole'
          
        END as "lieu_naissance_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SEXEM"
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '2' THEN 'Femmes'
          
            WHEN '1' THEN 'Hommes'
          
        END as "sexe_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPER2" as "personnes_feminin_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP19M" as "personnes_moins_19_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "EMPLM"
          
            WHEN '16' THEN 'Emplois sans limite de dur e'
          
            WHEN '23' THEN 'Non salari s   Aides familiaux'
          
            WHEN '15' THEN 'Autres emplois   dur e limit e'
          
            WHEN '11' THEN 'En contrat d apprentissage ou de professionnalisation'
          
            WHEN '22' THEN 'Non salari s   Employeurs'
          
            WHEN 'YY' THEN 'Hors r sidence principale'
          
            WHEN 'ZZ' THEN 'Sans objet  sans emploi '
          
            WHEN '13' THEN 'Emplois aid s  contrat unique d insertion'
          
            WHEN '14' THEN 'Stagiaires r mun r s en entreprise'
          
            WHEN '12' THEN 'Plac s par une agence d int rim'
          
            WHEN '21' THEN 'Non salari s   Ind pendants'
          
        END as "condition_empoi_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ASCEN"
          
            WHEN '1' THEN 'Avec ascenseur'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '2' THEN 'Sans ascenseur'
          
        END as "desserte_ascensseur"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "GARL"
          
            WHEN '1' THEN 'Avec emplacement s  de stationnement'
          
            WHEN '2' THEN 'Sans emplacement de stationnement'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "emplacement_stationnement_reserve"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "RECHM"
          
            WHEN '9' THEN 'Non d clar   inactif '
          
            WHEN '0' THEN 'Ne recherche pas d emploi'
          
            WHEN '1' THEN 'Cherche un emploi depuis moins d un an'
          
            WHEN '2' THEN 'Cherche un emploi depuis plus d un an'
          
            WHEN 'Z' THEN 'Sans objet  en emploi '
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "anciennete_recherche_emploi_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TRANSM"
          
            WHEN 'Z' THEN 'Sans objet'
          
            WHEN '6' THEN 'Transports en commun'
          
            WHEN '5' THEN 'Voiture  camion  fourgonnette'
          
            WHEN '3' THEN 'V lo  y compris   assistance  lectrique '
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '2' THEN 'Marche   pied  ou rollers  patinette '
          
            WHEN '1' THEN 'Pas de transport'
          
            WHEN '4' THEN 'Deux roues motoris '
          
        END as "mode_transport_plus_utilise_travail_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CHFL"
          
            WHEN '2' THEN 'Chauffage central individuel avec une chaudi re propre au logement'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '4' THEN 'Autre moyen de chauffage'
          
            WHEN '3' THEN 'Chauffage tout  lectrique'
          
            WHEN 'Z' THEN 'Logement ordinaire DOM'
          
            WHEN '1' THEN 'Chauffage central collectif  y compris chauffage urbain '
          
        END as "chauffage_central_logement__metro"
      
      
      
      , 
    
      
      
      
      
            
      
        "IPONDL" as "poids_du_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPER1" as "personnes_masculin_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "STAT_CONJM"
          
            WHEN '6' THEN 'C libataire'
          
            WHEN '4' THEN 'Veuf  veuve'
          
            WHEN '5' THEN 'Divorc  e '
          
            WHEN '3' THEN 'En concubinage ou union libre'
          
            WHEN '2' THEN 'Pacs  e '
          
            WHEN '1' THEN 'Mari  e '
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "status_conjugal_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP60M" as "personnes_plus_60_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "HLML"
          
            WHEN '1' THEN 'Logement appartenant   un organisme HLM'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '2' THEN 'Logement n appartenant pas   un organisme HLM'
          
        END as "logement_hml"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "EGOUL"
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '2' THEN 'Raccordement   une fosse septique'
          
            WHEN '1' THEN 'Raccordement au r seau d  gouts'
          
            WHEN '4' THEN ' vacuation des eaux us es   m me le sol'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '3' THEN 'Raccordement   un puisard'
          
        END as "mode_evacuation_eaux_usagers__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP75M" as "personnes_plus_75_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ILETUDM"
          
            WHEN 'Z' THEN 'Sans objet  pas d inscription dans un  tablissement d enseignement '
          
            WHEN '5' THEN 'Hors de la r gion de r sidence actuelle   dans un DOM'
          
            WHEN '4' THEN 'Hors de la r gion de r sidence actuelle   en m tropole'
          
            WHEN '2' THEN 'Dans une autre commune du d partement de r sidence'
          
            WHEN '6' THEN 'Hors de la r gion de r sidence actuelle   dans une COM'
          
            WHEN '3' THEN 'Dans un autre d partement de la r gion de r sidence'
          
            WHEN '7' THEN '  l  tranger'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '1' THEN 'Dans la commune de r sidence actuelle'
          
        END as "lieu_etudes_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CATL"
          
            WHEN '2' THEN 'Logements occasionnels'
          
            WHEN '1' THEN 'R sidences principales'
          
            WHEN '3' THEN 'R sidences secondaires'
          
            WHEN '4' THEN 'Logements vacants'
          
        END as "categorie_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ANEMR"
          
            WHEN '00' THEN 'Moins de 2 ans'
          
            WHEN '06' THEN 'De 40   49 ans'
          
            WHEN '03' THEN 'De 10   19 ans'
          
            WHEN '01' THEN 'De 2   4 ans'
          
            WHEN '07' THEN 'De 50   59 ans'
          
            WHEN '04' THEN 'De 20   29 ans'
          
            WHEN '08' THEN 'De 60   69 ans'
          
            WHEN '05' THEN 'De 30   39 ans'
          
            WHEN '99' THEN 'Logement ordinaire inoccup '
          
            WHEN '02' THEN 'De 5   9 ans'
          
            WHEN '09' THEN '70 ans ou plus'
          
        END as "aciennete_regroupe_ammenagement_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPER" as "personnes_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "AEMMR"
          
            WHEN '7' THEN 'Emm nagement entre 1980 et 1989'
          
            WHEN '3' THEN 'Emm nagement entre 1920 et 1939'
          
            WHEN '6' THEN 'Emm nagement entre 1970 et 1979'
          
            WHEN '5' THEN 'Emm nagement entre 1960 et 1969'
          
            WHEN '4' THEN 'Emm nagement entre 1940 et 1959'
          
            WHEN '2' THEN 'Emm nagement entre 1900 et 1919'
          
            WHEN '9' THEN 'Emm nagement apr s 1999'
          
            WHEN '8' THEN 'Emm nagement entre 1990 et 1999'
          
            WHEN '0' THEN 'Logement ordinaire inoccup '
          
        END as "annee_regroupe_ammenagement_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "VOIT"
          
            WHEN '2' THEN 'Deux voitures'
          
            WHEN '3' THEN 'Trois voitures ou plus'
          
            WHEN '1' THEN 'Une seule voiture'
          
            WHEN '0' THEN 'Aucune voiture'
          
            WHEN 'X' THEN 'Logement ordinaire inoccup '
          
        END as "nombre_voitures_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "COMMUNE" as "code_commune_insee"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPAM" as "personnes_actives_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CHAU"
          
            WHEN '2' THEN 'Absence de moyen de chauffage'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '1' THEN 'Pr sence de moyen de chauffage'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
        END as "chauffage_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "IMMIM"
          
            WHEN '1' THEN 'Immigr s'
          
            WHEN '2' THEN 'Non immigr s'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "situation_immigration_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SURF"
          
            WHEN '3' THEN 'De 40   moins de 60 m '
          
            WHEN '5' THEN 'De 80   moins de 100 m '
          
            WHEN '1' THEN 'Moins de 30 m '
          
            WHEN '6' THEN 'De 100   moins de 120 m '
          
            WHEN '4' THEN 'De 60   moins de 80 m '
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '7' THEN '120 m  ou plus'
          
            WHEN '2' THEN 'De 30   moins de 40 m '
          
        END as "superficie_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "EAU"
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '1' THEN 'Eau froide seulement'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '3' THEN 'Aucun point d eau   l int rieur du logement'
          
            WHEN '2' THEN 'Eau froide et chaude'
          
        END as "eau_potabke_interieur_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "AGEMEN8"
          
            WHEN '80' THEN '80 ans ou plus'
          
            WHEN '00' THEN 'Moins de 15 ans'
          
            WHEN '15' THEN '15   19 ans'
          
            WHEN '40' THEN '40   54 ans'
          
            WHEN '55' THEN '55   64 ans'
          
            WHEN '25' THEN '25   39 ans'
          
            WHEN '65' THEN '65   79 ans'
          
            WHEN '20' THEN '20   24 ans'
          
            WHEN 'YY' THEN 'Hors r sidence principale'
          
        END as "age_regroupe_pr_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "METRODOM"
          
            WHEN 'D' THEN 'DOM'
          
            WHEN 'M' THEN 'France m tropolitaine'
          
        END as "residence_metropole_ou_dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP3M" as "personnes_moins_3_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ELEC"
          
            WHEN '2' THEN 'Sans  lectricit '
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '1' THEN 'Avec  lectricit '
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
        END as "electricite_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CLIM"
          
            WHEN '2' THEN 'Sans pi ce climatis e'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '1' THEN 'Avec pi ce climatis e'
          
        END as "min_une_piece_climatise__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "NBPI" as "nombre_pieces_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "BAIN"
          
            WHEN '2' THEN 'Sans baignoire ni douche'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '1' THEN 'Avec baignoire ou douche'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
        END as "baignoire_ou_douche__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "REGION"
          
            WHEN '84' THEN 'Auvergne Rh ne Alpes'
          
            WHEN '04' THEN 'La R union'
          
            WHEN '11' THEN ' le de France'
          
            WHEN '52' THEN 'Pays de la Loire'
          
            WHEN '93' THEN 'Provence Alpes C te d Azur'
          
            WHEN '03' THEN 'Guyane'
          
            WHEN '94' THEN 'Corse'
          
            WHEN '75' THEN 'Nouvelle Aquitaine'
          
            WHEN '76' THEN 'Occitanie'
          
            WHEN '32' THEN 'Hauts de France'
          
            WHEN '27' THEN 'Bourgogne Franche Comt '
          
            WHEN '53' THEN 'Bretagne'
          
            WHEN '44' THEN 'Grand Est'
          
            WHEN '02' THEN 'Martinique'
          
            WHEN '24' THEN 'Centre Val de Loire'
          
            WHEN '28' THEN 'Normandie'
          
            WHEN '01' THEN 'Guadeloupe'
          
        END as "region_residence"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CHOS"
          
            WHEN '1' THEN 'Pr sence de chauffe eau solaire'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '2' THEN 'Absence de chauffe eau solaire'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "chauffe_eau_solaire__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "INEEM" as "personnes_scolarise_plus_14_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP17M" as "personnes_moins_17_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "CUIS"
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '2' THEN 'Absence de cuisine int rieure avec  vier'
          
            WHEN '1' THEN 'Pr sence de cuisine int rieure avec  vier'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "cuisine_interieur_avec_evier__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "STOCD"
          
            WHEN '22' THEN 'Locataire ou sous locataire d un logement lou  vide HLM'
          
            WHEN '21' THEN 'Locataire ou sous locataire d un logement lou  vide non HLM'
          
            WHEN '00' THEN 'Logement ordinaire inoccup '
          
            WHEN '30' THEN 'Log  gratuitement'
          
            WHEN '23' THEN 'Locataire ou sous locataire d un logement lou  meubl  ou d une chambre d h tel'
          
            WHEN '10' THEN 'Propri taire'
          
        END as "status_occupation_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SANI"
          
            WHEN '0' THEN 'Ni baignoire'
          
            WHEN '1' THEN 'Baignoire ou douche hors pi ce r serv e'
          
            WHEN 'Z' THEN 'Logement ordinaire DOM'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '2' THEN 'Salle s  de bains  avec douche ou baignoire '
          
        END as "installation_sanitaires__metro"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "DEROU"
          
            WHEN '3' THEN 'Trois deux roues   moteur ou plus'
          
            WHEN 'X' THEN 'Logement ordinaire inoccup   DOM '
          
            WHEN '2' THEN 'Deux deux roues   moteur'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '0' THEN 'Aucun deux roues   moteur'
          
            WHEN '1' THEN 'Un seul deux roues   moteur'
          
        END as "nombre_deux_roues_menage__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "BATI"
          
            WHEN '2' THEN 'Cases traditionnelles'
          
            WHEN '1' THEN 'Habitations de fortune'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN '4' THEN 'Maisons ou immeubles en dur'
          
            WHEN '3' THEN 'Maisons ou immeubles en bois'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "aspect_bati__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP5M" as "personnes_moins_5_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "SANIDOM"
          
            WHEN 'ZZ' THEN 'Logement ordinaire France m tropolitaine'
          
            WHEN 'YY' THEN 'Hors r sidence principale'
          
            WHEN '11' THEN 'Avec baignoire ou douche et avec WC   l int rieur'
          
            WHEN '21' THEN 'Sans baignoire ni douche mais avec WC   l int rieur'
          
            WHEN '22' THEN 'Sans baignoire ni douche'
          
            WHEN '12' THEN 'Avec baignoire ou douche mais sans WC   l int rieur'
          
        END as "installation_sanitaires__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP15M" as "personnes_moins_15_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "WC"
          
            WHEN '2' THEN 'Sans W  C    l int rieur du logement'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '1' THEN 'Avec W  C    l int rieur du logement'
          
            WHEN 'Z' THEN 'Logement ordinaire France m tropolitaine'
          
        END as "presence_wc_interieur_logement__dom"
      
      
      
      , 
    
      
      
      
      
            
      
        "INP11M" as "personnes_moins_11_ans_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPOM" as "personnes_actives_ayant_emploi_menage"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TYPC"
          
            WHEN '5' THEN 'Construction provisoire'
          
            WHEN '2' THEN 'B timent d habitation d un seul logement jumel  ou group  de toute autre fa on'
          
            WHEN '3' THEN 'B timent d habitation de 2 logements ou plus'
          
            WHEN '1' THEN 'B timent d habitation d un seul logement isol '
          
            WHEN '4' THEN 'B timent   usage autre qu habitation'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
        END as "type_construction"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ILTM"
          
            WHEN '6' THEN 'Hors de la r gion de r sidence actuelle   dans une COM'
          
            WHEN '7' THEN '  l  tranger'
          
            WHEN '2' THEN 'Dans une autre commune du d partement de r sidence'
          
            WHEN 'Z' THEN 'Sans objet  sans emploi '
          
            WHEN '5' THEN 'Hors de la r gion de r sidence actuelle   dans un DOM'
          
            WHEN '4' THEN 'Hors de la r gion de r sidence actuelle   en m tropole'
          
            WHEN '1' THEN 'Dans la commune de r sidence actuelle'
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '3' THEN 'Dans un autre d partement de la r gion de r sidence'
          
        END as "lieu_travail_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "IRANM"
          
            WHEN 'Y' THEN 'Hors r sidence principale'
          
            WHEN '4' THEN 'Dans un autre d partement de la r gion'
          
            WHEN '3' THEN 'Dans une autre commune du d partement'
          
            WHEN '7' THEN 'Hors de la r gion de r sidence actuelle   dans un TOM COM'
          
            WHEN '2' THEN 'Dans un autre logement de la m me commune'
          
            WHEN '9' THEN '  l  tranger hors Union Europ enne'
          
            WHEN '6' THEN 'Hors de la r gion de r sidence actuelle   dans un DOM'
          
            WHEN '8' THEN '  l  tranger dans l Union Europ enne  28 pays membres '
          
            WHEN '1' THEN 'Dans le m me logement'
          
            WHEN '5' THEN 'Hors de la r gion de r sidence actuelle   en m tropole'
          
        END as "residence_annee_avant_pr"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "ACHL"
          
            WHEN 'B11' THEN 'De 1946   1970'
          
            WHEN 'B12' THEN 'De 1971   1990'
          
            WHEN 'C117' THEN 'En 2017'
          
            WHEN 'C2022' THEN 'En 2022 partiel '
          
            WHEN 'C100' THEN 'De 1991   2005'
          
            WHEN 'C2020' THEN 'En 2020  partiel '
          
            WHEN 'C115' THEN 'En 2015'
          
            WHEN 'C114' THEN 'En 2014'
          
            WHEN 'C109' THEN 'En 2009'
          
            WHEN 'C111' THEN 'En 2011'
          
            WHEN 'C110' THEN 'En 2010'
          
            WHEN 'C2018' THEN 'En 2018  partiel '
          
            WHEN 'C116' THEN 'En 2016'
          
            WHEN 'C112' THEN 'En 2012'
          
            WHEN 'C2021' THEN 'En 2021 partiel '
          
            WHEN 'A11' THEN 'Avant 1919'
          
            WHEN 'C107' THEN 'En 2007'
          
            WHEN 'A12' THEN 'De 1919   1945'
          
            WHEN 'C108' THEN 'En 2008'
          
            WHEN 'C2019' THEN 'En 2019  partiel '
          
            WHEN 'C113' THEN 'En 2013'
          
            WHEN 'C106' THEN 'En 2006'
          
        END as "achevement_construction_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        CASE "TYPL"
          
            WHEN '6' THEN 'Pi ce ind pendante  ayant sa propre entr e '
          
            WHEN '1' THEN 'Maison'
          
            WHEN '2' THEN 'Appartement'
          
            WHEN '5' THEN 'Habitation de fortune'
          
            WHEN '4' THEN 'Chambre d h tel'
          
            WHEN '3' THEN 'Logement foyer'
          
        END as "type_logement"
      
      
      
      , 
    
      
      
      
      
            
      
        "INPSM" as "personnes_scolarisees_menage"
      
      
      
      
    
  FROM logement

)


SELECT 
    *
FROM 
    decode_logement
  );