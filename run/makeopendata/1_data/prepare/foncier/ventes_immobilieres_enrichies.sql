
  
    

  create  table "defaultdb"."prepare"."ventes_immobilieres_enrichies__dbt_tmp"
  
  
    as
  
  (
    -- Enrichie la base dvf groupée par mutation
-- Ajoute le knn des prix au m2



select * from "defaultdb"."intermediaires"."ventes_immobilieres"
  );
  