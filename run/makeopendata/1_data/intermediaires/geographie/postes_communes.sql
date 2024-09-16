
  create view "defaultdb"."intermediaires"."postes_communes__dbt_tmp"
    
    
  as (
    

with unique_codes_communes_postaux as (
    select DISTINCT
        LPAD(CAST(code_postal AS TEXT), 5, '0') as code_postal,
        code_commune_insee
    from "defaultdb"."sources"."cog_poste" as cog_poste
)

select *
from unique_codes_communes_postaux
  );