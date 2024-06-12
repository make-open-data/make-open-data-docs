
  
    

  create  table "make_open_data"."prep"."geo_postaux_communes__dbt_tmp"
  
  
    as
  
  (
    --- Un code postal peut appartenir à plusieurs communes
--- Une code commune insee peut appartenir plusieurs codes postaux
--- On crée une table de passage pour avoir une relation unique entre les codes postaux et les codes communes insee



with unique_codes_communes_postaux as (
    select DISTINCT
        LPAD(CAST(code_postal AS TEXT), 5, '0') as code_postal,
        code_commune_insee
    from "make_open_data"."sources"."cog_poste" as cog_poste
)

select *
from unique_codes_communes_postaux
  );
  