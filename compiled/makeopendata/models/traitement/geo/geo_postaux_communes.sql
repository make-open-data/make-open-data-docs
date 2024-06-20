

with unique_codes_communes_postaux as (
    select DISTINCT
        LPAD(CAST(code_postal AS TEXT), 5, '0') as code_postal,
        code_commune_insee
    from "make_open_data"."sources"."cog_poste" as cog_poste
)

select *
from unique_codes_communes_postaux