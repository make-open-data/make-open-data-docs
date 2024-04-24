--- Un code postal peut appartenir à plusieurs communes
--- Une code commune insee peut appartenir plusieurs codes postaux
--- On crée une table de passage pour avoir une relation unique entre les codes postaux et les codes communes insee



with unique_codes_communes_postaux as (
    select DISTINCT
        LPAD(CAST(code_postal AS TEXT), 5, '0') as code_postal,
        code_commune_insee
    from codes_postaux
)

select *
from unique_codes_communes_postaux