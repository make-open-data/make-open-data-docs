
    
    

select
    "code_commune_2024" as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."revenu_commune"
where "code_commune_2024" is not null
group by "code_commune_2024"
having count(*) > 1


