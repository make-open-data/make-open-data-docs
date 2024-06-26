
    
    

select
    code_commune_insee as unique_field,
    count(*) as n_records

from "defaultdb"."prep"."aggreger_demographie_communes"
where code_commune_insee is not null
group by code_commune_insee
having count(*) > 1


