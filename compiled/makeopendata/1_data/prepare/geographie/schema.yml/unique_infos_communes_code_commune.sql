
    
    

select
    code_commune as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."infos_communes"
where code_commune is not null
group by code_commune
having count(*) > 1


