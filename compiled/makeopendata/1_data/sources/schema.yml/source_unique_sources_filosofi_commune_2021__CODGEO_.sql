
    
    

select
    "CODGEO" as unique_field,
    count(*) as n_records

from "defaultdb"."sources"."filosofi_commune_2021"
where "CODGEO" is not null
group by "CODGEO"
having count(*) > 1


