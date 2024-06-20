
    
    

select
    code_commune as unique_field,
    count(*) as n_records

from "make_open_data"."prep"."geo_communes"
where code_commune is not null
group by code_commune
having count(*) > 1


