
    
    

select
    code_postal as unique_field,
    count(*) as n_records

from "make_open_data"."prep"."geo_postaux"
where code_postal is not null
group by code_postal
having count(*) > 1


