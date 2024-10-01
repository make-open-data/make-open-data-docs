
    
    

select
    "IRIS" as unique_field,
    count(*) as n_records

from "defaultdb"."sources"."filosofi_iris_2021"
where "IRIS" is not null
group by "IRIS"
having count(*) > 1


