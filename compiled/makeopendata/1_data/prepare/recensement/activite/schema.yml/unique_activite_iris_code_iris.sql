
    
    

select
    code_iris as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."activite_iris"
where code_iris is not null
group by code_iris
having count(*) > 1


