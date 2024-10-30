
    
    

select
    code_departement as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."activite_departements"
where code_departement is not null
group by code_departement
having count(*) > 1


