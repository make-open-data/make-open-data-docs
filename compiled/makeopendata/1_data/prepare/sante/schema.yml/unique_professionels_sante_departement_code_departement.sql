
    
    

select
    code_departement as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."professionels_sante_departement"
where code_departement is not null
group by code_departement
having count(*) > 1


