
    
    

select
    departement as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."professionels_sante_departement"
where departement is not null
group by departement
having count(*) > 1


