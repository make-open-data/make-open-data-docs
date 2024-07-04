
    
    

select
    departement as unique_field,
    count(*) as n_records

from "defaultdb"."prep"."aggeger_effectifs_sante_departement_2022"
where departement is not null
group by departement
having count(*) > 1


