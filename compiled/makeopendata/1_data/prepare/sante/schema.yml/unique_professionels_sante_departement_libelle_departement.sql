
    
    

select
    libelle_departement as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."professionels_sante_departement"
where libelle_departement is not null
group by libelle_departement
having count(*) > 1


