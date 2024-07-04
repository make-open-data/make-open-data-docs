
    
    

select
    libelle_departement as unique_field,
    count(*) as n_records

from "defaultdb"."prep"."aggeger_effectifs_sante_departement_2022"
where libelle_departement is not null
group by libelle_departement
having count(*) > 1


