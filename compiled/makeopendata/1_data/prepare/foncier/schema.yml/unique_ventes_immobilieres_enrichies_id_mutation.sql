
    
    

select
    id_mutation as unique_field,
    count(*) as n_records

from "defaultdb"."prepare"."ventes_immobilieres_enrichies"
where id_mutation is not null
group by id_mutation
having count(*) > 1


