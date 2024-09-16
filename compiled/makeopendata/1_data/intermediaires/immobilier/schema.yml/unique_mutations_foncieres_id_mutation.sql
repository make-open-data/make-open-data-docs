
    
    

select
    id_mutation as unique_field,
    count(*) as n_records

from "defaultdb"."intermediaires"."mutations_foncieres"
where id_mutation is not null
group by id_mutation
having count(*) > 1


