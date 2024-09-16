-- Test qu'il n'ya pas de doublons dans la table des codes géographiques des régions

WITH counts as (
    select 
        cog_regions.code,
        count(*) as num_reg
    from "defaultdb"."sources"."cog_regions" as cog_regions
    group by cog_regions.code
)

select *
from counts
where num_reg > 1