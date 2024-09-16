
  
    

  create  table "defaultdb"."prepare"."professionels_sante_departement__dbt_tmp"
  
  
    as
  
  (
    

with aggreger_effectif_sante_unpivot as (
	select 
		region,
		libelle_region,
		departement,
		libelle_departement,
		profession_sante,
		sum(cast(effectif as numeric)) as effectif
	FROM "defaultdb"."sources"."professionels_sante"
	where (annee = '2022') and (libelle_departement != 'Tout département')  and (libelle_departement != 'FRANCE')
	group by region, departement, libelle_region, libelle_departement, profession_sante
),
aggreger_effectif_sante_departements as (
	select 
		region,
		libelle_region,
		departement,
		libelle_departement,
		
  
    sum(
      
      case
      when profession_sante = 'Stomatologues'
        then effectif
      else 0
      end
    )
    
      
            as "Stomatologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Anesthésistes-réanimateurs'
        then effectif
      else 0
      end
    )
    
      
            as "Anesthésistes-réanimateurs"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Autres médecins'
        then effectif
      else 0
      end
    )
    
      
            as "Autres médecins"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Cardiologues'
        then effectif
      else 0
      end
    )
    
      
            as "Cardiologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Chirurgiens'
        then effectif
      else 0
      end
    )
    
      
            as "Chirurgiens"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Chirurgiens-dentistes (hors spécialistes d''orthopédie dento-faciale - ODF)'
        then effectif
      else 0
      end
    )
    
      
            as "Chirurgiens-dentistes (hors spécialistes d'orthopédie dento-faciale - ODF)"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Chirurgiens-dentistes spécialistes d''orthopédie dento-faciale (ODF)'
        then effectif
      else 0
      end
    )
    
      
            as "Chirurgiens-dentistes spécialistes d'orthopédie dento-faciale (ODF)"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Dermatologues'
        then effectif
      else 0
      end
    )
    
      
            as "Dermatologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Endocrinologues'
        then effectif
      else 0
      end
    )
    
      
            as "Endocrinologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Ensemble des auxiliaires médicaux'
        then effectif
      else 0
      end
    )
    
      
            as "Ensemble des auxiliaires médicaux"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Ensemble des chirurgiens-dentistes'
        then effectif
      else 0
      end
    )
    
      
            as "Ensemble des chirurgiens-dentistes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Ensemble des médecins'
        then effectif
      else 0
      end
    )
    
      
            as "Ensemble des médecins"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Ensemble des médecins généralistes'
        then effectif
      else 0
      end
    )
    
      
            as "Ensemble des médecins généralistes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Ensemble des médecins spécialistes (hors généralistes)'
        then effectif
      else 0
      end
    )
    
      
            as "Ensemble des médecins spécialistes (hors généralistes)"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Gynécologues médicaux et obstétriciens'
        then effectif
      else 0
      end
    )
    
      
            as "Gynécologues médicaux et obstétriciens"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Hépato-gastro-entérologues'
        then effectif
      else 0
      end
    )
    
      
            as "Hépato-gastro-entérologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Infirmiers'
        then effectif
      else 0
      end
    )
    
      
            as "Infirmiers"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Masseurs-kinésithérapeutes'
        then effectif
      else 0
      end
    )
    
      
            as "Masseurs-kinésithérapeutes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Médecins généralistes à expertise particulière (MEP)'
        then effectif
      else 0
      end
    )
    
      
            as "Médecins généralistes à expertise particulière (MEP)"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Médecins généralistes (hors médecins à expertise particulière - MEP)'
        then effectif
      else 0
      end
    )
    
      
            as "Médecins généralistes (hors médecins à expertise particulière - MEP)"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Médecins nucléaires'
        then effectif
      else 0
      end
    )
    
      
            as "Médecins nucléaires"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Médecins pathologistes'
        then effectif
      else 0
      end
    )
    
      
            as "Médecins pathologistes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Rhumatologues'
        then effectif
      else 0
      end
    )
    
      
            as "Rhumatologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Sages-femmes'
        then effectif
      else 0
      end
    )
    
      
            as "Sages-femmes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Néphrologues'
        then effectif
      else 0
      end
    )
    
      
            as "Néphrologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Neurologues'
        then effectif
      else 0
      end
    )
    
      
            as "Neurologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Ophtalmologues'
        then effectif
      else 0
      end
    )
    
      
            as "Ophtalmologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Orthophonistes'
        then effectif
      else 0
      end
    )
    
      
            as "Orthophonistes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Orthoptistes'
        then effectif
      else 0
      end
    )
    
      
            as "Orthoptistes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Oto-rhino-laryngologistes'
        then effectif
      else 0
      end
    )
    
      
            as "Oto-rhino-laryngologistes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Pédiatres'
        then effectif
      else 0
      end
    )
    
      
            as "Pédiatres"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Pédicures-podologues'
        then effectif
      else 0
      end
    )
    
      
            as "Pédicures-podologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Pneumologues'
        then effectif
      else 0
      end
    )
    
      
            as "Pneumologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Psychiatres'
        then effectif
      else 0
      end
    )
    
      
            as "Psychiatres"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Radiologues'
        then effectif
      else 0
      end
    )
    
      
            as "Radiologues"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Radiothérapeutes'
        then effectif
      else 0
      end
    )
    
      
            as "Radiothérapeutes"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Médecins vasculaires'
        then effectif
      else 0
      end
    )
    
      
            as "Médecins vasculaires"
      
    
    ,
  
    sum(
      
      case
      when profession_sante = 'Allergologues'
        then effectif
      else 0
      end
    )
    
      
            as "Allergologues"
      
    
    
  

  	FROM aggreger_effectif_sante_unpivot
	group by region, libelle_region, departement, libelle_departement
),
infos_par_departement as (
	select 
		code_departement,
		sum(cast(population as numeric)) as population_departement,
		avg(commune_latitude) as latitude_centre_departement,
		avg(commune_longitude) as longitude_centre_departement
	from "defaultdb"."prepare"."infos_communes"
	group by code_departement
)

select 
	aggreger_effectif_sante_departements.*,
	infos_par_departement.population_departement,
	infos_par_departement.latitude_centre_departement,
	infos_par_departement.longitude_centre_departement
from aggreger_effectif_sante_departements
left join infos_par_departement 
on aggreger_effectif_sante_departements.departement = infos_par_departement.code_departement
  );
  