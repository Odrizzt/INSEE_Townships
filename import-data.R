rm(list = ls())
###rrrr <- read.csv(url("https://www.data.gouv.fr/fr/datasets/r/73302880-e4df-4d4c-8676-1a61bb997f3d"))
##ad <- read.table("https://www.data.gouv.fr/fr/datasets/r/73302880-e4df-4d4c-8676-1a61bb997f3d")
##head(ad)


### URL Open Data >> Data Gouv >> demographie commune
dem <- "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/demographyref-france-pop-legale-commune-arrondissement-municipal-millesime/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"


### URL Open Data >> Data Gouv >> annuaire-de-ladministration-base-de-donnees-locales
adm <- "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/annuaire-de-ladministration-base-de-donnees-locales/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"


######################## 
### Data preparation ### 
######################## 

#colnames(data.dem)
adm <- fread(adm)
dem <- fread(dem)
data_adm <- as.data.frame(adm)
data_dem <- as.data.frame(dem)

## Filtrer pour chaque commune sur la dernière année de recensement de la population ##
##data.dem %>% group_by(data.dem[3],data.dem[4]) %>%
##summarise(max = max(data.dem[10]))
#adm <- data.adm

data.adm.sav <- data_adm
data.dem.sav <- data_dem


#colnames(data.adm)
#colnames(data.dem)
#colnames(data.all)

########### Selection adm sur le type Mairie ###########
#data.adm <- data.adm[data.adm["Type d'entité"]=="Mairie",]


#################### Mise en forme ####################
### ADM uniquement des Mairies 
data_adm <- sqldf("select 
adm.`ID de l'entité`,
adm.`Code INSEE`,
adm.`Type d'entité`,
adm.`Nom de l'entité`,
adm.`Editeur de la source`,
adm.`Commune`,
adm.`Adresse`,
adm.`Code Postal`,
adm.`Coordonnées`,
adm.`Email`,
adm.`URL`,
adm.`Notes`,
adm.`EPCI`,
adm.`Département`,
adm.`Région`
from data_adm adm where adm.`Type d'entité` = 'Mairie'")

### ADM Alimentation du Champ Commune par le Champ Nom de l'Entité qaund Commune est vide
data_adm <- sqldf("select 
adm.`ID de l'entité`,
adm.`Code INSEE`,
adm.`Type d'entité`,
adm.`Nom de l'entité`,
adm.`Editeur de la source`,
case when adm.`Commune` is null or adm.`Commune` = '' then replace(replace(adm.`Nom de l'entité`,'Mairie déléguée - ',''),'Mairie - ','')
else adm.`Commune`end as `Commune`,
adm.`Adresse`,
adm.`Code Postal`,
adm.`Coordonnées`,
adm.`Email`,
adm.`URL`,
adm.`Notes`,
adm.`EPCI`,
adm.`Département`,
adm.`Région`
from data_adm adm where adm.`Type d'entité` = 'Mairie'")


## DEM Dernier Recensement
data_dem <- sqldf("select *,max(`Année de recensement`) over (partition by `Code Officiel Commune / Arrondissement Municipal`,replace(`Nom Officiel Commune / Arrondissement Municipal`,' ','')) as `Dernier Recensement` from data_dem")
data_dem <- data_dem[data_dem["Année de recensement"]==data_dem["Dernier Recensement"],]


#################### Mise en forme ####################

#######################################################
####################### Union #########################
#######################################################

#data_adm <- data.adm
#data_dem <- data.dem
data_all <- sqldf("select 
adm.`ID de l'entité`,
adm.`Code INSEE`,
adm.`Type d'entité`,
adm.`Nom de l'entité`,
adm.`Editeur de la source`,
adm.`Commune`,
adm.`Adresse`,
adm.`Code Postal`,
adm.`Coordonnées`,
adm.`Email`,
adm.`URL`,
adm.`Notes`,
adm.`EPCI`,
adm.`Département`,
adm.`Région`,
dem.`Code Officiel Région`,
dem.`Nom Officiel Région`,
dem.`Code Officiel Commune / Arrondissement Municipal`,
dem.`Nom Officiel Commune / Arrondissement Municipal`,
dem.`Population municipale`,
dem.`Population comptée à part`,
dem.`Population totale`,
dem.`Code Officiel Département`,
dem.`Code Officiel Arrondissement Départemental`,
dem.`Année de recensement`,
dem.`Année d’entrée en vigueur`,
dem.`Année de référence géographique`,
dem.`Nom Officiel EPCI`,
dem.`Code Officiel EPCI`,
dem.`Nom Officiel Département`,
dem.`Dernier Recensement`,
case when dem.`Population municipale` >= 3500 then 'Yes' else 'No' end as `Déclaration Obligatoire`
from data_adm adm left join data_dem dem on (adm.`Code INSEE` = dem.`Code Officiel Commune / Arrondissement Municipal`)")


#######################################################
################## Preparation URL ####################
#######################################################

##readLines(con = check_url, n = -1L, ok = TRUE, warn = TRUE,
##          encoding = "unknown", skipNul = FALSE)

mairie_weblinks_results <- as.data.frame(matrix(nrow = 1, ncol = 6))
colnames(mairie_weblinks_results) <- c("Code INSEE","mot","URL","link","found","found_in_url")  
mairie_weblinks_all_results <- as.data.frame(matrix(nrow = 1, ncol = 6))
colnames(mairie_weblinks_all_results) <- c("Code INSEE","mot","URL","link","found","found_in_url")  

mairie_webdocs_results <- as.data.frame(matrix(nrow = 1, ncol = 6))
colnames(mairie_webdocs_results) <- c("Code INSEE","mot","URL","link","found","found_in_url") 
mairie_webdocs_all_results <- as.data.frame(matrix(nrow = 1, ncol = 6))
colnames(mairie_webdocs_all_results) <- c("Code INSEE","mot","URL","link","found","found_in_url") 


#######################################################
################## Preparation URL ####################
#######################################################

print("Data ready")


######################## 
### Data preparation ### 
######################## 

write.csv(data_all, "townships-united.csv", row.names=FALSE)
rm(list = ls())