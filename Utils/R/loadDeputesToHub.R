###############################################################################
##### Install the required packages if they are not installed
#####

required.packages <- c("clessnverse",
                       "clessn-hub-r",
                       "NLP")

### Install missing packages
new.packages <- required.packages[!(required.packages %in% installed.packages()[,"Package"])]

for (p in 1:length(new.packages)) {
  if ( grepl("clessn", new.packages[p]) ) {
    devtools::install_github(paste("clessn/",new.packages[p],sep=""))
  } else {
    install.packages(new.packages[p])
  } 
}

### load the packages
### We will not invoque the CLESSN packages with 'library'. The functions 
### in the package will have to be called explicitely with the package name
### in the prefix : example clessnverse::evaluateRelevanceIndex
for (p in 1:length(required.packages)) {
  if ( !grepl("clessn", required.packages[p]) ) {
    library(required.packages[p], character.only = TRUE)
  }
}


upload_deputes_to_hub <- function(dataset) {
  for (i in 1:nrow(dataset)) {
    cat(i,"\r")
    FirstName <- dataset$prenom[i]
    
    if (is.na(dataset$nom2[i])) {
      LastName <- dataset$nom1[i]
    }
    else {
      LastName <- paste(dataset$nom1[i], dataset$nom2[i], sep = ' ')
    }
      
    FullName <- paste(FirstName, LastName, sep=' ')
    IsFemale <- dataset$femme[i]
    Party <- dataset$parti[i]
    CurrentDistrict <- dataset$circonscription[i]
    IsMinister <- dataset$ministre[i]
    CurrentTitle <- NA
    TwitterHandle <- NA
    TwitterID <- NA
    TwitterAccountProtected <- NA

    if (is.na(IsFemale)) IsFemale <- 0
    
    current_list <- list(firstName=FirstName, lastName=LastName, fullName=FullName, isFemale=IsFemale, 
                         party=Party, currentDistrict=CurrentDistrict, isMinister=IsMinister, currentTitle=CurrentTitle,
                         twitterHandle=TwitterHandle, twitterID=TwitterID, twitterAccountProtected=TwitterAccountProtected)
    
    clessnhub::create_item(current_list, 'warehouse_quebec_mnas')
  }
}



purge_deputes_in_hub <- function(dataset) {
  for (i in 1:nrow(dataset)) {
    #cat(i,"\r")
    clessnhub::delete_item(dataset$uuid[i], 'warehouse_quebec_mnas')
  }
}



migrate_deputes_from_prod_to_dev <- function(dataset) {
  clessnhub::configure("https://dev-clessn.apps.valeria.science")
  
  for (i in 1:nrow(dataset)) {
    hubline <- dataset[i,-c(1:4)]
    hubline <- hubline %>%
      mutate_if(is.numeric , replace_na, replace = 0) %>%
      mutate_if(is.character , replace_na, replace = "") %>%
      mutate_if(is.logical , replace_na, replace = 0)
    print(hubline)
    clessnhub::create_item(as.list(hubline), 'warehouse_quebec_mnas')
  }
}



# deputes <- read.csv(
#   "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/Deputes_Quebec_Coordonnees.csv",
#   sep=";")
# deputes <- rbind(deputes, data.frame(nom="Horacio Arruda", femme=0, parti="fonctionnaire",
#                                      circonscription="fonctionnaire", ministre = 0))
# deputes <- rbind(deputes, data.frame(nom="Paul St-Pierre Plamondon", femme=0, parti="pq",
#                                      circonscription="non définie", ministre = 0))
# deputes <- deputes %>% separate(nom, c("prenom", "nom1", "nom2"), " ")


clessnhub::configure()

prod_deputes <- clessnhub::download_table('warehouse_quebec_mnas')

#upload_deputes_to_hub(deputes)
#purge_deputes_in_hub(hub_deputes)
migrate_deputes_from_prod_to_dev(prod_deputes)


