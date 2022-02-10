

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=list(institution="European Parliament"))  
dfPersonsDirty <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

logger <- clessnverse::loginit("clean-mps-from-hub", "file,console", Sys.getenv("LOG_PATH"))


###  Clean all names longer than 3 words
# keys <- which(sapply(strsplit(df$data.fullName, " "),length) > 3)
# 
# fullnames <- df$data.fullName[which(sapply(strsplit(df$data.fullName, " "),length) > 3)]
# 
# for (u in 1:length(keys)) {
#   clessnhub::delete_item('persons', keys[u])
# }
### Delete all european mps from hub
# for (i in 1:nrow(df)) {
#   clessnhub::delete_item('persons', df$key[i])
# }

### Delete all european mps of which the native name is not in the intervention dataset
# for (i in 1:nrow(dfPersonsDirty)) {
#   
#   index <- which(dfInterventions_new2$data.speakerFullName == dfPersonsDirty$data.fullName[i])
#   
#   if ( length(index) == 0 ) {
#     cat("deleting", dfPersonsDirty$data.fullName[i], "from hub\n")
#     #clessnhub::delete_item('persons', df$key[i])
#   }
#}


### Add missing traslated MP named to the hub from a dataset and correct existing ones
# clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
# 
# logger <- clessnverse::loginit("clesn-mos-from-hub", "file,console", Sys.getenv("LOG_PATH"))
# 
# df <- readRDS("dfPersonsToHub.rds")
# for (i in s:nrow(df)) {
# 
#   # see if record with native name exists in the hub
#   if (!stringr::str_detect(df$data.fullNameNative[i], ":")) {
#     filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=list(institution="European Parliament"), data=list(fullNameNative=df$data.fullNameNative[i]))  
#     person <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)
#   } else {
#     clessnverse::logit("clean-mps-from-hub", paste("\n\n\n\n\n", i), logger)
#     clessnverse::logit("clean-mps-from-hub", paste("Skipping", df$data.fullName[i], "-", df$data.fullNameNative[i], "\n" ), logger)
#     next
#   }
#   
#   if (is.null(person)) {
#     
#     if (df$data.fullName[i] != "DeleteDelete") {
#       # Insert
#       clessnverse::logit("clean-mps-from-hub",paste("\n\n\n\n\n", i),logger)
#       clessnverse::logit("clean-mps-from-hub",paste("corrected version:",df$data.fullName[i], "-", df$data.fullNameNative[i]), logger)
#       clessnverse::logit("clean-mps-from-hub",paste("hub       version:",person$data.fullName, "-", person$data.fullNameNative), logger)
#       clessnverse::logit("clean-mps-from-hub",paste("inserting new mp",df$data.fullName[i], "-", df$data.fullNameNative[i]), logger)
#       new_person_metadata <- list("source"="https://www.europarl.europa.eu/meps/fr/download/advanced/xml?name=",
#                                   "country"=df$metadata.country[i],
#                                   "institution"="European Parliament",
#                                   "province_or_state"=df$metadata.country[i],
#                                   "twitterAccountHasBeenScraped"="0"
#                                   )
#       new_person_data <- list("fullName"=df$data.fullName[i],
#                               "fullNameNative" = df$data.fullNameNative[i],
#                               "isFemale"= df$data.isFemale[i],
#                               "lastName"=df$data.lastName[i],
#                               "firstName"=df$data.firstName[i],
#                               "twitterID"=NA_character_,
#                               "isMinister"="0",
#                               "twitterName"=NA_character_,
#                               "currentParty"=df$data.currentParty[i],
#                               "twitterHandle"=NA_character_,
#                               "currentMinister"=NA_character_,
#                               "currentPolGroup"=df$data.currentPolGroup[i],
#                               "twitterLocation"=NA_character_,
#                               "twitterPostsCount"=NA_character_,
#                               "twitterProfileURL"=NA_character_,
#                               "twitterListedCount"=NA_character_,
#                               "twitterFriendsCount"=NA_character_,
#                               "currentFunctionsList"=NA_character_,
#                               "twitterFollowersCount"=NA_character_,
#                               "currentProvinceOrState"=df$data.currentProvinceOrState[i],
#                               "twitterAccountVerified"=NA_character_,
#                               "twitterProfileImageURL"=NA_character_,
#                               "twitterAccountCreatedAt"=NA_character_,
#                               "twitterAccountCreatedOn"=NA_character_,
#                               "twitterAccountProtected"=NA_character_,
#                               "twitterProfileBannerURL"=NA_character_,
#                               "twitterUpdateDateStamps"=NA_character_,
#                               "twitterUpdateTimeStamps"=NA_character_
#                               )
#       
#       clessnhub::create_item(table = 'persons', key = paste("EU-",digest::digest(new_person_data$fullNameNative),sep=''), 
#                              type = "mp", schema = "v3", metadata = new_person_metadata, data = new_person_data)
#       next
#     } 
#     
#   } else {
#   
#     if (nrow(person) > 1) stop("Wooo more than one row")
#     
#     if (df$data.fullName[i] == person$data.fullName && df$data.fullNameNative[i] == person$data.fullNameNative && person$data.fullNameNative == person$data.fullNameNative) {
#       # Doing nothing
#     } else {
#       # Doing something
#       clessnverse::logit("clean-mps-from-hub", paste("\n\n\n\n\n", i), logger)
#       clessnverse::logit("clean-mps-from-hub",paste("corrected version:",df$data.fullName[i], "-", df$data.fullNameNative[i]), logger)
#       clessnverse::logit("clean-mps-from-hub",paste("hub       version:",person$data.fullName, "-", person$data.fullNameNative), logger)
# 
#       if (df$data.fullName[i] == "DeleteDelete") {
#         # Delete
#         clessnverse::logit("clean-mps-from-hub", paste("deleting", person$data.fullName, "-", person$data.fullNameNative), logger)
#         clessnhub::delete_item('persons', person$key)
#         next
#       }
#       
#       if (df$data.fullName[i] != person$data.fullName && df$data.fullNameNative[i] == person$data.fullNameNative) {
#         # Update
#         clessnverse::logit("clean-mps-from-hub", paste("updating",  person$data.fullName, "-", person$data.fullNameNative, "to", df$data.fullName[i], "in the hub"), logger)
#         person$data.fullName <- df$data.fullName[i]
#         person$data.firstName <- trimws(stringr::str_to_title(stringr::str_split(person$data.fullName, "\\s")[[1]][[1]]))
#         person$data.lastName <- trimws(stringr::str_to_title(stringr::str_match(person$data.fullName, paste("^",person$data.firstName,"(.*)$",sep=''))[2]))
#         person_metadata <- person[,which(stringr::str_detect(colnames(person), "^metadata."))]
#         names(person_metadata) <- gsub("metadata.", "", names(person_metadata))
#         person_data <- person[,which(stringr::str_detect(colnames(person), "^data."))]
#         names(person_data) <- gsub("data.", "", names(person_data))
#         clessnhub::edit_item('persons', person$key, person$type, person$schema, as.list(person_metadata), as.list(person_data))
#         next
#       }
#     }
#     
#   }
# }
 


#logger <- clessnverse::loginit("clean-mps-from-hub", "file,console", Sys.getenv("LOG_PATH"))

for (s in 101:nrow(dfPersonsDirty)) {
#for (s in 1:100) {


    newMP <- clessnverse::getEuropeMepData(dfPersonsDirty$data.fullName[s])    
    
    if (!is.na(newMP$fullname)) {
      
    # newCountry <- dfPersonsDirty$metadata.country[s]
    # change <- FALSE
    # 
    # if (dfPersonsDirty$metadata.country[s] == "Allemagne") {
    #   newCountry <- "Germany"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Autriche") {
    #   newCountry <- "Austria"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Belgique") {
    #   newCountry <- "Belgium"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Bulgarie") {
    #   newCountry <- "Bulgaria"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Chypre") {
    #   newCountry <- "Cyprus"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Espagne") {
    #   newCountry <- "Spain"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Estonie") {
    #   newCountry <- "Estonia"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Finlande") {
    #   newCountry <- "Finland"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Grèce") {
    #   newCountry <- "Greece"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Hongrie") {
    #   newCountry <- "Hungary"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Irland") {
    #   newCountry <- "Ireland"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Italie") {
    #   newCountry <- "Italy"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Lettonie") {
    #   newCountry <- "Latvia"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Lituanie") {
    #   newCountry <- "Lithuania"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Malte") {
    #   newCountry <- "Malta"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Pays-Bas") {
    #   newCountry <- "Netherlands"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Pologne") {
    #   newCountry <- "Poland"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Roumanie") {
    #   newCountry <- "Romania"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Slovaquie") {
    #   newCountry <- "Slovakia"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Slovénie") {
    #   newCountry <- "Slovenia"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Suède") {
    #   newCountry <- "Sweden"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Tchéquie") {
    #   newCountry <- "Czechia"
    #   change <- TRUE
    # }
    # if (dfPersonsDirty$metadata.country[s] == "Croatie") {
    #   newCountry <- "Croatia"
    #   change <- TRUE
    # }
    # 
    
    # if (change) {
    #   dfPersonsDirty$metadata.country[s] <- newCountry
    #   dfPersonsDirty$metadata.province_or_state[s] <- newCountry
    #   dfPersonsDirty$data.currentProvinceOrState[s] <- newCountry
    #   
    #   person <- dfPersonsDirty[s,]
    # 
    #   person_metadata <- person[,which(stringr::str_detect(colnames(person), "^metadata."))]
    #   names(person_metadata) <- gsub("metadata.", "", names(person_metadata))
    #   person_data <- person[,which(stringr::str_detect(colnames(person), "^data."))]
    #   names(person_data) <- gsub("data.", "", names(person_data))
    #   
    #   clessnverse::logit("clean-mps-from-hub", paste(s,"updating",  person$data.fullName, "-", person$metadata.country, "in the hub"), logger)
    #   
    #   clessnhub::edit_item('persons', person$key, person$type, person$schema, as.list(person_metadata), as.list(person_data))
    # }
      
        dfPersonsDirty$metadata.country[s] <- newMP$country
        dfPersonsDirty$metadata.province_or_state[s] <- newMP$country
        dfPersonsDirty$data.currentProvinceOrState[s] <- newMP$country
        dfPersonsDirty$data.fullName[s] <- newMP$fullname
        dfPersonsDirty$data.currentParty[s] <- newMP$party
        dfPersonsDirty$data.currentPolGroup[s] <- newMP$polgroup

        person <- dfPersonsDirty[s,]

        person_metadata <- person[,which(stringr::str_detect(colnames(person), "^metadata."))]
        names(person_metadata) <- gsub("metadata.", "", names(person_metadata))
        person_data <- person[,which(stringr::str_detect(colnames(person), "^data."))]
        names(person_data) <- gsub("data.", "", names(person_data))

        clessnverse::logit("clean-mps-from-hub", paste(s,"updating",  person$key, "-", person$data.fullName, "-", person$metadata.country, "-", person$data.currentPolGroup, "-", person$data.currentParty, "in the hub"), logger)

        clessnhub::edit_item('persons', person$key, person$type, person$schema, as.list(person_metadata), as.list(person_data))

  }
  
}

clessnverse::logclose(logger = logger)
