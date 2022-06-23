# Prends un fichier csv et l'importe dans la table persons
# du HUB 2.0 *seulement*
#
# Le fichier csv doir avoir les colonnes suivantes
# - name
# - personne_contact
# - population
# - twitter_handle
# - charge_de_projet
#
# Le séparateur doit être le point-virgule

# importer le csv
df_source <- read.csv2('people.csv')

# connection au hub 2.0
clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

logger <- clessnverse::loginit("persons_loader", c("file", "console"), Sys.getenv("LOG_PATH"))

for (i in 1:nrow(df_source)) {
    key <- digest::digest(df_source$twitter_handle[i])
    partner_name <- df_source$name[i]
    twitter_handle <- df_source$twitter_handle[i]
    role <- tolower(df_source$role[i])
    contact_full_name <- stringr::str_to_title(df_source$contact[i])
    type <- df_source$type[i]

    person_metadata_row <- list("source"="csv_file",
                            "twitterAccountHasBeenScraped"="0")
            
    person_data_row <- list("partner_name"=partner_name,
                            "role"=role,
                            "contact_full_name"=contact_full_name,
                            "twitter_id"=NA_character_,
                            "twitter_handle"=twitter_handle)

    tryCatch(
        {
            person <- clessnhub::get_item('persons', key)
            clessnverse::logit("persons_loader", 
                               paste("updating existing person", paste(list(list(key, type, paste(type,"_v1",sep="")), person_metadata_row, person_data_row), collapse = ' ')), 
                               logger)

            clessnhub::edit_item("persons", key = key, type = type, schema = paste(type,"_v1",sep=""), 
                                person_metadata_row, 
                                person_data_row)
        },
        
        error = function(e) {
            clessnverse::logit("persons_loader", 
                               paste("adding new person", paste(list(list(key, type, paste(type,"_v1",sep="")), person_metadata_row, person_data_row), collapse = ' ')), 
                               logger)

            clessnhub::create_item("persons", key = key, type = type, schema = paste(type,"_v1",sep=""), 
                                  person_metadata_row, 
                                  person_data_row)
        },

        finally={}
    )

}

closeAllConnections()
rm(logger)
