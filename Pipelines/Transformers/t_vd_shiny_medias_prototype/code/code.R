
###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                             agora-plus-canada                               #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################

clessnverse::version()
hubr::check_version()

scriptname <- "r_vd_shiny_medias_prototype"

credentials <- hubr::get_credentials(Sys.getenv("HUB3_URL"), 
                                     Sys.getenv("HUB3_USERNAME"), 
                                     Sys.getenv("HUB3_PASSWD"))

credentials <- hubr::get_credentials("https://clhub.clessn.cloud/", 
                                     "patrick.poncet", 
                                     "w2UpJGqU5QPstpkk")

clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

dbx_token <- Sys.getenv("DROPBOX_TOKEN")

logger <- clessnverse::loginit(script=scriptname, backend=c("console", "file", "hub"), logpath=".")

clessnverse::logit(scriptname = scriptname, )

###############################################################################
######################  Get Data Sources from Warehouse  ######################
######################              HUB 3.0              ######################
###############################################################################
my_table <- "clhub_tables_datamart_vd_shiny_medias_prototype"
hubr::count_table_items(my_table, credentials) # le nombre total d'éléments dans la table
# les éléments d'une table sont paginés, généralement à coup de 1000. Pour récupérer tous les éléments, on doit demander les données suivantes. On commence par une page, puis on demande une autre, jusqu'à ce que la page soit NULL

page <- hubr::list_table_items(my_table, credentials) # on récupère la première page et les informations pour les apges suivantes
data <- list() # on crée une liste vide pour contenir les données
repeat {
    data <- c(data, page$results)
    page <- hubr::list_next(page, credentials)
    if (is.null(page)) {
        break
    }
}
datamart <- tidyjson::spread_all(data) # on convertir maintenant les données en tibble

clessnverse::logit(scriptname=scriptname, message=paste("clhub_tables_datamart_vd_shiny_medias_prototype dataframe contains", nrow(datamart)), logger=logger)

###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################

# Getting journalists
filter <- clessnhub::create_filter(type="journalist")
df_journalists <- clessnhub::get_items('persons', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Journalists dataframe contains", nrow(df_journalists)), logger=logger)

# Getting tweets from journalists of feb 2022
filter <- clessnhub::create_filter(metadata = list("personType"="journalist"), data=list("creationDate__gte"="2022-02-01", "creationDate__lte"="2022-02-02"))
df_tweets <- clessnhub::get_items('tweets', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Tweets dataframe contains", nrow(df_tweets)), logger=logger)

# Getting agoraplus press conf interventions from journalists of feb 2022
filter <- clessnhub::create_filter(type="press_conference", metadata=list("location"="CA-QC"), data=list("eventDate__gte"="2022-02-01", "eventDate__lte"="2022-02-02"))
df_interventions <- clessnhub::get_items('agoraplus_interventions', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Agoraplus press_conference interventions dataframe contains", nrow(df_interventions)), logger=logger)


###############################################################################
#################   Get Data Sources from Files in Hub 3.0   ##################
###############################################################################
# Getting the lexicoder of stakes of the vitrine democratique
file_info <- hubr::retrieve_file("dictionnaire_LexicoderFR-enjeux", credentials)
stakes_dictionnary <- read.csv(file_info$file)
stakes_dictionnary$X <- NULL

# Getting the political parties dictionnary
file_info <- hubr::retrieve_file("dictionnaire_politiqueCAN", credentials)
polparties_dictionnary <- read.csv(file_info$file)
polparties_dictionnary$X <- NULL

# Getting the radar+ csv file from february
hubr::retrieve_lake_item("21c99719-701b-4876-867d-0795b3b1aea3", credentials)

###############################################################################
########################               Main              ######################
###############################################################################

# Ajouter un élément dans une table
hubr::add_table_item(table_name,
        body = list(
            key = key,
            timestamps <- "2020-01-01",
            data = jsonlite::toJSON(
                list(type = "potato", kind = "vegetable"), # stockage de json par des listes (nommées pour dict, non nommées pour arrays)
                auto_unbox = T # très important, sinon les valeurs json seront stockées comme liste d'un objet (ie. {"type": ["potato"], "kind": ["vegetable"]})
            )
        ),
        credentials
    )