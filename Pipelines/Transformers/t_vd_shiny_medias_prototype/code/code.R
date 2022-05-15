
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

credentials <- hubr::get_credentials(Sys.getenv("HUB3_URL"), 
                                     Sys.getenv("HUB3_USERNAME"), 
                                     Sys.getenv("HUB3_PASSWD"))

credentials <- hubr::get_credentials("https://clhub.clessn.cloud/", 
                                     "patrick.poncet", 
                                     "w2UpJGqU5QPstpkk")

clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

dbx_token <- Sys.getenv("DROPBOX_TOKEN")

logger <- clessnverse::loginit(script="scraper", backend=c("console", "file", "hub"), logpath=".")

###############################################################################
######################  Get Data Sources from Warehouse  ######################
######################              HUB 3.0              ######################
###############################################################################
# admettons que j'ai sélectionné une table et je veux y extraire des données
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


###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################

filter <- clessnhub::create_filter(type="journalist")
df_journalists <- clessnhub::get_items('persons', filter)

clessnverse::logit(scriptname="scraper", message=paste("Journalists dataframe contains", nrow(df_journalists)), logger=logger)


###############################################################################
######################   Get Data Sources from Dropbox   ######################
###############################################################################

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