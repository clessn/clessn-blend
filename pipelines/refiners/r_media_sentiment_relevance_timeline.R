###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        <e|l|r_name_of_the_etl_script>                       #
#                                                                             #
#  Describe what the script does.  You can copy/paste part of the README.md   #
#  of the folder which this script resides in                                 #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################
issues <- clessnverse::get_dictionary("issues", lang = "fr", credentials)
sentiments <- clessnverse::get_dictionary("sentiments", lang="fr", credentials)
# Se connecter au système de Radar+ pour aller chercher les données.
auth <- quorum::createRadarplusAuth(Sys.getenv("RADARPLUS_USERNAME"), Sys.getenv("RADARPLUS_PASSWORD"))

load_radarplus_data <- function(){
  
  # on crée une requête avec les paramètres des articles qu'on veut
  # Date de début des données désirées
  begin_date <- quorum::createDate(20, 07, 2022, 00,00,00)
  
  # # Créer la date de fin (aujourd'hui)
  day2 <- as.numeric(format(Sys.time(), "%d"))
  month2 <- as.numeric(format(Sys.time(), "%m"))
  year2 <- as.numeric(format(Sys.time(), "%Y"))
  
  # Date de fin des données désirées
  end_date <- quorum::createDate(day2, month2, year2, 00, 00, 00)
  
  # Obtenir les données en français
  queryFR <- quorum::createRadarplusQuery(begin_date=begin_date,                # les articles dont la fin de la une est après cette date
                                          end_date=end_date,                      # les articles dont la fin de la une est avant cette date
                                          # title_contains=c('covid', 'legault'),  # dont le titre contient covid ET legault (case ignorée)
                                          # text_contains=c('covid', 'québec'),    # dont le titre contient covid ET québec (case ignorée)
                                          tags=c('quorum', 'french'),             # dont la source possède les tags quorum ET french
                                          type='text')                            # choix: slug (info minime), text (texte seulement) ou html (html seulement)
  
  # Obtenir les données en anglais
  #queryEN <- quorum::createRadarplusQuery(begin_date=begin_date,
                                          #end_date=end_date,
                                          # title_contains=c('covid', 'legault'),
                                          # text_contains=c('covid', 'québec'),
                                          #tags=c('quorum', 'english'),
                                          #type='text')
  
  # On télécharge les données dans un data.table "result"
  ResultFR <- quorum::loadRadarplusData(queryFR, auth)
  
  # On télécharge les données dans un data.table "result"
  #ResultEN <- quorum::loadRadarplusData(queryEN, auth)
  
  # Mettre les données FR et celles EN ensemble
  #ResultTot <- ResultFR %>%
    #bind_rows(ResultEN)
  
  # Nettoyer les dates
  ResultFR$cleanDate <- NA
  ResultFR$cleanDate <- substr(ResultFR$begin_date, 1, nchar(ResultFR$begin_date)) # cette ligne pour AAAA/MM/JJ
  ResultFR$cleanDate2 <- substr(ResultFR$begin_date, 1, nchar(ResultFR$begin_date) - 3) # cette ligne pour AAAA/MM
  
  # On complète le csv avec des données de langue et de région!
  DataRadar <- ResultFR %>%
    mutate(language = ifelse(source %in% c("le-devoir", "la-presse", "tva-nouvelles", "journal-de-montreal", "radio-canada"),
                             "french", "english")) %>%
    mutate(area = ifelse(source %in% c("le-devoir", "la-presse", "tva-nouvelles", "journal-de-montreal", "radio-canada", "montreal-gazette"),
                         "quebec",
                         ifelse(source %in% c("new-york-times", "cnn", "fox-news", "wall-street-journal"),
                                "usa", "canada"))) %>%
    filter(area %in% c("canada", "quebec")) %>%
    mutate(headline_time = as.numeric(end_date - begin_date))
  
  return(DataRadar)
}

radarplus_data <- load_radarplus_data()






# Declare and define all the general-purpose functions specific to this script.  
# If there are functions that are shared between many scripts, you should ask 
# yourself whether they should go to in the clessnverse:: Package
#

# Ex:
#      get_lake_press_releases <- functions(parties_list) {
#         your function code goes here
#      }

###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################


###############################################################################
######################            Functions to           ######################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################


###############################################################################
######################            Functions to          ######################
######################   Get Data Sources from Dropbox   ######################
###############################################################################


###############################################################################
########################               Main              ######################
## This function is the core of your script. It can use global R objects and ##
## variables that you can define in the tryCatch section of the script below ##
###############################################################################

main <- function() {
    ###########################################################################
    # Define local objects of your core algorithm here
    # Ex: parties_list <- list("CAQ", "PLQ", "QS", "PCQ", "PQ")


    ###########################################################################
    # Start your main script here using the best practices in activity logging
    #
    # Ex: warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0)
    #     clessnverse::logit(scriptname, "Getting political parties press releases from the datalake", logger) 
    #     lakes_items_list <- get_lake_press_releases(parties_list)
    #      



}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch( 
  withCallingHandlers(
  {
    # Package dplyr for the %>% 
    # All other packages must be invoked by specifying its name
    # in front ot the function to be called
    library(dplyr) 

    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to 
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #



    #########################################################################
    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"




    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally") 
    if (!exists("scriptname")) scriptname <<- "l_agoraplus-pressreleases-qc"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    # opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))
    
    # if your script uses hub 2.0 uncomment the line below
    # clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    # or 
    # use this one
    # clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    status <<- 0

    # Call main script    
    main()
  },

  warning = function(w) {
      clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
      print(w)
      status <<- 2
  }),
  
  # Handle an error or a call to stop function in the code
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
    print(e)
    status <<- 1
  },
  
  # Terminate gracefully whether error or not
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::logclose(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)

    quit(status = status)
  }
)

