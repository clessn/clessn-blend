###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        l_datagotchi_elections_pilote                        #
#                                                                             #
#  This script wil take files in the data lake under the path                 #
#  datagotchi/elections/pilote and will process the csv files to load them    #
#  imnto the data warehouse                                                   #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################


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
    # Get files from the data lake
    data <- hublot::filter_lake_items(credentials, filter = lake_items_selection_metadata)

    # Load csv into a dataframe
    if (length(data$results) > 0) {
      input_df <- data.frame()

      url <- data$results[[1]]$file
      input_df <- read.csv(url)

    } else {
      clessnverse::logit(
        scriptname, 
        paste("could not find file corresponding to metadata", paste(lake_items_selection_metadata, collapse = " ")),
        logger
        )
      stop(paste("could not find file corresponding to metadata", paste(lake_items_selection_metadata, collapse = " ")))
    }

    # Load dataframe into warehouse table
    for (i in 1:nrow(input_df)) {
      row <- as.list(input_df[i,])

      clessnverse::commit_warehouse_row(
        table = warehouse_table_name, 
        key = input_df$CASEID[i], 
        row,
        refresh_data = TRUE,
        credentials
      )
    } #for (i in 1:nrow(input_df))

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
    lake_items_selection_metadata <- list(metadata__content_type="datagocthi_lifestyle_survey", metadata__storage_class="lake")
    warehouse_table_name <- "datagotchi_elxncan2021_pilot_1"


    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally") 
    if (!exists("scriptname")) scriptname <<- "l_datagotchi_elections_pilote"

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
    status <<- main()
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

