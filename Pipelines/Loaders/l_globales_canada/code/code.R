###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        l_globales_canada                                    #
#                                                                             #
#  This loader merges the CES from 1965 to 2019 together and creates          #
#  merged variables when it is appropriate.                                   #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################

# Function to prepare data of GlobalES data before loading it in the warehouse
process_info_globales <- function() {
  #log for each file processed
}

###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################

# Function to list lake items for GlobalES data by country
#
# Search all the files under the subdirectory corresponding to the country
# given in parameter in the GlobalES files in the lake
#
# Return a list of data.frame corresponding to the content of all files of the
# country subdirectory
get_lake_items_globales <- function() {

}

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

    # Initial logging
    clessnverse::logit(
      scriptname,
      paste("Getting the GlobalES data for ",
        country,
        " from year 1965 to 2019"
      ),
      logger)

    # Get lake items

    # Process lake items

    # Load items to the warehouse
      # log during loading of the data to the data warehouse

    # Final logging
    clessnverse::logit(
      scriptname,
      paste("GlobalES data for ",
      country,
      " from year 1965 to 2019 has been loaded to the warehouse",
      logger)
    )

}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch(
  withCallingHandlers({

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
    country <- "Canada"
    country_code <- "CAN"

    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally")
    if (!exists("scriptname")) scriptname <<- "l_globales_canada"

    # Uncomment the line below to hardcode the command line option passed to
    # this script when it runs
    # This is particularly useful while developping your script but it's wiser
    # to use real command-line options when puting your script in production in
    # an automated container.
    # opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    # Initialize the log file
    if (!exists("logger") || is.null(logger) || logger == 0) logger <<-
      clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

    # login to hublot
    clessnverse::logit(scriptname, "Connecting to hub3", logger)
    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"),
        Sys.getenv("HUB3_USERNAME"),
        Sys.getenv("HUB3_PASSWORD"))

    clessnverse::logit(
      scriptname,
      paste("Execution of",  scriptname, "starting"),
      logger
    )

    status <<- 0

    # Call main script
    main()
  },

  warning = function(w) {
    clessnverse::logit(scriptname, paste(w, collapse = ' '), logger)
    print(w)
    status <<- 2
  }),

  # Handle an error or a call to stop function in the code
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse = ' '), logger)
    print(e)
    status <<- 1
  },

  # Terminate gracefully whether error or not
  finally = {
    clessnverse::logit(
      scriptname,
      paste("Execution of",  scriptname, "program terminated"),
      logger
    )
    clessnverse::logclose(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)

    quit(status = status)
  }
)
