###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                              l_file_to_hub2.0                               #
#                                                                             #
# This script takles a csv or xlsx file from hublot and loads it to hub2.0    #
# In the appropriate table.   Currently supported tables are                  #
#                                                                             #
#  - people                                                                   #
#                                                                             #
# The table targeted for loading the data depends on the 'content_type' meta  #
# data value of the file in hublot.                                           #
#                                                                             #
# So current supported content types are                                      #
#                                                                             #
#  - people                                                                   #
#                                                                             #
# The input file must be stored in hublot in the Files blob storage and must  #
# be named import_<content_type>_<records_type>                               #
#                                                                             #
# supported records_type are                                                  #
#                                                                             #
#  - mp                                                                       #
#  - candidate                                                                #
#  - journalist                                                               #
#  - media                                                                    #
#  - partner                                                                  #
#  - political_party                                                          #
#  - political_staff                                                          #
#  - public_service                                                           #
#                                                                             #
# The file name must be passed as first paramets of the script when it runs   #
# automated                                                                   #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################

# Declare and define all the general-purpose functions specific to this script.  
# If there are functions that are shared between many scripts, you should ask 
# yourself whether they should go to in the clessnverse:: Package
#
# Ex:
#      get_lake_press_releases <- functions(parties_list) {
#         your function code goes here
#      }

load_input_file_to_df(filename) {
    file_info <- hubr::retrieve_file(filename, credentials)

    if (file_info$metadata$format = "xlsx") {
        df <- openxlsx::read.xlsx(file_info$file)
        return(df)
    }

    if (file_info$metadata$format = "csv") {
        L <- readLines("myfile", n = 1)
        numfields <- count.fields(textConnection(L), sep = ";")
        if (numfields == 1) read.csv("myfile") else read.csv2("myfile")

        file <- read.csv2(file_info$file)
        return(df)
    }

    clessnverse::logit(scriptname = scriptname, 
                       message = paste("filetype", file_info$metadata$format, "for filename", filename, "not supporter"), 
                       logger = logger)
    return(data.frame())

}

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
    df <- load_input_file_to_df(opt$filename)


}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch( 
  {
    ###########################################################################
    # Package dplyr for the %>% 
    # All other packages must be invoked by specifying its name
    # in front ot the function to be called
    library(dplyr) 

    ###########################################################################
    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to 
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #

    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"
    filename <- "import_people_journalists"

    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally") 
    if (!exists("scriptname")) scriptname <<- "l_file_to_hublot"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    opt <- list(
        filename = filename,
        dataframe_mode = "refresh", hub_mode = "refresh",  
        log_output = c("file", "console"), download_data = FALSE, translate=FALSE
        )

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hublot", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))
    
    # Connecting to hub 2.0
    clessnhub::login(
        Sys.getenv("HUB_USERNAME"),
        Sys.getenv("HUB_PASSWORD"),
        Sys.getenv("HUB_URL"))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    # Call main script    
    main()
  },
  
  # Handle an error or a call to stop function in the code
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
    print(e)
  },
  
  # Terminate gracefully whether error or not
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::logclose(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)
  }
)

