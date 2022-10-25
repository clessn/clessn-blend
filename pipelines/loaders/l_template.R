###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                                  l_template                                 #
#                                                                             #
# Script pour extraire les pages html (ou autre format - ex: xml) sur le web  #
# contenant les communiqués de presse des partis politiques du Québec         #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################



###############################################################################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################


###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################


###############################################################################
######################   Get Data Sources from Dropbox   ######################
###############################################################################


###############################################################################
######################   Get Data Sources from WAS.      ######################
###############################################################################


###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
  
    clessnverse::logit(scriptname,  "starting main function", logger)

    # Put your code here 
    # Create functions above and call theme here to make your code more readable
    # Use global variables scriptname, logger, opt and status
    # Log lots of activity
    # Exit your script with stop(...) when there is a fatal error
    # Call warning() to issue warnings and alert the clessn that some non fatal problem occured during execution


    clessnverse::logit(scriptname, "ending main function"), logger)
    clessnverse::logit(scriptname, "write the results of your script here"), logger)

}




tryCatch( 
    withCallingHandlers(
    {
        # Package
        library(dplyr) 
        
        Sys.setlocale("LC_TIME", "fr_CA.utf8")

        # Globals : scriptname, opt, logger, credentials
        if (!exists("scriptname")) scriptname <<- "*** change your script name here *** ex: l_pressreleases_qc"

        opt <- list(dataframe_mode = "refresh", log_output = c("file"), hub_mode = "refresh", download_data = FALSE, translate=FALSE, refresh_data=TRUE)

        if (!exists("opt")) {
            opt <- clessnverse::process_command_line_options()
        }

        if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
        
        # login to hublot
        clessnverse::logit(scriptname, "connecting to hub", logger)

        credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))
        
        
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
        clessnverse::log_close(logger)

        # Cleanup
        closeAllConnections()
        rm(logger)
        cat("exit status: ", status, "\n")
        quit(status = status)
    }
)

