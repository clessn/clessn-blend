###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                        e_name_of_your_data_extractor                        
#                                                                             
# Some description
#                                                                             
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################

my_global_variable <- "my_value"

my_function <- function(parm1 = "default_value", parm2 = "defaul_value", parm3...) {

    # put some trycatch in your fonctions that
    # set status and
    # append final_message
    # for example

    while (everything_is_not_good) {
        tryCatch(
        {
            clessnverse::logit(scriptname, "doing this", logger)
            # some code that does stuff
        },
        error = function(e) {
            clessnverse::logit(scriptname, "give details about what went wrong", logger)
            status <<- 1 # this means error
            if (final_message == "") {
                final_message <- "message final qui va sortir dans les alertes dans slack"
            } else {
                final_message <- paste(final_message, "\n", "suite du message d'alerte")
            }
        }, 
        warning = function(w) {
            clessnverse::logit(scriptname, "give details about what went wrong", logger)
            status <<- 2 # this means a warning
            if (final_message == "") {
                final_message <- "message final qui va sortir dans les alertes dans slack"
            } else {
                final_message <- paste(final_message, "\n", "suite du message d'alerte")
            }
        },
        finally = {
            i_get_attempt <- i_get_attempt + 1
        }
        )
    }#</while>

} #</my_function>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function(scriptname, logger, credentials) {
    
  clessnverse::logit(scriptname, "starting main function", logger)
  
  for (i in 1:10) {
    clessnverse::logit(scriptname, paste("doing this - iteration #", i), logger)
    #do some stuff in a loop if a loop is needed - this is just an example

    # put some trycatch in your code that
    # set status and
    # append final_message

  }#</for (i in 1:10)>
  
}



tryCatch( 
    withCallingHandlers(
    {
        library(dplyr)
        
        if (!exists("scriptname")) scriptname <<- "the_name_of_the_script_without_R_extension"

        # you can use log_output = c("console") to debug your script if you want
        # but set it to c("file") before putting in automated containerized production
        opt <<- list(log_output = c("file"), hub_mode = "refresh")

        if (!exists("opt")) {
            opt <<- clessnverse::processCommandLineOptions()
        }

        if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
        
        # login to hublot
        clessnverse::logit(scriptname, "connecting to hub", logger)

        # connect to hublot
        credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))
        
        # or connect to hub2
        #clessnhub::login(
        #    Sys.getenv("HUB_USERNAME"),
        #    Sys.getenv("HUB_PASSWORD"),
        #    Sys.getenv("HUB_URL"))

        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

        status <<- 0
        final_message <<- ""
        total_press_releases_scraped <<- 0
        
        main(scriptname, logger, credentials)
    },

    warning = function(w) {
        clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
        print(w)
        final_message <<- if (final_message == "") w else paste(final_message, "\n", w, sep="")    
        status <<- 2
    }),
    
    error = function(e) {
        clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
        print(e)
        final_message <<- if (final_message == "") e else paste(final_message, "\n", e, sep="")    
        status <<- 1
    },
  
    finally={
        clessnverse::logit(scriptname, final_message, logger)

        clessnverse::logit(scriptname, 
            paste(
                some_global_variable_to_count_stuff_your_script_has_done, 
                "QC press releases were extracted from the web"
            ),
            logger
        )

        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
        clessnverse::log_close(logger)
        rm(logger)
        print(paste("exiting with status", status))
        quit(status = status)
    }
)


