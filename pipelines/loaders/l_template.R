###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                                l_template                        
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
            some_variable_to_count_what_the_script_does <- some_variable_to_count_what_the_script_does + 1

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
        }
        )
    }#</while>

} #</my_function>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    
  clessnverse::logit(scriptname, "starting main function", logger)
  
  for (i in 1:10) {
    clessnverse::logit(scriptname, paste("doing this - iteration #", i), logger)
    #do some stuff in a loop if a loop is needed - this is just an example

    # put some trycatch in your code that
    # set status and
    # append final_message
    # call functions in your code

  }#</for (i in 1:10)>

  clessnverse::logit(scriptname, "ending main function"), logger)
  clessnverse::logit(scriptname, "write the results of your script here"), logger)
  
}



tryCatch( 
  withCallingHandlers(
  {
    library(dplyr)

    Sys.setlocale("LC_TIME", "fr_CA.utf8")
    
    status <<- 0
    final_message <<- ""
    some_variable_to_count_what_the_script_does <<- 0
    
    if (!exists("scriptname")) scriptname <<- "the_name_of_the_script_without_R_extension"

    # valid options for this script are
    #    log_output = c("file","console","hub")
    #    scrapind_method = c("range", "start_date", num_days, start_parliament, num_parliament) | "frontpage" (default)
    #    hub_mode = "refresh" | "skip" | "update"
    #    translate = "TRUE" | "FALSE"
    #    refresh_data = "TRUE" | "FALSE"
    #    
    #    you can use log_output = c("console") to debug your script if you want
    #    but set it to c("file") before putting in automated containerized production

    #opt <<- list(
    #    log_output = c("console"),
    #    scraping_method = c("date_range", "2014-09-18", 1, 8, 1),
    #    hub_mode = "refresh"
    #)

    if (!exists("opt")) {
        opt <<- clessnverse::process_command_line_options()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) {
      logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    }
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    # connect to hublot
    credentials <<- hublot::get_credentials(
      Sys.getenv("HUB3_URL"), 
      Sys.getenv("HUB3_USERNAME"), 
      Sys.getenv("HUB3_PASSWORD"))
    
    # or connect to hub2
    #clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)
    
    main()
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
        some_variable_to_count_what_the_script_does, 
        "some message to say what it has done"
      ),
      logger
    )

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::log_close(logger)
    if (exists("logger")) rm(logger)
    print(paste("exiting with status", status))
    quit(status = status)
  }
)