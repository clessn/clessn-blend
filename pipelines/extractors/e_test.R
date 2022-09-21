
main <- function(scriptname, logger, credentials) {
    #warning("Ceci est un warning qui est affiché seulement à la fin du script grâce a withCallingHandlers")
    stop("Voici une erreur générée par un script R et fournie en output pour être affichées dans Slack")
}


tryCatch( 
    withCallingHandlers(
    {
        library(dplyr)
        
        if (!exists("scriptname")) scriptname <<- "test"

        opt <<- list(dataframe_mode = "refresh", log_output = c("file"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

        if (!exists("opt")) {
            opt <<- clessnverse::process_command_line_options()
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
        
        main(scriptname, logger, credentials)
    },

    warning = function(w) {
        clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
        print(w)
        status <<- 2
    }),
    
    error = function(e) {
        clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
        print(e)
        status <<- 1
    },
  
    finally={
        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
        clessnverse::log_close(logger)
        rm(logger)
        quit(status = status)
    }
)


