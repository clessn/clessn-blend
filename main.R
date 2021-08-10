###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                           main-agoraplus-quebec                             #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################


###############################################################################
#   Function : installPackages
#   This function installs all packages requires in this script and all the
#   scripts called by this one
#

installPackages <- function() {
  # Define the required packages if they are not installed
  required_packages <- c("stringr", 
                         "optparse",
                         "sendmailR",
                         "clessn/clessnverse")
  
  # Install missing packages
  new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
  
  if (length(new_packages) >0) {
    for (p in 1:length(new_packages)) {
      if ( grepl("\\/", new_packages[p]) ) {
        if (grepl("clessnverse", new_packages[p])) {
          devtools::install_github(new_packages[p], ref = "v1", upgrade = "never", quiet = FALSE, build = FALSE)
        } else {
          devtools::install_github(new_packages[p], upgrade = "never", quiet = FALSE, build = FALSE)
        }
      } else {
        install.packages(new_packages[p])
      }  
    }
  } 
  
  # load the packages
  # We will not invoque the CLESSN packages with 'library'. The functions 
  # in the package will have to be called explicitely with the package name
  # in the prefix : example clessnverse::evaluateRelevanceIndex
  for (p in 1:length(required_packages)) {
    if ( !grepl("\\/", required_packages[p]) ) {
      library(required_packages[p], character.only = TRUE)
    } else {
      if (grepl("clessn-hub-r", required_packages[p])) {
        packagename <- "clessnhub"
      } else {
        packagename <- stringr::str_split(required_packages[p], "\\/")[[1]][2]
      }
    } 
  } 
  
} # </function installPackages>


###############################################################################
#   Globals
#
#script_list <- c("agoraplus-confpresse.R", "agoraplus-debats.R", "agoraplus-youtube.R")
script_list <- c("agoraplus-confpresse-v2.R", "agoraplus-debats-v2.R")


###############################################################################
########################               MAIN              ######################
###############################################################################


tryCatch( 
  {
    installPackages()
    
    main_logger <- clessnverse::loginit("main.R", "file", Sys.getenv("LOG_PATH"))

    clessnverse::logit("======================================", main_logger)
    clessnverse::logit(paste("Starting init program in main.R", 
                             "with parms", 
                             paste(commandArgs(), collapse=' ')), main_logger)
    
    clessnverse::version()
  },

  error = function(e) {
    clessnverse::logit(paste(e, collapse=''), main_logger)
    print(e)
  },
  
  finally={
    clessnverse::logit("Execution of init program terminated", main_logger)
  }
)

for (scriptname in script_list) {
  tryCatch( 
    {
      logger <- clessnverse::loginit(scriptname, "file", Sys.getenv("LOG_PATH"))
      opt <- list(cache_mode = "rebuild",simple_mode = "rebuild",deep_mode = "rebuild",
                  dataframe_mode = "update", hub_mode = "update", download_data = FALSE)
      
      clessnverse::logit(paste("launching", scriptname, "with options:", paste(names(opt), opt, collapse = ' ')), main_logger)
      
      source(paste("./agoraplus-quebec/R/", scriptname, sep=""))
    },
    
    error = function(e) {
      print(e)
      clessnverse::logit(paste(scriptname, ":", paste(e, collapse=' ')), main_logger)
    },
    
    finally={
      clessnverse::logit(paste("Execution of", scriptname, "terminated"), main_logger)
      logger <- clessnverse::logclose(logger)
    }
  )
}

clessnverse::logit(paste("Exitting main.R normally"), main_logger)
main_logger <- clessnverse::logclose(main_logger)

