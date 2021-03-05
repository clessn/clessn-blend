logit <- function(message) {
  logger <- file(Sys.getenv("LOG_FILENAME"), open = "at")
  sink(logger, type="message")
  cat(format(Sys.time(), "%Y-%m-%d %X"), "-",
      paste(Sys.getenv("SCRIPT_FILENAME"),":",message), "\n",
      append = T,file = logger)
} 


installPackages <- function() {
  ###############################################################################
  ##### Install the required packages if they are not installed
  #####
  logit("installPackages: start")
  logit("installPackages: set required packages")
  required_packages <- c("stringr", 
                         "RCurl", 
                         "httr",
                         "jsonlite",
                         "dplyr", 
                         "XML", 
                         "tm",
                         "tidytext", 
                         "tibble",
                         "devtools",
                         "countrycode",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r",
                         "ropensci/gender",
                         "lmullen/genderdata"
  )
  
  ### Install missing packages
  logit("installPackages: installing missing packages:")
  new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
  
  for (p in 1:length(new_packages)) {
    if ( grepl("\\/", new_packages[p]) ) {
      logit(paste("installPackages: installing with devtools::install_github", new_packages[p]))
      devtools::install_github(new_packages[p])
    } else {
      logit(paste("installPackages: installing with install.packages", new_packages[p]))
      install.packages(new_packages[p])
    }  
  }
  
  ###############################################################################
  ##### load the packages
  ##### We will not invoque the CLESSN packages with 'library'. The functions 
  ##### in the package will have to be called explicitely with the package name
  ##### in the prefix : example clessnverse::evaluateRelevanceIndex
  ##### 
  for (p in 1:length(required_packages)) {
    if ( !grepl("\\/", required_packages[p]) ) {
      logit(paste("installPackages: loading", required_packages[p]))
      library(required_packages[p], character.only = TRUE)
    } else {
      if (grepl("clessn-hub-r", required_packages[p])) {
        packagename <- "clessnhub"
      } else {
        packagename <- stringr::str_split(required_packages[p], "\\/")[[1]][2]
      }
      logit(paste("installPackages: loading", packagename))
      library(packagename, character.only = TRUE)
    }
  }
}




init <- tryCatch( 
  {
    scriptname <- "main.r"
    
    Sys.setenv(SCRIPT_FILENAME = scriptname)
    Sys.setenv(LOG_FILENAME = paste("log/",scriptname,".txt",sep=""))
    
    installPackages()
    
    clessnverse::version()
  },

  warning = function(w) { 
    logit(w)
  },
  
  error = function(e) {
    logit(e)
  },
  
  finally={
    logit("Execution terminated")
  }
)


script1 <- tryCatch( 
  {
    scriptname <- "main.r"
    
    Sys.setenv(SCRIPT_FILENAME = scriptname)
    Sys.setenv(LOG_FILENAME = paste("log/",scriptname,".txt",sep=""))
    
    logit("launching agora-plus-debats-v1.r")
    source("agora-plus-debats-v1.R")
  },
  
  warning = function(w) { 
    logit(w)
  },
  
  error = function(e) {
    logit(e)
  },
  
  finally={
    logit("Execution terminated")
  }
)

script2 <- tryCatch( 
  {
    scriptname <- "agora-plus-quebec.r"
    
    Sys.setenv(SCRIPT_FILENAME = scriptname)
    Sys.setenv(LOG_FILENAME = paste("log/",scriptname,".txt",sep=""))
    
    logit("launching agora-plus-debats-v1.r")
    source("agora-plus-debats-v1.R")
  },
  
  warning = function(w) { 
    logit(w)
  },
  
  error = function(e) {
    logit(e)
  },
  
  finally={
    logit("Execution terminated")
  }
)