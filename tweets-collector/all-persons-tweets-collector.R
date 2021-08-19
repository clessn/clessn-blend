###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        all-persons-tweets-collector                         #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################


###############################################################################
# Function : installPackages
# This function installs all packages requires in this script and all the
# scripts called by this one
#
installPackages <- function() {
  # Define the required packages if they are not installed
  required_packages <- c("stringr", 
                         "tidyr",
                         "optparse",
                         "RCurl", 
                         "httr",
                         "jsonlite",
                         "dplyr", 
                         "XML", 
                         "tm",
                         "textcat",
                         "tidytext", 
                         "tibble",
                         "twittr",
                         "devtools",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r")
  
  # Install missing packages
  new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
  
  for (p in 1:length(new_packages)) {
    if ( grepl("\\/", new_packages[p]) ) {
      devtools::install_github(new_packages[p], upgrade = "never", quiet = FALSE, build = FALSE)
    } else {
      install.packages(new_packages[p])
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
#   scriptname
#   logger
#
installPackages()
library(dplyr)

if (!exists("scriptname")) scriptname <- "all-persons-tweets-collector.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, "file", Sys.getenv("LOG_PATH"))

# login to the hub
clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

# get all mps and journalists
filter <- clessnhub::create_filter(type="mp", schema="v2")
dfMPs <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

filter <- clessnhub::create_filter(type="election_candidate", schema="v2")
dfCandidates <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

filter <- clessnhub::create_filter(type="journalist", schema="v2")
dfJournalists <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

dfPersons <- dfMPs %>% full_join(dfCandidates) %>% full_join(dfJournalists) %>%
  dplyr::select(data.fullName,data.twitterHandle) %>%
  na.omit()


###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it o extract the
# press conference content
#

#### 1. get_tweets ####

# Probablement besoin d'une clé pour elle
Tweets <- rtweet::get_timeline(Persons$data.twitterHandle[1],n=3200)

# Pas de clé, mais plus limitée
Tweets2 <- rtweet::search_tweets(q = paste0("from:",Persons$data.twitterHandle[4]),
                                retryonratelimit=T)

#To check limits of use for search_tweets
Limits<-rtweet::rate_limit()
Limits$limit[235]
Limits$remaining[235]

nbCandidates <- 338*5 
totalTimeMin <- ((nbCandidates/180)*15)
totalTimeHrs <- ((nbCandidates/180)*15)/60
# so it's typically gonna take 2 hours 21 minutes

#### 2. Wrap arround the table ####
for (i in 1:nrow(Persons)){
  Tweets <- rtweet::search_tweets(q = paste0("from:",Persons$data.twitterHandle[i]),
                                   retryonratelimit=T)
  if(i==1){
    TwitterData <- Tweets
  }
  else{
    TwitterData <- rbind(TwitterData,Tweets)
  }
  print(i)
}
