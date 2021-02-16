###############################################################################
##### Install the required packages if they are not installed
#####

required.packages <- c("clessn-verse",
                       "clessn-hub-r",
                       "NLP")

### Install missing packages
new.packages <- required.packages[!(required.packages %in% installed.packages()[,"Package"])]

for (p in 1:length(new.packages)) {
  if ( grepl("clessn", new.packages[p]) ) {
    devtools::install_github(paste("clessn/",new.packages[p],sep=""))
  } else {
    install.packages(new.packages[p])
  } 
}

### load the packages
### We will not invoque the CLESSN packages with 'library'. The functions 
### in the package will have to be called explicitely with the package name
### in the prefix : example clessnverse::evaluateRelevanceIndex
for (p in 1:length(required.packages)) {
  if ( !grepl("clessn", required.packages[p]) ) {
    library(required.packages[p], character.only = TRUE)
  }
}


upload_journalists_to_hub <- function(dataset) {
  for (i in 1:nrow(dataset)) {
  #for (i in 1:1) {
    cat(i,"\r")
    FirstName <- NLP::words(dataset$author[i])[1]
    LastName <- paste(NLP::words(dataset$author[i])[-1], collapse = ' ')
    FullName <- paste(FirstName, LastName, sep=' ')
    IsFemale <- dataset$female[i]
    ThinkIsJournalist <- dataset$selfIdJourn[i]
    Source <- dataset$source[i]
    TwitterHandle <- dataset$handle[i]
    TwitterID <- dataset$user_id[i]
    TwitterAccountProtected <- dataset$protected[i]
    TwitterJobTitle <- dataset$realID[i]
    
    if (!is.na(ThinkIsJournalist) && ThinkIsJournalist == 0.5) ThinkIsJournalist <-1
    if (is.na(ThinkIsJournalist)) ThinkIsJournalist <- 0 
    if (is.na(IsFemale)) IsFemale <- 0
    
    current_list <- list(firstName=FirstName, lastName=LastName, fullName=FullName, isFemale=IsFemale, 
                         thinkIsJournalist=ThinkIsJournalist, source=Source, twitterHandle=TwitterHandle, 
                         twitterID=TwitterID, twitterAccountProtected=TwitterAccountProtected, twittweJobTitle=TwitterJobTitle)
  
    clessnhub::create_item(current_list, 'warehouse_journalists')
  }
}


purge_journalists_in_hub <- function(dataset) {
  for (i in 1:nrow(dataset)) {
    cat(i,"\r")
    clessnhub::delete_item(dataset$uuid[i], 'warehouse_journalists')
  }
}



journalists <- read.csv(
  "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/journalist_handle.csv",
  sep=";")

journalists <- journalists %>% rbind(c(NA, 0, "antoine gosselin", "TVA Nouvelles", "PAGosselinTVA", NA, NA, NA, NA, NA))
journalists <- journalists %>% rbind(c(NA, 1, "gloria henriquez", "Global News", "GloriaMTL", NA, NA, NA, NA, NA))
journalists <- journalists %>% rbind(c(NA, 0, "marco bélair", "Le Devoir", "MBelairCirino", NA, NA, NA, NA, NA))
journalists <- journalists %>% rbind(c(NA, 1, "ariane lacoursère", "La Presse", NA, NA, NA, NA, NA, NA))
journalists <- journalists %>% rbind(c(NA, 0, "philip authier", "Montreal Gazette", "PhilipAuthier", NA, NA, NA, NA, NA))
journalists <- journalists %>% rbind(c(NA, 1, "angela mckenzie", "CTV", "AMacKenzieCTV", NA, NA, NA, NA, NA))

journalists <- rbind(journalists, data.frame(X=NA,female=NA, author="journaliste", source=NA,
                                             handle=NA,selfIdJourn=NA,realID=NA,user_id=NA,protected=NA))

journalists$X <- NULL

clessnhub::configure()

hub_journalists <- clessnhub::download_table('warehouse_journalists')

#upload_journalists_to_hub(journalists)
purge_journalists_in_hub(hub_journalists)

