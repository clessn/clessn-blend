###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                             agora-plus-debats                               #
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
  logit("installPackages: start")
  logit("installPackages: set required packages")
  required_packages <- c("stringr", 
                         "tidyr",
                         "optparse",
                         "RCurl", 
                         "httr",
                         "jsonlite",
                         "dplyr", 
                         "XML", 
                         "tm",
                         "cld3",
                         "textcat",
                         "tidytext", 
                         "tibble",
                         "devtools",
                         "countrycode",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r",
                         "ropensci/gender",
                         "lmullen/genderdata")
  
  # Install missing packages
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
  
  # load the packages
  # We will not invoque the CLESSN packages with 'library'. The functions 
  # in the package will have to be called explicitely with the package name
  # in the prefix : example clessnverse::evaluateRelevanceIndex
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
    }
  }
} # </function installPackages>

###############################################################################
#   Function : loginit, logit and logclose
#   This function is used to log the script activities into a file for 
#   automation debug and monitoring purposed
#

loginit <- function(script) {
  log_handle <- file(paste("./log/",script,".log",sep=""), open = "at")
  
  return(log_handle)
}

logit <- function(message) {
  cat(format(Sys.time(), "%Y-%m-%d %X"), "-", .ChildEnv$scriptname, ":", message, "\n", 
      append = T,
      file = .ChildEnv$logger)
}

logclose <- function(log_handle) {
  close(log_handle)
}


###############################################################################
#   Globals
#   .ChildEnv : 
#   .ChildEnv$scriptname
#   .ChildEnv$logger
#

if (!exists(".ChildEnv")) .ChildEnv <- new.env()
.ChildEnv$scriptname <- "agora-plus-debats-v1.R"
.ChildEnv$logger <- loginit(.ChildEnv$scriptname)


###############################################################################
########################               MAIN              ######################
###############################################################################


installPackages()

#logclose(.ChildEnv$logger)
stop("this was a test")

###############################################################################
##### Set the update modes of each database in the HUB
#####
##### Possible values : update, refresh, rebuild or skip
##### update : updates the dataset by adding only new observations to it
##### refresh : refreshes existing observations and adds new observations to the dataset
##### rebuild : wipes out completely the dataset and rebuilds it from scratch
##### skip : does not make any change to the dataset
#####
opt_cache_update <- "update"
opt_simple_update <- "update"
opt_deep_update <- "update"
opt_hub_update <- "update"
opt_csv_update <- "skip"


#####
##### set which backend we're working with
##### - CSV : work with the CSV in the shared folders - good for testing
#####         or for datamining and research or messing around
##### - HUB : work with the CLESSNHUB data directly : this is prod data
#####
#opt_backend_type <- "CSV"
opt_backend_type <- "HUB"


removeSpeakerTitle <- function(string) {
  result <- ""
  
  if ( grepl("M\\.", string) )  result <- gsub("M.", "", string) 
  if ( grepl("Mme", string) ) result <- gsub("Mme", "", string) 
  
  result <- sub("^\\s+", "", result)
  result <- sub("\\s+$", "", result)
  
  if ( result == "" ) result <- string
  
  return(result)
} 


###############################################################################
##### Get some data to start the fun!
#####

###
### connect to the dataSource : the provincial parliament web site 
### get the index page containing the URLs to all the press conference
### to extract those URLS and get them individually in order to parse
### each press conference
###
base.url <- "http://www.assnat.qc.ca"
content.url <- "/fr/travaux-parlementaires/journaux-debats.html"

######
# Pour obtenir l'historique des débats depuis le début de l'année 2020 enlever le commentaire dans le ligne ci-dessous
#doc.html <- getURL("file:///Users/patrick/Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/JournalDebatsHistorique2020.html")

######
# Pour rouler le script sur une base quotidienne et aller chercher les débats récents Utiliser le ligne ci-dessous à la place
doc.html <- getURL(paste(base.url,content.url,sep=""))


parsed.html <- htmlParse(doc.html, asText = TRUE)
doc.urls <- xpathSApply(parsed.html, "//a/@href")
list.urls <- doc.urls[grep("assemblee-nationale/42-1/journal-debats", doc.urls)]

###
### Define the datasets containing
### - the cache which contains the previously scraped html content
### - dfSimple which contains the entire content of each press conference per observation
### - dfDeep which contains each intervention of each press conference per observation
###
### We only do this if we want ro rebuild those datasets from scratch to start fresh
### or if then don't exist in the environment of the current R session
###
if ( !exists("dfCache") || opt_cache_update == "rebuild" ) 
  dfCache <- data.frame(eventID = character(),
                        eventHtml = character(),
                        stringsAsFactors = FALSE)

if ( !exists("dfSimple") || opt_simple_update == "rebuild" ) 
  dfSimple <- data.frame(eventID = character(),
                         eventSourceType = character(),
                         eventURL = character(),
                         eventDate = character(), 
                         eventStartTime = character(),
                         eventEndTime = character(), 
                         eventTitle = character(), 
                         eventSubtitle = character(),
                         eventSentenceCount = character(),
                         eventParagraphCount = integer(),
                         eventContent = character(),
                         eventTranslatedContent = character(),
                         stringsAsFactors = FALSE)

if ( !exists("dfDeep") || opt_deep_update == "rebuild" ) 
  dfDeep <- data.frame(eventID = character(),
                       interventionSeqNum = integer(),
                       speakerFirstName = character(),
                       speakerLastName = character(),
                       speakerFullName = character(),
                       speakerGender = character(),
                       speakerIsMinister = character(),
                       speakerType = character(),
                       speakerParty = character(),
                       speakerCirconscription = character(),
                       speakerMedia = character(),
                       speakerSpeechType = character(),
                       speakerSpeechLang = character(),
                       speakerSpeechWordCount = integer(),
                       speakerSpeechSentenceCount = integer(),
                       speakerSpeechParagraphCount = integer(),
                       speakerSpeech = character(),
                       speakerTranslatedSpeech = character(), 
                       stringsAsFactors = FALSE)
  
#####
##### Get all data from the HUB or from CSV
##### - Cache which contains the raw html scraped from the assnat.qc.ca site
##### - dfSimple which contains one observation per event (débat or press conf)
##### - dfDeep which contains one observation per intervention per event
##### - journalists : a reference dataframe that contains the journalists in order
#####                 to add relevant data on journalists in the interventions
#####                 dataset
##### - deputes     : a reference dataframe that contains the deputes in order
#####                 to add relevant data on journalists in the interventions
#####                 dataset
#####
if (opt_backend_type == "HUB") {
  ### Connect to the HUB
  clessnhub::configure()

  ###
  # Récuperer les données du cache pour ne pas avoir à aller rechercher 
  # sur le site de l'assnat ce qu'on est allé déjà chercher auparavant  
  # C'est pour éviter de lever des suspicions de la part des admins de  
  # l'assnat avec trop de http GET répetitifs trop rapprochés
  ###
  if (opt_cache_update != "rebuild" && opt_cache_update != "skip") {
    dfCache.hub <- clessnhub::download_table('agoraplus_warehouse_cache_items')
    if (is.null(dfCache.hub)) {
      dfCache.hub <- data.frame(uuid = character(),
                                created = character(),
                                modified = character(),
                                metedata = character(),
                                eventID = character(),
                                eventHtml = character(),
                                stringsAsFactors = FALSE)
    } 
    
    dfCache <- dfCache.hub[,-c(1:4)]
  }
  
  
  
  
  ###
  # Récuperer les données Simple et Deep 
  ###
  if (opt_simple_update != "rebuild" && opt_simple_update != "skip") {
    dfSimple.hub <- clessnhub::download_table('agoraplus_warehouse_event_items')
    if (is.null(dfSimple.hub)) {
      dfSimple.hub <- data.frame(uuid = character(),
                                 created = character(),
                                 modified = character(),
                                 metedata = character(),
                                 eventID = character(),
                                 eventSourceType = character(),
                                 eventURL = character(),
                                 eventDate = character(), 
                                 eventStartTime = character(),
                                 eventEndTime = character(), 
                                 eventTitle = character(), 
                                 eventSubtitle = character(),
                                 eventSentenceCount = character(),
                                 eventParagraphCount = integer(),
                                 eventContent = character(),
                                 eventTranslatedContent = character(),
                                 stringsAsFactors = FALSE)
    }
    
    dfSimple <- dfSimple.hub[,-c(1:4)]
  }
  
  
  if (opt_deep_update != "rebuild" && opt_deep_update != "skip") {
    dfDeep.hub <- clessnhub::download_table('agoraplus_warehouse_intervention_items')
    if (is.null(dfDeep.hub)) {
      dfDeep.hub <- data.frame(uuid = character(),
                               created = character(),
                               modified = character(),
                               metedata = character(),
                               eventID = character(),
                               interventionSeqNum = integer(),
                               speakerFirstName = character(),
                               speakerLastName = character(),
                               speakerFullName = character(),
                               speakerGender = character(),
                               speakerIsMinister = character(),
                               speakerType = character(),
                               speakerParty = character(),
                               speakerCirconscription = character(),
                               speakerMedia = character(),
                               speakerSpeechType = character(),
                               speakerSpeechLang = character(),
                               speakerSpeechWordCount = integer(),
                               speakerSpeechSentenceCount = integer(),
                               speakerSpeechParagraphCount = integer(),
                               speakerSpeech = character(),
                               speakerTranslatedSpeech = character(), 
                               stringsAsFactors = FALSE)
    }
    
    dfDeep <- dfDeep.hub[,-c(1:4)]
  }
  
  deputes <- clessnhub::download_table('warehouse_quebec_mnas')
  deputes <- deputes %>% separate(lastName, c("lastName1", "lastName2"), " ")
  
  journalists <- clessnhub::download_table('warehouse_journalists')
  
} #if (opt_backend_type == "HUB")


if (opt_backend_type == "CSV") {
  
  if (opt_cache_update != "rebuild")
    dfCache <- read.csv2(file =
                           "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfCacheAgoraPlus-v3.csv",
                           #"/Users/patrick/dfCacheAgoraPlus-v3.csv",
                           sep=";", comment.char="#")  
  
  if (opt_simple_update != "rebuild")
    dfSimple <- read.csv2(file=
                            "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfSimpleAgoraPlus-v3.csv",
                            #"/Users/patrick/dfSimpleAgoraPlus-v3.csv",
                            sep=";", comment.char="#", encoding = "UTF-8")
  
  if (opt_deep_update != "rebuild")
    dfDeep <- read.csv2(file=
                          "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfDeepAgoraPlus-v3.csv",
                          #"/Users/patrick/dfDeepAgoraPlus-v3.csv",
                          sep=";", comment.char="#", encoding = "UTF-8")
  
  deputes <- read.csv(
    "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/Deputes_Quebec_Coordonnees.csv",
    sep=";")
  deputes <- deputes %>% separate(nom, c("firstName", "lastName1", "lastName2"), " ")
  names(deputes)[names(deputes)=="femme"] <- "isFemale"
  names(deputes)[names(deputes)=="parti"] <- "party"
  names(deputes)[names(deputes)=="circonscription"] <- "currentDistrict"
  names(deputes)[names(deputes)=="ministre"] <- "isMinister"
  
  journalists <- read.csv(
    "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/journalist_handle.csv",
    sep=";")
  journalists$X <- NULL
  names(journalists)[names(journalists)=="female"] <- "isFemale"
  names(journalists)[names(journalists)=="author"] <- "fullName"
  names(journalists)[names(journalists)=="selfIdJourn"] <- "thinkIsJournalist"
  names(journalists)[names(journalists)=="handle"] <- "twitterHandle"
  names(journalists)[names(journalists)=="realID"] <- "twittweJobTitle"
  names(journalists)[names(journalists)=="user_id"] <- "twitterID"
  names(journalists)[names(journalists)=="protected"] <- "twitterAccountProtected"
  
} #if (opt_backend_type == "CSV")
  

#####
##### Create some reference vectors used for dates conversion or detecting 
##### patterns in the conferences
#####
months.fr <- c("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
               "octobre", "novembre", "décembre")
months.en <- c("january", "february", "march", "april", "may", "june", "july", "august", "september",
               "october", "november", "december")

patterns.titres <- c("M\\.", "Mme", "Modérateur", "Modératrice", "Le Modérateur", "La Modératrice",
                     "journaliste :", "Le Président", "La Présidente", "La Vice-Présidente",
                     "Le Vice-Président", "Titre :")

patterns.periode.de.questions <- c("période de questions", "période des questions",
                                   "prendre les questions", "prendre vos questions",
                                   "est-ce qu'il y a des questions", "passer aux questions")


###############################################################################
##### Let's get serious!!!
##### Run through the URLs list, get the html content from the cache if it is 
##### in it, or from the assnat website and start parsing it o extract the
##### press conference content
#####
for (i in 1:length(list.urls)) {
  clessnhub::refresh_token(configuration$token, configuration$url)
  current.url <- paste(base.url,list.urls[i],sep="")
  current.id <- str_replace_all(list.urls[i], "[[:punct:]]", "")
  
  cat("\n********************** Débat:", i, "de", length(list.urls), "**********************", sep = " ")
  #cat(current.id, "\n")

  # Make sure the data comes from the debats parlementaires (we know that from the URL)
  if (grepl("/travaux-parlementaires/assemblee-nationale/", current.url)) {     
    ###
    # If the data is not cache we get the raw html from assnat.qc.ca
    # if it is cached (we scarped it before), we prefer not to bombard
    # the website with HTTP_GET requests and ise the cached version
    ###
    if ( !(current.id %in% dfCache$eventID) ) {
      # Read and parse HTML from the URL directly
      doc.html <- getURL(current.url)
      doc.html.original <- doc.html
      doc.html <- str_replace_all(doc.html, "<a name=\"_Toc([:digit:]{8})\">([:alpha:])",
                                  "<a name=\"_Toc\\1\">Titre : \\2")
      doc.html <- str_replace_all(doc.html, "<a name=\"Page([:digit:]{5})\"></a>([:alpha:])",
                                  "<a name=\"Page\\1\"></a>Titre : \\2")
      parsed.html <- htmlParse(doc.html, asText = TRUE)
      cached.html <- FALSE
      cat("not cached")
    } else{ 
      # Retrieve the XML structure from dfCache and Parse
      doc.html <- dfCache$eventHtml[which(dfCache$eventID==current.id)]
      doc.html.original <- doc.html
      doc.html <- str_replace_all(doc.html, "<a name=\"_Toc([:digit:]{8})\">([:alpha:])",
                                  "<a name=\"_Toc\\1\">Titre : \\2")
      doc.html <- str_replace_all(doc.html, "<a name=\"Page([:digit:]{5})\"></a>([:alpha:])",
                                  "<a name=\"Page\\1\"></a>Titre : \\2")
      parsed.html <- htmlParse(doc.html, asText = TRUE)
      cached.html <- TRUE
      cat("cached")
    }
    
    # Dissect the text based on html tags
    doc.h1 <- xpathApply(parsed.html, '//h1', xmlValue)
    doc.h2 <- xpathApply(parsed.html, '//h2', xmlValue)
    doc.h3 <- xpathApply(parsed.html, '//h3', xmlValue)
    doc.span <- xpathApply(parsed.html, '//span', xmlValue)
    doc.span <- unlist(doc.span)
    
    # Valide la version : préliminaire ou finale
    if ( length(grep("version finale", tolower(doc.h2))) > 0 ) {
      version.finale <- TRUE
    }
    else {
      version.finale <- FALSE
      cat("version préliminaire")
    }
  
    #cat("Version finale = ", version.finale,"\n")
    
    if ( version.finale && 
         ( ((opt_simple_update == "update" && !(current.id %in% dfSimple$eventID) ||
             opt_simple_update == "refresh" ||
             opt_simple_update == "rebuild") ||
            (opt_deep_update == "update" && !(current.id %in% dfDeep$eventID) ||
             opt_deep_update == "refresh" ||
             opt_deep_update == "rebuild")) ||
           ((opt_hub_update == "refresh" ||
             opt_hub_update == "update") && current.id %in% dfSimple$eventID))
    ) {
      
      ###############################
      # Columns of the simple dataset
      date <- NA
      time <- NA
      title <- NA
      subtitle <- NA
      end.time <- NA
      doc.text <- NA
      
      # Extract SourceType
      current.source <- doc.span[34]
      current.source <- gsub("\n","",current.source)
      current.source <- gsub("\r","",current.source)
      current.source <- sub("^\\s+", "", current.source)
      current.source <- sub("\\s+$", "", current.source)
      
      # Extract date of the conference
      date.time <- doc.h2[6]
      
      date.time <- gsub("\n","",date.time)
      date.time <- gsub("\r","",date.time)
      date.time <- sub("^\\s+", "", date.time)
      date.time <- sub("\\s+$", "", date.time)
      
      date <- word(date.time[1],2:5)
      date <- gsub(",", "", date)
      day.of.week <- date[1]
      datestr <- paste(date[2],months.en[match(tolower(date[3]),months.fr)],date[4])
      date <- as.Date(datestr, format = "%d %B %Y")
      
      # Extract start time of the conference
      time <- word(date.time[1],6:8)
      if (time[3] == "") time[3] <- "00"
      time <- paste(time[1], ":", time[3])
      time <- gsub(" ", "", time)
      time <- strptime(paste(date,time), "%Y-%m-%d %H:%M")
      
      # Title and subtitle of the conference  
      title <- doc.h1[2]
      title <- gsub("\n","",title)
      title <- gsub("\r","",title)
      title <- sub("^\\s+", "", title)
      title <- sub("\\s+$", "", title)
      
      subtitle <- doc.h2[6]
      subtitle <- gsub("\n","",subtitle)
      subtitle <- gsub("\r","",subtitle)
      subtitle <- sub("^\\s+", "", subtitle)
      subtitle <- sub("\\s+$", "", subtitle)
      
        
      # Extract all the paragraphs (HTML tag is p, starting at
      # the root of the document). Unlist flattens the list to
      # create a character vector.
      doc.text <- unlist(xpathApply(parsed.html, '//p', xmlValue))
      
      # Replace all \n by spaces and clean leading and trailing spaces
      # and clean the conference vector of unneeded paragraphs
      
      doc.text <- gsub('\\n',' ', doc.text)
      doc.text <- sub("^\\s+", "", doc.text)
      doc.text <- sub("\\s+$", "", doc.text)

      start.index <- grep("ajournement", tolower(doc.text))[2]
      
      for (x in 0:start.index+1) doc.text[x] <- NA
      
      doc.text <- na.omit(doc.text)  
      
      # Figure out the end time of the conference
      end.time <- doc.text[length(doc.text)]
      end.time <- gsub("\\(",'', end.time)
      end.time <- gsub("\\)",'', end.time)
      end.time <- words(end.time)
      
      if ( end.time[length(end.time)] == "heures" ) {
        end.time[length(end.time)] <- ":"
        end.time[length(end.time)+1] <- "00"
      }
      
      end.time <- paste(end.time[length(end.time)-2],":",end.time[length(end.time)])
      end.time <- gsub(" ", "", end.time)
      end.time <- strptime(paste(date,end.time), "%Y-%m-%d %H:%M")
      
      # We no longer need the last line
      doc.text[length(doc.text)] <- NA
      
      # Remove consecutive spaces (cleaning)
      doc.text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", doc.text, perl=TRUE)
    

      ####################################
      # The colums of the detailed dataset
      first.name <- NA
      last.name <- NA
      full.name <- NA
      gender <- NA
      type <- NA
      party <- NA
      circ <- NA
      is.minister <- NA
      media <- NA
      speech.type <- NA
      speech <- NA
      
      speaker <- data.frame()
      periode.de.questions <- FALSE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      seqnum <- 1
  
      event.paragraph.count <- length(doc.text) - 1
      event.sentence.count <- clessnverse::countVecSentences(doc.text) - 1
      
      for (j in 1:length(doc.text)) {
        
        #cat("Débat:", i, "Paragraph:", j, "\n", sep = " ")
        
        # Is this a new speaker taking the stand?  If so there is typically a : at the begining of the sentence
        # And the Sentence starts with the Title (M. Mme etc) and the last name of the speaker
        speech.paragraph.count <- 0
        intervention.start <- substr(doc.text[j],1,40)
        
        if ( (grepl("^M\\.\\s+(.*?)\\s+:", intervention.start) || 
              grepl("^Mme\\s+(.*?)\\s+:", intervention.start) || 
              grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?)\\s+:", intervention.start) ||
              grepl("^Titre(.*?):", intervention.start) ||
              grepl("^Journaliste(.*?):", intervention.start) ||
              grepl("^Modérat(.*?):", intervention.start) ||
              grepl("^Une\\svoix(.*?):", intervention.start) ||
              grepl("^Des\\svoix(.*?):", intervention.start)) &&
             !grepl(",", str_match(intervention.start, "^(.*):")) ) {
          # It's a new person speaking
          first.name <- NA
          last.name <- NA
          gender <- NA
          type <- NA
          party <- NA
          circ <- NA
          is.minister <- NA
          media <- NA
          speech.type <- NA
          speech <- NA
          speaker <- data.frame()
          
          speech.paragraph.count <- 1
          speech.sentence.count <- 0
          speech.word.count <- 0
          
          #cat("New person", doc.text[j], "\n")
          
          # let's rule out the president first
          if ( grepl("président(.*):", tolower(intervention.start)) ) { ### MODERATEUR ###
  
            first.name <- NA
            
            # On enlève les parenthèses
            if ( grepl("Mme", intervention.start) || grepl("M\\.", intervention.start) ) 
              last.name <- removeSpeakerTitle(str_match(intervention.start, "\\((.*)\\)\\s+:")[2])
            
            if ( grepl("le président(.*):", tolower(intervention.start)) ||
                 grepl("la présidente(.*):", tolower(intervention.start)) ) {
              type <- "président(e)"
              last.name <- "Paradis"
              first.name <- "François"
              gender <- "M"
              #speaker <- subset(deputes, grepl(paste(tolower(first.name),tolower(last.name),sep=" "),tolower(nom)))
              speaker <- filter(deputes, tolower(firstName) == tolower(first.name) & (tolower(lastName1) == tolower(last.name) | tolower(lastName2) == tolower(last.name)))
              
            } else {
              if ( grepl("le vice-président(.*):", tolower(intervention.start)) ||
                   grepl("la vice-présidente(.*):", tolower(intervention.start)) ) {
                type <- "vice-président(e)"
                
                if ( grepl("mme(.*):", tolower(intervention.start)) )
                  gender.femme <- 1
                else
                  gender.femme <- 0
                    
                last.name <- removeSpeakerTitle(str_match(intervention.start, "\\((.*)\\)\\s+:")[2])
                
                #speaker <- subset(deputes, grepl(tolower(last.name), tolower(nom)) & grepl(gender.femme, femme))
                ln1 <- word(last.name, 1)
                ln2 <- word(last.name, 2)
                if (is.na(ln2)) {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender.femme)
                } else {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender.femme)
                }
                ln1 <- NA
                ln2 <- NA
    
              }
            }
            
            media <- NA
            
            if (  1 %in% match(patterns.periode.de.questions, tolower(c(intervention.start)),FALSE) )
              periode.de.questions <- TRUE
            
            if ( nrow(speaker) > 0 ) { 
              last.name <- paste(na.omit(speaker[1,]$lastName1), na.omit(speaker[1,]$lastName2), sep = " ")
              last.name <- trimws(last.name, which = c("both"))
              if (length(last.name) == 0) last.name <- NA
              first.name <- speaker[1,]$firstName
              gender <- if ( speaker[1,]$isFemale ) "F" else "M"
              if ( tolower(speaker[1,]$party) == "fonctionnaire" ) {
                type <- "fonctionnaire"
                party <- NA
                circ <- NA
              }
              else {
                type <- "élu(e)"
                party <- speaker[1,]$party
                circ <- speaker[1,]$currentDistrict
              }
              
              media <- NA
              is.minister <- speaker$isMinister[1]
            } # ( nrow(speaker) > 0 ) 
            
            speech.type <- "modération"
            speech <- substr(doc.text[j], unlist(gregexpr(":", intervention.start))+1, nchar(doc.text[j]))
            speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
            
            
          } else {  ### DÉPUTÉ ou SECRÉTAIRE ###
            gender.femme <- 0
            
            if ( grepl("Mme", str_match(intervention.start, "(.+)\\s+:")[1]) || 
                 grepl("M\\.", str_match(intervention.start, "(.+)\\s+:")[1]) ) {
              
              if ( grepl("\\(", str_match(intervention.start, "(.+)\\s+:")[1]) ) {
                
                circ <- removeSpeakerTitle(str_match(intervention.start, "\\((.*)\\)")[2])
                
                if ( grepl("Mme", str_match(intervention.start, "(.+)\\s+:"))[1] ) {
                  last.name <- str_match(intervention.start, "Mme\\s+(.*?)\\s+\\(")[2]
                  gender.femme <- 1
                }
                else {
                  last.name <- str_match(intervention.start, "M\\.\\s+(.*?)\\s+\\(")[2]
                }
                
              } else {
                
                if ( grepl("Mme", str_match(intervention.start, "(.+)\\s+:"))[1] ) {
                  last.name <- str_match(intervention.start, "Mme\\s+(.*?)\\s+:")[2]
                  gender.femme <- 1
                }
                else {
                  last.name <- str_match(intervention.start, "M\\.\\s+(.*?)\\s+:")[2]
                }
                
              }
              last.name <- sub("^\\s+", "", last.name)
              last.name <- sub("\\s+$", "", last.name)
              
              ln1 <- word(last.name, 1)
              ln2 <- word(last.name, 2)
              
              first.name <- NA
              
              if ( is.na(circ) ) {
                #speaker <- deputes[match("TRUE", str_detect(tolower(deputes$nom), tolower(last.name))),]
                #speaker <- subset(deputes, grepl(tolower(last.name), tolower(nom)))
                if (is.na(ln2)) {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender.femme)
                } else {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender.femme)
                }
              } else {
                #speaker <- subset(deputes, grepl(tolower(last.name), tolower(nom)) & grepl(tolower(circ), tolower(circonscription)))
                if (is.na(ln2)) {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & tolower(currentDistrict) == tolower(circ))
                } else {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & tolower(currentDistrict) == tolower(circ))
                }
              }
              
              ln1 <- NA
              ln2 <- NA
            }
            
            if ( nrow(speaker) > 0 ) { 
                  #cat("we have a politician", last.name, "\n")
                  last.name <- paste(na.omit(speaker[1,]$lastName1), na.omit(speaker[1,]$lastName2), sep = " ")
                  last.name <- trimws(last.name, which = c("both"))
                  if (length(last.name) == 0) last.name <- NA
                  first.name <- speaker[1,]$firstName
                  gender <- if ( speaker[1,]$isFemale ) "F" else "M"
                  if ( tolower(speaker[1,]$party) == "fonctionnaire" ) {
                    type <- "fonctionnaire"
                    party <- NA
                    circ <- NA
                  }
                  else {
                    type <- "élu(e)"
                    party <- speaker[1,]$party
                    circ <- speaker[1,]$currentDistrict
                  }
                  
                  media <- NA
                  is.minister <- speaker$isMinister[1]
            } # ( nrow(speaker) > 0 ) 

                        
            if (j == 1)
              speech.type <- "allocution"
            else
              if ( periode.de.questions || substr(doc.text[j-1], nchar(doc.text[j-1]),
                                                  nchar(doc.text[j-1])) == "?" ) 
                speech.type <- "réponse"
              else
                speech.type <- "allocution"
          }

            
          if ( grepl("titre :", tolower(intervention.start)) ) {
            first.name <- NA
            last.name <- NA
            gender <- NA
            type <- NA
            party <- NA
            circ <- NA
            media <- NA
            speaker <- data.frame()
            speech.type <- "titre"
          }
            
          if ( grepl("secrétaire", tolower(intervention.start)) ) {
            first.name <- NA
            last.name <- NA
            gender <- NA
            type <- "secrétaire ou secrétaire adjoint(e)"
            party <- NA
            circ <- NA
            media <- NA
            speaker <- data.frame()
            speech.type <- "modération"
          }
          
          speech <- substr(doc.text[j], unlist(gregexpr(":", intervention.start))+1, nchar(doc.text[j]))
          speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
          
          
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          speech <- paste(speech,"\n\n",doc.text[j], sep="")
          speech.paragraph.count <- speech.paragraph.count + 1
        }
        
          language <- textcat(str_replace_all(speech, "[[:punct:]]", ""))
          if ( !(language %in% c("english","french")) ) { 
            language <- NA
          }
          else language <- substr(language,1,2)

        speech.sentence.count <- clessnverse::countSentences(paste(speech, collapse = ' '))
        speech.word.count <- length(words(removePunctuation(paste(speech, collapse = ' '))))
        speech.paragraph.count <- str_count(speech, "\\n\\n")+1

        if (is.na(first.name) && is.na(last.name)) 
          full.name <- NA
        else 
          full.name <- trimws(paste(na.omit(first.name), na.omit(last.name), sep = " "),which = "both")
        
        # If the next speaker is different or if it's the last record, then let's commit this observation into the dataset
        
        if ( ((grepl("^M\\.\\s+(.*?)\\s+:", substr(doc.text[j+1],1,40)) || 
               grepl("^Mme\\s+(.*?)\\s+:", substr(doc.text[j+1],1,40)) || 
               grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?)\\s+:", substr(doc.text[j+1],1,40)) ||
               grepl("^Titre(.*?):", substr(doc.text[j+1],1,40)) ||
               grepl("^Journaliste(.*?):", substr(doc.text[j+1],1,40)) ||
               grepl("^Modérat(.*?):", substr(doc.text[j+1],1,40)) ||
               grepl("^Une\\svoix(.*?):", substr(doc.text[j+1],1,40)) ||
               grepl("^Des\\svoix(.*?):", substr(doc.text[j+1],1,40))) &&
              !grepl(",", str_match(substr(doc.text[j+1],1,40), "^(.*):"))) &&
              !is.na(doc.text[j]) || 
              (j == length(doc.text)-1 && is.na(doc.text[j+1]))
           ) {  
          
          matching.deep.row.index <- 0
          # Commit a new row if we rebuilt the df from scratch
          if ( (opt_deep_update == "rebuild") ||
               (opt_deep_update == "refresh" && 
                nrow(dplyr::filter(dfDeep, eventID == current.id & interventionSeqNum == seqnum)) == 0) ||
               (opt_deep_update == "update" && 
                nrow(dplyr::filter(dfDeep, eventID == current.id & interventionSeqNum == seqnum)) == 0) ) {
            matching.deep.row.index <- nrow(dfDeep) + 1
            dfDeep   <- rbind.data.frame(dfDeep, data.frame(eventID = current.id,
                                                            interventionSeqNum = seqnum,
                                                            speakerFirstName = first.name,
                                                            speakerLastName = last.name,
                                                            speakerFullName = full.name,
                                                            speakerGender = gender,
                                                            speakerIsMinister = is.minister,
                                                            speakerType = type,
                                                            speakerParty = party,
                                                            speakerCirconscription = circ,
                                                            speakerMedia = media,
                                                            speakerSpeechType = speech.type,
                                                            speakerSpeechLang = language,
                                                            speakerSpeechWordCount = speech.word.count,
                                                            speakerSpeechSentenceCount = speech.sentence.count,
                                                            speakerSpeechParagraphCount = speech.paragraph.count,
                                                            speakerSpeech = speech,
                                                            speakerTranslatedSpeech = NA,
                                                            stringsAsFactors = FALSE))
          }
          
          
          # Update the existing row with primary key eventID*interventionSeqNum  
          if (opt_deep_update == "refresh" && 
              nrow(dplyr::filter(dfDeep, eventID == current.id & interventionSeqNum == seqnum)) > 0) {
            matching.deep.row.index <- which(dfDeep$eventID == current.id & dfDeep$interventionSeqNum == seqnum)
            
            deep.line.to.update <- data.frame(eventID = current.id,
                                              interventionSeqNum = seqnum,
                                              speakerFirstName = first.name,
                                              speakerLastName = last.name,
                                              speakerFullName = full.name,
                                              speakerGender = gender,
                                              speakerIsMinister = is.minister,
                                              speakerType = type,
                                              speakerParty = party,
                                              speakerCirconscription = circ,
                                              speakerMedia = media,
                                              speakerSpeechType = speech.type,
                                              speakerSpeechLang = language,
                                              speakerSpeechWordCount = speech.word.count,
                                              speakerSpeechSentenceCount = speech.sentence.count,
                                              speakerSpeechParagraphCount = speech.paragraph.count,
                                              speakerSpeech = speech,
                                              speakerTranslatedSpeech = NA, 
                                              stringsAsFactors = FALSE)
            
            dfDeep[matching.deep.row.index,] <- deep.line.to.update
            deep.line.to.update <- data.frame()
          }
          
          if (matching.deep.row.index == 0 && opt_hub_update == "refresh") {
            matching.deep.row.index <- which(dfDeep$eventID == current.id & dfDeep$interventionSeqNum == seqnum)
          }
          
          
          ###
          ### If the backend is CLESSNHUB, we have to update the backend
          ### Either with a new record 
          ### or with an existing record is opt_hub_update == "refresh"
          ###
          if ( opt_hub_update != "skip" && opt_backend_type == "HUB" && matching.deep.row.index > 0) {
            matching.hub.row.index <- which(dfDeep.hub$eventID == dfDeep$eventID[matching.deep.row.index] & 
                                            dfDeep.hub$interventionSeqNum == dfDeep$interventionSeqNum[matching.deep.row.index])
            if (length(matching.hub.row.index) == 0) {
              # Here there was no existing observation in dfDeep for the intervention
              # being processed in this iteration so it's a new record that we
              # have to add to the HUB
              hubline.to.add <- dfDeep[matching.deep.row.index,] %>% 
                mutate_if(is.numeric , replace_na, replace = 0) %>% 
                mutate_if(is.character , replace_na, replace = "") %>%
                mutate_if(is.logical , replace_na, replace = 0)
              
              clessnhub::create_item(as.list(hubline.to.add), 'agoraplus_warehouse_intervention_items')
              hubline.to.add <- NULL
            } else {
              hubline.to.update <- dfDeep[matching.deep.row.index,] %>% 
                mutate_if(is.numeric , replace_na, replace = 0) %>% 
                mutate_if(is.character , replace_na, replace = "") %>%
                mutate_if(is.logical , replace_na, replace = 0)
              
              hubline.uuid <- dfDeep.hub$uuid[matching.hub.row.index]
              
              clessnhub::edit_item(hubline.uuid, as.list(hubline.to.update), 'agoraplus_warehouse_intervention_items')
              hubline.to.update <- NULL
              hubline.uuid <- NULL
            }
            matching.hub.row.index <- NULL
          }
  
          seqnum <- seqnum + 1
          
          first.name <- NA
          last.name <- NA
          full.name <- NA
          gender <- NA
          type <- NA
          party <- NA
          circ <- NA
          media <- NA
          speech.type <- NA
          speech <- NA
          
          speaker <- data.frame()
        } #If the next speaker is different or if it's the last record
      } # for (j in 1:length(doc.text)) : loop back to the next 
      
      
      # Join all the elements of the character vector into a single
      # character string, separated by spaces for the simple dataSet
      collapsed.doc.text <- paste(paste(doc.text, "\n\n", sep=""), collapse = ' ')
      collapsed.doc.text <- str_replace_all(
        string = collapsed.doc.text, pattern = "\n\n NA\n\n", replacement = "")
      
      matching.cache.row.index <- 0
      if (cached.html) {
        #cat("updating dfCache")
        matching.cache.row.index <- which(dfCache$eventID == current.id)
        if (opt_cache_update == "refresh") {
          dfCache$eventHtml[matching.cache.row.index] = doc.html.original
        }
      } else {
        #cat("adding to dfCache")
        matching.cache.row.index <- which(dfCache$eventID == current.id)
        if (length(matching.cache.row.index) > 0) {
          # The entry already exists
          # We do nothing
        } else {
          matching.cache.row.index <- nrow(dfCache) + 1
          dfCache <- rbind.data.frame(dfCache, data.frame(eventID = current.id, eventHtml = doc.html.original, stringsAsFactors = FALSE))
        }
      }
      
      
      ###
      ### If the backend is CLESSNHUB, we have to update the backend
      ### Either with a new record 
      ### or with an existing record is opt_hub_update == "refresh"
      ###
      if ( opt_hub_update != "skip" && opt_backend_type == "HUB" && matching.cache.row.index > 0) {
        matching.hub.row.index <- which(dfCache.hub$eventID == dfCache$eventID[matching.cache.row.index])
        if (length(matching.hub.row.index) == 0) {
          # Here there was no existing observation in dfSimple for the event
          # being processed in this iteration so it's a new record that we
          # have to add to the HUB
          hubline.to.add <- dfCache[matching.cache.row.index,] %>% 
            mutate_if(is.numeric , replace_na, replace = 0) %>% 
            mutate_if(is.character , replace_na, replace = "") %>%
            mutate_if(is.logical , replace_na, replace = 0)
          
          clessnhub::create_item(as.list(hubline.to.add), 'agoraplus_warehouse_cache_items')
          hubline.to.add <- NULL
        } else {
          hubline.to.update <- dfCache[matching.cache.row.index,] %>% 
            mutate_if(is.numeric , replace_na, replace = 0) %>% 
            mutate_if(is.character , replace_na, replace = "") %>%
            mutate_if(is.logical , replace_na, replace = 0)
          
          hubline.uuid <- dfCache.hub$uuid[matching.hub.row.index]
          
          clessnhub::edit_item(hubline.uuid, as.list(hubline.to.update), 'agoraplus_warehouse_cache_items')
          hubline.to.update <- NULL
          hubline.uuid <- NULL
        }
        matching.hub.row.index <- NULL
      }
      
      
      matching.simple.row.index <- 0
      if ( opt_simple_update == "refresh" && nrow(dplyr::filter(dfSimple, eventID == current.id)) > 0 ) {
        matching.simple.row.index <- which(dfSimple$eventID == current.id)
        
        dfSimple[matching.simple.row.index,]$eventSourceType = current.source
        dfSimple[matching.simple.row.index,]$eventURL = current.url
        dfSimple[matching.simple.row.index,]$eventDate = as.character(date)
        dfSimple[matching.simple.row.index,]$eventStartTime = as.character(time)
        dfSimple[matching.simple.row.index,]$eventEndTime = as.character(end.time)
        dfSimple[matching.simple.row.index,]$eventTitle = title
        dfSimple[matching.simple.row.index,]$eventSubtitle = subtitle
        dfSimple[matching.simple.row.index,]$eventSentenceCount = event.sentence.count
        dfSimple[matching.simple.row.index,]$eventParagraphCount = event.paragraph.count
        dfSimple[matching.simple.row.index,]$eventContent = collapsed.doc.text
        dfSimple[matching.simple.row.index,]$eventTranslatedContent = NA
      }
      
      if ( (opt_simple_update == "rebuild") ||
           (opt_simple_update == "update"  && nrow(dplyr::filter(dfSimple, eventID == current.id)) == 0) ||
           (opt_simple_update == "refresh" && nrow(dplyr::filter(dfSimple, eventID == current.id)) == 0) ) {
          
        matching.simple.row.index <- nrow(dfSimple) + 1
        
        dfSimple <- rbind.data.frame(dfSimple, data.frame(eventID = current.id,
                                                          eventSourceType = current.source,
                                                          eventURL = current.url,
                                                          eventDate = as.character(date), 
                                                          eventStartTime = as.character(time), 
                                                          eventEndTime = as.character(end.time), 
                                                          eventTitle = title, 
                                                          eventSubtitle = subtitle, 
                                                          eventSentenceCount = event.sentence.count,
                                                          eventParagraphCount = event.paragraph.count,
                                                          eventContent = collapsed.doc.text,
                                                          eventTranslatedContent = NA,
                                                          stringsAsFactors = FALSE))
      }
      
      if (matching.simple.row.index == 0 && opt_hub_update == "refresh") {
        matching.simple.row.index <- which(dfSimple$eventID == current.id)
      }
      
      ###
      ### If the backend is CLESSNHUB, we have to update the backend
      ### Either with a new record 
      ### or with an existing record is opt_hub_update == "refresh"
      ###
      if ( opt_hub_update != "skip" && opt_backend_type == "HUB" && matching.simple.row.index > 0) {
        matching.hub.row.index <- which(dfSimple.hub$eventID == dfSimple$eventID[matching.simple.row.index])
        if (length(matching.hub.row.index) == 0) {
          # Here there was no existing observation in dfSimple for the event
          # being processed in this iteration so it's a new record that we
          # have to add to the HUB
          hubline.to.add <- dfSimple[matching.simple.row.index,] %>% 
            mutate_if(is.numeric , replace_na, replace = 0) %>% 
            mutate_if(is.character , replace_na, replace = "") %>%
            mutate_if(is.logical , replace_na, replace = 0)
          
          clessnhub::create_item(as.list(hubline.to.add), 'agoraplus_warehouse_event_items')
          hubline.to.add <- NULL
        } else {
          hubline.to.update <- dfSimple[matching.simple.row.index,] %>% 
            mutate_if(is.numeric , replace_na, replace = 0) %>% 
            mutate_if(is.character , replace_na, replace = "") %>%
            mutate_if(is.logical , replace_na, replace = 0)
          
          hubline.uuid <- dfSimple.hub$uuid[matching.hub.row.index]
          
          clessnhub::edit_item(hubline.uuid, as.list(hubline.to.update), 'agoraplus_warehouse_event_items')
          hubline.to.update <- NULL
          hubline.uuid <- NULL
        }
        matching.hub.row.index <- NULL
      }
      
    } # version finale
    
  } #if (grepl("actualites-salle-presse", current.url))
  
} #for (i in 1:nrow(result))




if (opt_csv_update != "skip" && opt_backend_type == "CSV") { 
  write.csv2(dfCache, file=
               "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfCacheAgoraPlus-v3.csv",
               #"/Users/patrick/dfCacheAgoraPlus-v3.csv",
               row.names = FALSE)
  write.csv2(dfDeep, file=
               "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfDeepAgoraPlus-v3.csv",
               #"/Users/patrick/dfDeepAgoraPlus-v3.csv",
               row.names = FALSE)
  write.csv2(dfSimple, file=
               "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfSimpleAgoraPlus-v3.csv",
               #"/Users/patrick/dfSimpleAgoraPlus-v3.csv",
               row.names = FALSE)
}
