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
#   Function : processCommandLineOptions
#
# Parse the command line options
# Which are the update modes of each database in the HUB or in the CSV backend
#
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataset by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataset
# - rebuild : wipes out completely the dataset and rebuilds it from scratch
# - skip : does not make any change to the dataset
# set which backend we're working with
# - CSV : work with the CSV in the shared folders - good for testing
#         or for datamining and research or messing around
# - HUB : work with the CLESSNHUB data directly : this is prod data
#
processCommandLineOptions <- function() {
  option_list = list(
    make_option(c("-c", "--cache_update"), type="character", default="rebuild", 
                help="update mode of the cache [default= %default]", metavar="character"),
    make_option(c("-s", "--simple_update"), type="character", default="rebuild", 
                help="update mode of the simple dataframe [default= %default]", metavar="character"),
    make_option(c("-d", "--deep_update"), type="character", default="rebuild", 
                help="update mode of the deep dataframe [default= %Adefault]", metavar="character"),
    make_option(c("-h", "--hub_update"), type="character", default="skip", 
                help="update mode of the hub [default= %default]", metavar="character"),
    make_option(c("-f", "--csv_update"), type="character", default="skip", 
                help="update mode of the simple dataframe [default= %default]", metavar="character"),
    make_option(c("-b", "--backend_type"), type="character", default="HUB", 
                help="type of the backend - either hub or csv [default= %default]", metavar="character")
  )
  
  opt_parser = OptionParser(option_list=option_list)
  opt = parse_args(opt_parser)
  
  return(opt)
}


###############################################################################
#   Globals
#
#   scriptname
#   logger
#
installPackages()

if (!exists("scriptname")) scriptname <- "agoraplus-youtube.R"
if (!exists("logger")) logger <- clessnverse::loginit(scriptname, "file")

# Create some reference vectors used for dates conversion or detecting patterns in the conferences
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
# Data source
# connect to the dataSource : the provincial parliament web site 
# get the index page containing the URLs to all the national assembly debates
# to extract those URLS and get them individually in order to parse
# each debate
#
base.url <- "http://www.assnat.qc.ca"
content.url <- "/fr/travaux-parlementaires/journaux-debats.html"


# Pour obtenir l'historique des débats depuis le début de l'année 2020 enlever le commentaire dans le ligne ci-dessous
#doc.html <- getURL("file:///Users/patrick/Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/JournalDebatsHistorique2020.html")
# Pour rouler le script sur une base quotidienne et aller chercher les débats récents Utiliser le ligne ci-dessous
doc.html <- getURL(paste(base.url,content.url,sep=""))


parsed.html <- htmlParse(doc.html, asText = TRUE)
doc.urls <- xpathSApply(parsed.html, "//a/@href")
list.urls <- doc.urls[grep("assemblee-nationale/42-1/journal-debats", doc.urls)]


###############################################################################
########################               MAIN              ######################
###############################################################################

if (!exists("opt")) {
  opt <- processCommandLineOptions()
}

clessnverse::logit(paste("command line options: ", 
                         paste(c(rbind(paste(" ",names(opt),"=",sep=''),opt)), collapse='')), logger)

# Process incompatible option sets
if ( opt$hub_update == "refresh" && 
     (opt$simple_update == "rebuild" || opt$deep_update == "rebuild" ||
      opt$simple_update == "skip" || opt$deep_update == "skip") ) 
  stop(paste("this set of options:", 
             paste("--hub_update=", opt$hub_update, " --simple_update=", opt$simple_update, " --deep_update=", opt$deep_update, sep=''),
             "will duplicate entries in the HUB, if you want to refresh the hub use refresh on all datasets"), call. = F)


# Get all data from the HUB or from CSV.  If neither was successful, then
# create empty datasets from scratch
#
# - Cache which contains the raw html scraped from the assnat.qc.ca site
# - dfSimple which contains one observation per event (débat or press conf)
# - dfDeep which contains one observation per intervention per event
# - journalists : a reference dataframe that contains the journalists in order
#                 to add relevant data on journalists in the interventions
#                 dataset
# - deputes     : a reference dataframe that contains the deputes in order
#                 to add relevant data on journalists in the interventions
#                 dataset
#
if (opt$backend_type == "HUB") {
  clessnverse::logit("getting data from HUB", logger)
  
  # Connect to the HUB
  clessnverse::logit(paste("login to the HUB", Sys.getenv("HUB_URL")), logger)
  clessnhub::login(username = Sys.getenv("HUB_USERNAME"), password = Sys.getenv("HUB_PASSWORD"), url = Sys.getenv("HUB_URL"))
  
  # Récuperer les données de Cache, Simple et Deep 
  if (opt$cache_update != "rebuild" && opt$cache_update != "skip" && 
      (!exists("dfCache") || is.null(dfCache) || nrow(dfCache) == 0) ||
      opt$hub_update == "refresh") {
    
    clessnverse::logit("getting cache from HUB", logger)
    dfCache <- clessnverse::loadCacheFromHub("quebec")
  }
  
  if (opt$simple_update != "rebuild" && opt$simple_update != "skip" && 
      (!exists("dfSimple") || is.null(dfSimple) || nrow(dfSimple) == 0) ||
      opt$hub_update == "refresh") {
    
    clessnverse::logit("getting simple from HUB", logger)
    dfSimple <- clessnverse::loadSimpleFromHub("quebec")
  }
  
  if (opt$deep_update != "rebuild" && opt$deep_update != "skip" && 
      (!exists("dfDeep") || is.null(dfDeep) || nrow(dfDeep) == 0) ||
      opt$hub_update == "refresh") {
    
    clessnverse::logit("getting deep from HUB", logger)
    dfDeep <- clessnverse::loadDeepFromHub("quebec")
  }
  
  clessnverse::logit("getting deputes from HUB", logger)
  deputes <- clessnhub::download_table('warehouse_quebec_mnas')
  deputes <- deputes %>% separate(lastName, c("lastName1", "lastName2"), " ")
  
  clessnverse::logit("getting journalists from HUB", logger)
  journalists <- clessnhub::download_table('warehouse_journalists')
  
} #if (opt$backend_type == "HUB")


if (opt$backend_type == "CSV") {
  clessnverse::logit("getting data from CSV", logger)
  
  base_csv_folder <- "../clessn-blend/_SharedFolder_clessn-blend/data/"
  
  if (opt$cache_update != "rebuild" && opt$cache_update != "skip") {
    clessnverse::logit("getting journalists from CSV", logger)
    dfCache <- read.csv2(file = paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''),
                         sep = ";", comment.char = "#")  
  }
  
  if (opt$simple_update != "rebuild" && opt$simple_update != "skip") {
    clessnverse::logit("getting Simple from CSV", logger)
    dfSimple <- read.csv2(file= paste(base_csv_folder,"dfSimpleAgoraPlus.csv",sep=''),
                          sep = ";", comment.char = "#", encoding = "UTF-8")
  }
  
  if (opt$deep_update != "rebuild" && opt$deep_update != "skip") {
    clessnverse::logit("getting Deep from CSV", logger)
    dfDeep <- read.csv2(file=paste(base_csv_folder,"dfDeepAgoraPlus.csv",sep=''),
                        sep = ";", comment.char = "#", encoding = "UTF-8")
  }
  
  
  clessnverse::logit("getting deputes from CSV", logger)
  deputes <- read.csv(file = paste(base_csv_folder,"Deputes_Quebec_Coordonnees.csv"), sep = ";")
  deputes <- deputes %>% separate(nom, c("firstName", "lastName1", "lastName2"), " ")
  names(deputes)[names(deputes)=="femme"] <- "isFemale"
  names(deputes)[names(deputes)=="parti"] <- "party"
  names(deputes)[names(deputes)=="circonscription"] <- "currentDistrict"
  names(deputes)[names(deputes)=="ministre"] <- "isMinister"
  
  clessnverse::logit("getting journalits from CSV", logger)
  journalists <- read.csv(file = paste(base_csv_folder, "journalist_handle.csv", sep = ";"), sep = ";")
  journalists$X <- NULL
  names(journalists)[names(journalists)=="female"] <- "isFemale"
  names(journalists)[names(journalists)=="author"] <- "fullName"
  names(journalists)[names(journalists)=="selfIdJourn"] <- "thinkIsJournalist"
  names(journalists)[names(journalists)=="handle"] <- "twitterHandle"
  names(journalists)[names(journalists)=="realID"] <- "twittweJobTitle"
  names(journalists)[names(journalists)=="user_id"] <- "twitterID"
  names(journalists)[names(journalists)=="protected"] <- "twitterAccountProtected"
  
} #if (opt$backend_type == "CSV")

# We only do this if we want to rebuild those datasets from scratch to start fresh
# or if then don't exist in the environment of the current R session

if ( !exists("dfCache") || is.null(dfCache) || opt$cache_update == "rebuild" ) {
  clessnverse::logit("creating cache either because it doesn't exist or because its rebuild option", logger)
  dfCache <- clessnverse::createCache(context = "quebec")
}

if ( !exists("dfSimple") || is.null(dfSimple) || opt$simple_update == "rebuild" ) {
  clessnverse::logit("creating Simple either because it doesn't exist or because its rebuild option", logger)
  dfSimple <- clessnverse::createSimple(context = "quebec")
}

if ( !exists("dfDeep") || is.null(dfDeep) || opt$deep_update == "rebuild" ) {
  clessnverse::logit("creating Deep either because it doesn't exist or because its rebuild option", logger)
  dfDeep <- clessnverse::createDeep(context = "quebec")
}


###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it o extract the
# press conference content
#
for (i in 1:length(list.urls)) {
  clessnhub::refresh_token(configuration$token, configuration$url)
  current.url <- paste(base.url,list.urls[i],sep="")
  current.id <- str_replace_all(list.urls[i], "[[:punct:]]", "")
  
  clessnverse::logit(paste("Debate", i, "of", length(list.urls),sep = " "), logger)
  
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
      clessnverse::logit(paste(current.id, "not cached"), logger)
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
      clessnverse::logit(paste(current.id, "cached"), logger)
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
               if (is.na(ln2)) {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender.femme)
                } else {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender.femme)
                }
              } else {
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
                  # we have a politician
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
              if ( periode.de.questions || substr(doc.text[j-1], nchar(doc.text[j-1]), nchar(doc.text[j-1])) == "?" ) 
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
          
          # Update Deep
          row_to_commit <- data.frame(uuid = "",
                                      created = "",
                                      modified = "",
                                      metadata = "",
                                      eventID = current.id,
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
          
          dfDeep <- clessnverse::commitDeepRows(row_to_commit, dfDeep, 'agoraplus_warehouse_intervention_items', opt$deep_update, opt$hub_update)
  
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
      
      # Update the cache
      row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = current.id, eventHtml = doc.html, stringsAsFactors = FALSE)
      dfCache <- clessnverse::commitCacheRows(row_to_commit, dfCache, 'agoraplus_warehouse_cache_items', opt$cache_update, opt$hub_update)
      
      # Update Simple
      row_to_commit <- data.frame(uuid = "",
                                  created = "",
                                  modified = "",
                                  metadata = "",
                                  eventID = current.id,
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
                                  stringsAsFactors = FALSE)
      
      dfSimple <- clessnverse::commitSimpleRows(row_to_commit, dfSimple, 'agoraplus_warehouse_event_items', opt$simple_update, opt$hub_update)
      
    } # version finale
    
  } #if (grepl("actualites-salle-presse", current.url))
  
} #for (i in 1:nrow(result))




if (opt$csv_update != "skip" && opt$backend_type == "CSV") { 
  write.csv2(dfCache, file=paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
  write.csv2(dfDeep, file = paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
  write.csv2(dfSimple, file = paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
}

clessnverse::logit(paste("reaching end of", scriptname, "script"), logger = logger)
clessnverse::logclose(logger)