###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                            agora-plus-youtube                               #
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
                         "devtools",
                         "countrycode",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r",
                         "ropensci/gender",
                         "lmullen/genderdata",
                         "karthik/rdrop2")
  
  # Install missing packages
  new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]

  for (p in 1:length(new_packages)) {
    if ( grepl("\\/", new_packages[p]) ) {
      devtools::install_github(new_packages[p], upgrade = "never", build = FALSE)
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
patterns.titres <- c("M\\.", "Mme", "Modérateur", "Modératrice", "Le Modérateur", "La Modératrice",
                     "journaliste :", "Le Président", "La Présidente", "La Vice-Présidente",
                     "Le Vice-Président", "Titre :")

patterns.periode.de.questions <- c("période de questions", "période des questions",
                                   "prendre les questions", "prendre vos questions",
                                   "est-ce qu'il y a des questions", "passer aux questions")

###############################################################################
#   Data source
#
dataRootFolder <- "clessn-blend/_SharedFolder_clessn-blend"
dataInputFolder <- paste(dataRootFolder, "/to_hub/ready", sep="")
dataOutputFolder <- paste(dataRootFolder, "/to_hub/done", sep = "")
#fileList <- list.files(dataInputFolder)

#For dropbox API
token <- readRDS("token.rds")
fileList <- rdrop2::drop_dir(dataInputFolder, dtoken = token)
fileList <- fileList$name


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

# Hack here to focus only on one conf only:
#fileList <- as.list("2021-03-02-en-Lv2Q1utCt70.txt")

i=1

for (fileName in fileList) {
  current.id <- str_match(fileName, "^.{14}(.*).txt")[2]
  
  clessnverse::logit(paste("Conf", i, "de", length(fileList), fileName, sep = " "), logger) 
  
  if ( !(current.id %in% dfCache$id) ) {
    # Read and parse HTML from the URL directly
    #doc.youtube <- readLines(paste(dataInputFolder, fileName, sep = "/"))
    clessnverse::logit(paste("reading", paste(dataInputFolder, fileName, sep='/'), "from dropbox"), logger)
    rdrop2::drop_download(paste(dataInputFolder, fileName, sep='/'), overwrite = T, dtoken = token)
    doc.youtube <- readLines(fileName)
    doc.youtube.original <- paste(paste(doc.youtube, "\n\n", sep=""), collapse = ' ')
    doc.youtube <- doc.youtube[doc.youtube!=""]
    cached.html <- FALSE
    current.url <- str_match(doc.youtube[3], "^URL   : (.*)")[2]
    clessnverse::logit(paste(current.id, "not cached"), logger)
  } else{ 
    # Retrieve the XML structure from dfCache and Parse
    doc.youtube.original <- dfCache$html[which(dfCache$eventID==current.id)]
    doc.youtube <- str_split(doc.youtube.original, '\n\n')[[1]]
    doc.youtube <- doc.youtube[doc.youtube!=""]
    cached.html <- TRUE
    current.url <- str_match(doc.youtube[3], "^URL   : (.*)")[2]
    clessnverse::logit(paste(current.id, "cached"), logger)
  }
    
  version.finale <- TRUE

  if ( version.finale && 
       ( ((opt$simple_update == "update" && !(current.id %in% dfSimple$eventID) ||
           opt$simple_update == "refresh" ||
           opt$simple_update == "rebuild") ||
          (opt$deep_update == "update" && !(current.id %in% dfDeep$eventID) ||
           opt$deep_update == "refresh" ||
           opt$deep_update == "rebuild")) ||
         ((opt$hub_update == "refresh" ||
           opt$hub_update == "update") && current.id %in% dfSimple$eventID))
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
      current.source <- "Conférences de Presse YouTube"

      # Extract date of the conference
      date <- str_match(doc.youtube[1], "^DATE  : (.*)")[2]

      # Extract start time of the conference
      time <- NA

      # Figure out the end time of the conference
      end.time <- NA
      
      # Title and subtitle of the conference  
      title <- str_match(doc.youtube[2], "^TITLE : (.*)")[2]
      subtitle <- NA

        
      # Extract all the paragraphs (HTML tag is p, starting at
      # the root of the document). Unlist flattens the list to
      # create a character vector.
      doc.text <- doc.youtube
                            
                            
      # Replace all \n by spaces and clean leading and trailing spaces
      # and clean the conference vector of unneeded paragraphs
      doc.text <- gsub('\\n',' ', doc.text)
      doc.text <- sub("^\\s+", "", doc.text)
      doc.text <- sub("\\s+$", "", doc.text)

      doc.text <- doc.text[doc.text!=""]
      doc.text <- doc.text[5:length(doc.text)]
      
      
      
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
      language <- NA
      speech.type <- NA
      speech <- NA
      
      speaker <- data.frame()
      periode.de.questions <- FALSE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      seqnum <- 1
  
      event.paragraph.count <- length(doc.text)
      event.sentence.count <- clessnverse::countVecSentences(doc.text)
      
      if (length(doc.text) > 0) {
        
        for (j in 1:length(doc.text)) {
          
          cat("Conf:", i, "Paragraph:", j, "Intervention", seqnum, "\n", sep = " ")
          
          # Is this a new speaker taking the stand?  If so there is typically a : at the begining of the sentence
          # And the Sentence starts with the Title (M. Mme etc) and the last name of the speaker
          intervention.start <- substr(doc.text[j],1,40)
          
          if ( (grepl("^M\\.\\s+(.*?):", intervention.start) || 
                grepl("^Mme\\s+(.*?):", intervention.start) || 
                grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?):", intervention.start) ||
                grepl("^Titre(.*?):", intervention.start) ||
                grepl("^Journaliste(.*?):", intervention.start) ||
                grepl("^Modérat(.*?):", intervention.start) ||
                grepl("^Une\\svoix(.*?):", intervention.start) ||
                grepl("^Des\\svoix(.*?):", intervention.start)) &&
               !grepl(",", str_match(intervention.start, "^(.*):")) ) {
            # It's a new person speaking
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
            
            speech.paragraph.count <- 1
            speech.sentence.count <- 0
            speech.word.count <- 0
            
            # let's rule out the president first
            if ( str_detect(intervention.start, "^Modérateur\\s?:\\s+") ) { ### MODERATEUR ###
              is.minister <- 0
              speech.type <- "modération"
              type <- "modérateur"

              if (  j>1 && TRUE %in% str_detect(doc.text[j], patterns.periode.de.questions) )
                periode.de.questions <- TRUE
            } #str_detect(intervention.start, "^Modérateur\\s?:\\s+")

            
            if ( str_detect(intervention.start, "^M\\.(.*)(\\s+)?:\\s+") ||
                 str_detect(intervention.start, "^Mme(.*)(\\s+)?:\\s+") ) {
              # A ce stade on cherche à identifier l'intervenant et savoir
              # si c'est un député ou si c'est un journaliste
              
              # il faut voir si c'est un homme ou une femme
              # et ensuite extirper le prénom et le nom (entre parnethèse)
              
              if ( str_detect(intervention.start, "^M\\.(.*):") ) gender.femme <- 0
              else gender.femme <- 1
              
              last.name <- str_match(intervention.start, "^((M\\.)|(Mme))(.*)?\\s+(.*)\\s+\\((.*)\\)(\\s+)?:")[6]
              first.name <- str_match(intervention.start, "^((M\\.)|(Mme))(.*)?\\s+(.*)\\s+\\((.*)\\)(\\s+)?:")[7]
              
              # on cherche s'il est dans la liste de référence des députés
              speaker <- filter(deputes, (tolower(lastName1) == tolower(last.name) | tolower(lastName2) == tolower(last.name)) & isFemale == gender.femme)
              
              if ( nrow(speaker) > 0 ) { ### DÉPUTÉ ###
                is.minister <- speaker$isMinister[1]
                
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
                
                if ( !is.na(dfDeep$speakerSpeechType[nrow(dfDeep)]) && 
                     (j>1 && dfDeep$speakerSpeechType[nrow(dfDeep)] == "question") ||
                     periode.de.questions
                   )
                  speech.type <- "réponse"
                else
                  speech.type <- "allocution"
                
              } else { ### JOURNALISTE ###
                # On n'a pas trouvé first.name last.name dans députés 
                # on devrait pouvoir le trouver dans les journalistes
                speaker <- filter(journalists, tolower(paste(first.name, last.name, sep = " ")) == tolower(fullName))
                
                if ( nrow(speaker) > 0 ) {
                  # we have a JOURNALIST
                  
                  gender <- if ( speaker[1,]$isFemale ) "F" else "M"
                  type <- "journaliste"
                  party <- NA
                  circ <- NA
                  media <- speaker[1,]$source
                } 
              } # if (nrow(speaker) > 0)
            }  # str_detect(intervention.start, "^M\\.(.*):\\s+") || str_detect(intervention.start, "^Mme(.*):\\s+")

            
            if ( str_detect(intervention.start, "^Journaliste(\\s+)?:\\s+") ) { ### Journaliste ###
              is.minister <- NA
              type <- "journaliste"
            } #str_detect(intervention.start, "^Journaliste(\\s+)?:\\s+")
            
            if ( (!is.na(type) && periode.de.questions && type == "journaliste") ||
                 (is.na(type) && periode.de.questions)
                 ) 
              speech.type <- "question"

            speech <- substr(doc.text[j], unlist(gregexpr(":", intervention.start))+1, nchar(doc.text[j]))
            speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
    
          } else {
            # It's the same person as in the previous paragraph speaking
            # We will append it to the same row instead of creating an extra row for a new paragraph
            speech <- paste(speech,"\n\n",doc.text[j], sep="")
            speech.paragraph.count <- speech.paragraph.count + 1
          }
          
          if ( !(language %in% c("en","fr")) ) {
            language <- textcat(str_replace_all(speech, "[[:punct:]]", ""))
            if ( !(language %in% c("english","french")) ) { 
              language <- NA
            }
            else language <- substr(language,1,2)
          }
          
          speech.sentence.count <- clessnverse::countSentences(paste(speech, collapse = ' '))
          speech.word.count <- length(words(removePunctuation(paste(speech, collapse = ' '))))

          if (is.na(first.name) && is.na(last.name)) 
            full.name <- NA
          else 
            full.name <- trimws(paste(na.omit(first.name), na.omit(last.name), sep = " "),which = "both")
          
          # If the next speaker is different or if it's the last record, then let's commit this observation into the dataset
          
          if ( (grepl("^M\\.\\s+(.*?):", substr(doc.text[j+1],1,40)) || 
                grepl("^Mme\\s+(.*?):", substr(doc.text[j+1],1,40)) || 
                grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?):", substr(doc.text[j+1],1,40)) ||
                grepl("^Titre(.*?):", substr(doc.text[j+1],1,40)) ||
                grepl("^Journaliste(.*?):", substr(doc.text[j+1],1,40)) ||
                grepl("^Modérat(.*?):", substr(doc.text[j+1],1,40)) ||
                grepl("^Une\\svoix(.*?):", substr(doc.text[j+1],1,40)) ||
                grepl("^Des\\svoix(.*?):", substr(doc.text[j+1],1,40))) &&
               !grepl(",", str_match(substr(doc.text[j+1],1,40), "^(.*):")) &&
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
        } # for (j in 1:length(doc.text))
      

      # Join all the elements of the character vector into a single
      # character string, separated by spaces for the simple dataSet
      collapsed.doc.text <- paste(paste(doc.text, "\n\n", sep=""), collapse = ' ')
      
      # Update the cache
      row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = current.id, eventHtml = doc.youtube.original, stringsAsFactors = FALSE)
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
      
    } # if (length(doc.text) > 0)
      
  } # version finale
  i <- i + 1
  file.remove(fileName)
  rdrop2::drop_move(paste(dataInputFolder, fileName, sep='/'), paste(dataOutputFolder, fileName, sep='/'), dtoken = token)
  #file.rename(paste(dataInputFolder, fileName, sep = "/"), paste(dataOutputFolder, fileName, sep = "/"))
} #for (fileName in fileList)



if (opt$csv_update != "skip" && opt$backend_type == "CSV") { 
  write.csv2(dfCache, file=paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
  write.csv2(dfDeep, file = paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
  write.csv2(dfSimple, file = paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
}

clessnverse::logit(paste("reaching end of", scriptname, "script"), logger = logger)
clessnverse::logclose(logger)

