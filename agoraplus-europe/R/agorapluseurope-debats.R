###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                             agora-plus-europe                               #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################


###############################################################################
### Function : installPackages
### This function installs all packages requires in this script and all the
### scripts called by this one
###

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
#
#   .ChildEnv : 
#   .ChildEnv$scriptname
#   .ChildEnv$logger
#    

if (!exists(".ChildEnv")) {
  .ChildEnv <- new.env()
  .ChildEnv$scriptname <- "agora-plus-confpresse-v2.R"
  .ChildEnv$logger <- loginit(.ChildEnv$scriptname)
}



###############################################################################
########################               MAIN              ######################
###############################################################################



# Install the rquired packages
installPackages()

# Parse the command line options
# Which are the update modes of each database in the HUB or in the CSV backend
#
# Possible values : update, refresh, rebuild or skip
# update : updates the dataset by adding only new observations to it
# refresh : refreshes existing observations and adds new observations to the dataset
# rebuild : wipes out completely the dataset and rebuilds it from scratch
# skip : does not make any change to the dataset
option_list = list(
  make_option(c("-c", "--cache_update"), type="character", default="skip", 
              help="update mode of the cache [default= %default]", metavar="character"),
  make_option(c("-s", "--simple_update"), type="character", default="rebuild", 
              help="update mode of the simple dataframe [default= %default]", metavar="character"),
  make_option(c("-d", "--deep_update"), type="character", default="rebuild", 
              help="update mode of the deep dataframe [default= %default]", metavar="character"),
  make_option(c("-h", "--hub_update"), type="character", default="skip", 
              help="update mode of the hub [default= %default]", metavar="character"),
  make_option(c("-f", "--csv_update"), type="character", default="skip", 
              help="update mode of the simple dataframe [default= %default]", metavar="character"),
  make_option(c("-b", "--backend_type"), type="character", default="HUB", 
              help="type of the backend - either hub or csv [default= %default]", metavar="character")
)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

opt_cache_update <- opt$cache_update
opt_simple_update <- opt$simple_update
opt_deep_update <- opt$deep_update
opt_hub_update <- opt$hub_update
opt_csv_update <- opt$csv_update

# set which backend we're working with
# - CSV : work with the CSV in the shared folders - good for testing
#         or for datamining and research or messing around
# - HUB : work with the CLESSNHUB data directly : this is prod data
opt_backend_type <- opt$backend_type


# Define the datasets containing
# - the cache which contains the previously scraped html content
# - dfSimple which contains the entire content of each press conference per observation
# - dfDeep which contains each intervention of each press conference per observation
#
# We only do this if we want to rebuild those datasets from scratch to start fresh
# or if then don't exist in the environment of the current R session
if ( !exists("dfCache") || opt_cache_update == "rebuild" ) 
  dfCache <- clessnverse::createCache(context = "europe")

if ( !exists("dfSimple") || opt_simple_update == "rebuild" ) 
  dfSimple <- clessnverse::createSimple(context = "europe")

if ( !exists("dfDeep") || opt_deep_update == "rebuild" ) 
  dfDeep <- clessnverse::createDeep(context = "europe")


stop("done for now before we go further")

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
  if (opt_cache_update != "rebuild") {
    dfCache_hub <- clessnhub::download_table('agoraplus_warehouse_cache_items')
    if (is.null(dfCache_hub)) {
      dfCache_hub <- data.frame(uuid = character(),
                                created = character(),
                                modified = character(),
                                metedata = character(),
                                eventID = character(),
                                eventHtml = character(),
                                stringsAsFactors = FALSE)
    } 
    
    #dfCache <- dfCache_hub[,-c(1:4)]
    dfCache <- dfCache_hub
  }

  
  
  
  ###
  # Récuperer les données Simple et Deep 
  ###
  if (opt_simple_update != "rebuild") {
    dfSimple_hub <- clessnhub::download_table('agoraplus_warehouse_event_items')
    if (is.null(dfSimple_hub)) {
      dfSimple_hub <- data.frame(uuid = character(),
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
    
    #dfSimple <- dfSimple_hub[,-c(1:4)]
    dfSimple <- dfSimple_hub
  }

  if (opt_deep_update != "rebuild") {
    dfDeep_hub <- clessnhub::download_table('agoraplus_warehouse_intervention_items')
    if (is.null(dfDeep_hub)) {
      dfDeep_hub <- data.frame(uuid = character(),
                               created = character(),
                               modified = character(),
                               metedata = character(),
                               eventID = character(),
                               chapterNumber = character(),
                               chapterTitle = character(),
                               chapterTabledDocId = character(),
                               chapterAdoptedDocId = character(),
                               interventionSeqNum = integer(),
                               speakerFirstName = character(),
                               speakerLastName = character(),
                               speakerFullName = character(),
                               speakerGender = character(),
                               speakerIsMinister = character(),
                               speakerType = character(),
                               speakerCountry = character(),
                               speakerParty = character(),
                               speakerPolGroup = character(),
                               speakerDistrict = character(),
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
    
    #dfDeep <- dfDeep_hub[,-c(1:4)]
    dfDeep <- dfDeep_hub
  }
  
  dfDeputes <- clessnhub::download_table('warehouse_quebec_mnas')
  dfDeputes <- dfDeputes %>% separate(lastName, c("lastName1", "lastName2"), " ")
  
  dfJournalists <- clessnhub::download_table('warehouse_journalists')

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
  

  dfDeputes <- read.csv(
    "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/Deputes_Quebec_Coordonnees.csv",
    sep=";")
  dfDeputes <- dfDeputes %>% separate(nom, c("firstName", "lastName1", "lastName2"), " ")
  names(dfDeputes)[names(dfDeputes)=="femme"] <- "isFemale"
  names(dfDeputes)[names(dfDeputes)=="parti"] <- "party"
  names(dfDeputes)[names(dfDeputes)=="circonscription"] <- "currentDistrict"
  names(dfDeputes)[names(dfDeputes)=="ministre"] <- "isMinister"
  
  dfJournalists <- read.csv(
    "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/journalist_handle.csv",
    sep=";")
  dfJournalists$X <- NULL
  names(dfJournalists)[names(dfJournalists)=="female"] <- "isFemale"
  names(dfJournalists)[names(dfJournalists)=="author"] <- "fullName"
  names(dfJournalists)[names(dfJournalists)=="selfIdJourn"] <- "thinkIsJournalist"
  names(dfJournalists)[names(dfJournalists)=="handle"] <- "twitterHandle"
  names(dfJournalists)[names(dfJournalists)=="realID"] <- "twittweJobTitle"
  names(dfJournalists)[names(dfJournalists)=="user_id"] <- "twitterID"
  names(dfJournalists)[names(dfJournalists)=="protected"] <- "twitterAccountProtected"

} #if (opt_backend_type == "CSV")


#####
##### Create some reference vectors used for dates conversion or detecting 
##### patterns in the conferences
#####
months_fr <- c("janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre",
               "octobre", "novembre", "décembre")
months_en <- c("january", "february", "march", "april", "may", "june", "july", "august", "september",
               "october", "november", "december")

patterns_titres <- c("M\\.", "Mme", "Modérateur", "Modératrice", "Le Modérateur", "La Modératrice",
                     "journaliste :", "Le Président", "La Présidente", "La Vice-Présidente",
                     "Le Vice-Président", "Titre :")

patterns_periode_de_questions <- c("période de questions", "période des questions",
                                   "prendre les questions", "prendre vos questions",
                                   "est-ce qu'il y a des questions", "passer aux questions")


###############################################################################
##### Get some data to start the fun!
#####

###
### connect to the dataSource : the provincial parliament web site 
### get the index page containing the URLs to all the press conference
### to extract those URLS and get them individually in order to parse
### each press conference
###
base_url <- "https://www.europarl.europa.eu"
content_url <- "/plenary/fr/debates-video.html#sidesForm"

source_page <- xml2::read_html(paste(base_url,content_url,sep=""))
source_page_html <- htmlParse(source_page, useInternalNodes = TRUE)

urls <- rvest::html_nodes(source_page, 'a')
urls <- urls[grep("\\.xml", urls)]
urls <- urls[grep("https", urls)]

urls_list <- rvest::html_attr(urls, 'href')


###############################################################################
##### Let's get serious!!!
##### Run through the URLs list, get the html content from the cache if it is 
##### in it, or from the assnat website and start parsing it o extract the
##### press conference content
#####
#urls_list <- list("https://www.europarl.europa.eu/doceo/document/CRE-9-2021-01-18_FR.xml")
urls_list <- list("https://www.europarl.europa.eu/doceo/document/CRE-9-2020-02-12_FR.xml")

pb_conf <- txtProgressBar(min = 0,      # Minimum value of the progress bar
                          max = length(urls_list), # Maximum value of the progress bar
                          style = 3,    # Progress bar style (also available style = 1 and style = 2)
                          width = length(urls_list),   # Progress bar width. Defaults to getOption("width")
                          char = "=")   # Character used to create the bar


#for (i in 1:length(urls_list)) {
for (i in 1:1) {
  if (opt_backend_type == "HUB") clessnhub::refresh_token(configuration$token, configuration$url)
  current_url <- urls_list[[i]]
  current_id <- str_replace_all(urls_list[i], "[[:punct:]]", "")
  
  cat(" conférence progress: \r")
  setTxtProgressBar(pb_conf, i)
  

  ###
  # If the data is not cache we get the raw html from assnat.qc.ca
  # if it is cached (we scarped it before), we prefer not to bombard
  # the website with HTTP_GET requests and ise the cached version
  ###
  if ( !(current_id %in% dfCache$eventID) ) {
    # Read and parse HTML from the URL directly
    doc_html <- getURL(current_url)
    doc_xml <- xmlTreeParse(doc_html, useInternalNodes = TRUE)
    top_xml <- xmlRoot(doc_xml)
    head_xml <- top_xml[[1]]
    core_xml <- top_xml[[2]]
    cached_html <- FALSE
  } else{ 
    # Retrieve the XML structure from dfCache and Parse
    doc_html <- dfCache$eventHtml[which(dfCache$eventID==current_id)]
    doc_xml <- xmlTreeParse(doc_html, useInternalNodes = TRUE)
    top_xml <- xmlRoot(doc_xml)
    head_xml <- top_xml[[1]]
    core_xml <- top_xml[[2]]
    cached_html <- TRUE
  }
    
  # Get the length of all branches of the XML document
  core_xml_nbchapters <- length(names(core_xml))

    
  ###############################
  # Columns of the simple dataset
  event_source_type <- xpathApply(source_page_html, '//title', xmlValue)[[1]]
  event_url <- current_url
  event_date <- substr(xmlGetAttr(core_xml[[2]][["NUMERO"]], "VOD-START"),1,10)
  event_start_time <- substr(xmlGetAttr(core_xml[[2]][["NUMERO"]], "VOD-START"),12,19)
  event_end_time <- substr(xmlGetAttr(core_xml[[core_xml_nbchapters]][["NUMERO"]], "VOD-END"),12,19)
  event_title <- NA
  event_subtitle <- NA
  event_word_count <- NA
  event_sentence_count <- NA
  event_paragraph_count <- NA
  event_content <- NA
  event_translated_content <- NA
  event_doc_text <- NA
  
  
  ####################################
  # The colums of the detailed dataset
  event_chapter <- NA
  intervention_seqnum <- NA
  speaker_first_name <- NA
  speaker_last_name <- NA
  speaker_full_name <- NA
  speaker_gender <- NA
  speaker_is_minister <- NA
  speaker_type <- NA
  speaker_party <- NA
  speaker_polgroup <- NA
  speaker_district <- NA
  speaker_country <- NA
  speaker_media <- NA
  speaker_speech_type <- NA
  speaker_speech_lang <- NA
  speaker_speech_word_count <- NA
  speaker_speech_sentence_count <- NA
  speaker_speech_paragraph_count <- NA
  speaker_speech <- NA
  speaker_translated_speech <- NA
  
  speaker <- data.frame()

  ########################################################
  # Go through the xml document chapter by chapter
  # and strip out any relevant info
  intervention_seqnum <- 0
  
  pb_chap <- txtProgressBar(min = 0,      # Minimum value of the progress bar
                          max = core_xml_nbchapters, # Maximum value of the progress bar
                          style = 3,    # Progress bar style (also available style = 1 and style = 2)
                          width = core_xml_nbchapters,   # Progress bar width. Defaults to getOption("width")
                          char = "=")   # Character used to create the bar
  
  event_content <- ""
  event_translated_content <- ""
  
  current_speaker_full_name <- ""
  
  for (j in 1:core_xml_nbchapters) {
    cat(" chapter progress\r")
    setTxtProgressBar(pb_chap, j)
    # New chapter
    
    chapter_node <- core_xml[[j]]
    chapter_number <- xmlGetAttr(chapter_node, "NUMBER")
    chapter_title <- xmlValue(xpathApply(chapter_node, "//CHAPTER/TL-CHAP[@VL='FR']")[j])
    
    if (!is.null(chapter_node[["TL-CHAP"]][[2]])) {
      # Here there is a URL within the title of the chapter => most likely a linked document 
      chapter_tabled_docid <- str_split(xmlGetAttr(chapter_node[["TL-CHAP"]][[2]], "redmap-uri"), "/")[[1]][3]
      
      tabled_document_url <- paste("https://www.europarl.europa.eu/doceo/document/", chapter_tabled_docid, "_FR.html", sep = "")
      tabled_document_html <- getURL(tabled_document_url)
      tabled_document_html_table <- readHTMLTable(tabled_document_html)
      
      list1_id <- grep("Textes adoptés", tabled_document_html_table)[1]
      list2_id <- grep("Textes adoptés", tabled_document_html_table[[list1_id]])[2]
      chapter_adopted_docid <- tabled_document_html_table[[list1_id]][[list2_id]][which(grepl("Textes adoptés",tabled_document_html_table[[list1_id]][[list2_id]]))]
      chapter_adopted_docid <- str_split(chapter_adopted_docid, " ")[[1]][length(str_split(chapter_adopted_docid, " ")[[1]])]
      
    } else {
      chapter_tabled_docid <- NA
      chapter_adopted_docid <- NA
    }
      
    chapter_nodes_list <- names(core_xml[[j]])
    
    if ( "INTERVENTION" %in% chapter_nodes_list ) {
      # There is one or more interventions in this section.
      # From potentially multiple speakers
      
      # We have to loop through every intervention
      for (k in which(chapter_nodes_list == "INTERVENTION")) {
        intervention_seqnum <- intervention_seqnum + 1
        speaker_speech <- ""
        #cat("Intervention", intervention_seqnum, "\r", sep = " ")
        
        # Strip out the speaker info
        intervention_node <- core_xml[[j]][[k]]
        speaker_node <- intervention_node[["ORATEUR"]]
        
        speaker_full_name <- xmlGetAttr(speaker_node, "LIB")
        speaker_last_name <- trimws(str_split(speaker_full_name, "\\|")[[1]][[2]], "both")
        speaker_first_name <- trimws(str_split(speaker_full_name, "\\|")[[1]][[1]], "both")
        speaker_full_name <- str_remove(speaker_full_name, "\\|\\s")
        
        dfSpeaker <- clessnverse::getEuropeMepData(speaker_full_name)
        
        if (!is.na(dfSpeaker$mepid)) {
          speaker_mepid <- dfSpeaker$mepid
          speaker_party <- dfSpeaker$party
          speaker_polgroup <- case_when(xmlGetAttr(speaker_node, "PP") != "NULL" ~ 
                                          paste(xmlGetAttr(speaker_node, "PP"), ":", dfSpeaker$polgroup, sep = " "),
                                        TRUE ~ 
                                          NA_character_)
          speaker_country <- dfSpeaker$country
        } else {
          speaker_mepid <- NA
          speaker_party <- case_when(xmlGetAttr(speaker_node, "PP") != "NULL" ~  xmlGetAttr(speaker_node, "PP"),
                                     TRUE ~ NA_character_)
          speaker_polgroup <- NA
          speaker_country <- codelist$country.name.en[which(codelist$iso2c == xmlGetAttr(speaker_node, "LG"))]
          if (length(speaker_country) == 0) speaker_country <- NA
        }
        
        speaker_gender <- paste("", gender::gender(word(speaker_first_name)[1])$gender, sep = "")
        speaker_is_minister <- NA
        
        speaker_type <- trimws(gsub("\\.|\\,|\\s\\–", "", xmlValue(speaker_node[["EMPHAS"]])))
        if ( str_detect(tolower(speaker_type), tolower(speaker_full_name)) ) speaker_type <- NA
        
        speaker_district <- NA
        speaker_media <- NA    
        
        speaker_speech_type <- NA
        speaker_speech_lang <- xmlValue(speaker_node[["LG"]])
        speaker_speech_word_count <- 0
        speaker_speech_sentence_count <- 0
        speaker_speech_paragraph_count <- 0
        speaker_speech <- ""
        speaker_translated_speech <- ""
        
        # Strip out the intervention by looping through paragraphs
        for (l in which(names(intervention_node) == "PARA")) {
          #cat("Paragraph", l, "\n", sep = " ")
          if (is.na(speaker_type)) speaker_type <- xmlValue(intervention_node[["PARA"]][["EMPHAS"]])
          speaker_speech <- paste(speaker_speech, xmlValue(intervention_node[[l]]), sep = " ")
        } #for (l in which(names(intervention_node) == "PARA"))
        
        if (str_detect(speaker_speech, ". – ")) {
          speaker_speech <- str_split(speaker_speech, ". – ")[[1]][2]
        }
        
        speaker_speech <- trimws(speaker_speech, "left")
        speaker_speech_word_count <- nrow(unnest_tokens(tibble(txt=speaker_speech), word, txt, token="words",format="text"))
        speaker_speech_sentence_count <- nrow(unnest_tokens(tibble(txt=speaker_speech), sentence, txt, token="sentences",format="text"))
        speaker_speech_paragraph_count <- length(which(names(intervention_node) == "PARA"))
        
        # Translation
        speaker_translated_speech <- clessnverse::translateText(text = speaker_speech, engine = "azure", 
                                                                target_lang = "fr", fake = TRUE)
        
        # commit to dfDeep and the Hub
        dfInterventionRow <- data.frame(
          uuid = "",
          created = "",
          modified = "",
          metedata = "",
          eventID = current_id,
          chapterNumber = chapter_number,
          chapterTitle = chapter_title,
          chapterTabledDocId = chapter_tabled_docid,
          chapterAdoptedDocId = chapter_adopted_docid,
          interventionSeqNum = intervention_seqnum,
          speakerFirstName = speaker_first_name,
          speakerLastName = speaker_last_name,
          speakerFullName = speaker_full_name,
          speakerGender = speaker_gender,
          speakerIsMinister = speaker_is_minister,
          speakerType = speaker_type,
          speakerCountry = speaker_country,
          speakerParty = speaker_party,
          speakerPolGroup = speaker_polgroup,
          speakerDistrict = speaker_district,
          speakerMedia = speaker_media,
          speakerSpeechType = speaker_speech_type,
          speakerSpeechLang = speaker_speech_lang,
          speakerSpeechWordCount = speaker_speech_word_count,
          speakerSpeechSentenceCount = speaker_speech_sentence_count,
          speakerSpeechParagraphCount = speaker_speech_paragraph_count,
          speakerSpeech = speaker_speech,
          speakerTranslatedSpeech = speaker_translated_speech)
        
        dfDeep <- clessnverse::commitDeepRows(dfSource = dfInterventionRow, 
                                                          dfDestination = dfDeep,
                                                          hubTableName = 'agoraplus-eu_warehouse_intervention_items', 
                                                          modeLocalData = opt_deep_update, 
                                                          modeHub = opt_hub_update)
        
        event_content <- paste(event_content, 
                               case_when(speaker_full_name == current_speaker_full_name ~ paste(speaker_speech, "\n\n", sep=""),
                                         TRUE ~ paste(speaker_full_name, 
                                                      " (", 
                                                      case_when(is.na(speaker_polgroup) ~ speaker_type, TRUE ~  xmlGetAttr(speaker_node, "PP")),
                                                      "). – ", speaker_speech, "\n\n", sep = "")), sep = "")
        
        event_translated_content <- paste(event_translated_content, 
                                          case_when(speaker_full_name == current_speaker_full_name ~ paste(speaker_translated_speech, "\n\n", sep=""),
                                                    TRUE ~ paste(speaker_full_name, 
                                                                 " (", 
                                                                 case_when(is.na(speaker_polgroup) ~ speaker_type, TRUE ~  xmlGetAttr(speaker_node, "PP")),
                                                                 "). – ", speaker_translated_speech, "\n\n", sep = "")), sep = "")
        
        current_speaker_full_name <- speaker_full_name
  
      } #for (k in which(chapter_nodes_list == "INTERVENTION"))
      
    } #if ( "INTERVENTION" %in% chapter_nodes_list )
  
  } #for (j in 1:core_xml_nbchapters)
  
  event_word_count <- sum(dfDeep$speakerSpeechWordCount[which(dfDeep$eventID == current_id)])
  event_sentence_count <- sum(dfDeep$speakerSpeechSentenceCount[which(dfDeep$eventID == current_id)])
  event_paragraph_count <- sum(dfDeep$speakerSpeechParagraphCount[which(dfDeep$eventID == current_id)])
  
  dfEventRow <- data.frame(uuid = "",
                           created = "",
                           modified = "",
                           metedata = "",
                           eventID = current_id,
                           eventSourceType = event_source_type,
                           eventURL = current_url,
                           eventDate = as.character(event_date),
                           eventStartTime = as.character(event_start_time),
                           eventEndTime = as.character(event_end_time),
                           eventTitle = event_title,
                           eventSubtitle = event_subtitle,
                           eventWordCount = event_word_count,
                           eventSentenceCount = event_sentence_count,
                           eventParagraphCount = event_paragraph_count,
                           eventContent = event_content,
                           eventTranslatedContent = event_translated_content)
  
  dfSimple <- clessnverse::commitSimpleRows(dfSource = dfEventRow, 
                                             dfDestination = dfSimple,
                                             hubTableName = 'agoraplus-eu_warehouse_event_items', 
                                             modeLocalData = opt_simple_update, 
                                             modeHub = opt_hub_update)
  
  
  dfCache <- clessnverse::commitCacheRows(dfSource = data.frame(eventID = current_id, eventHtml = doc_html),
                                            dfDestination = dfCache,
                                            hubTableName = 'agoraplus-eu_warehouse_cache_items', 
                                            modeLocalData = opt_cache_update, 
                                            modeHub = opt_hub_update)
  
} #for (i in 1:length(urls_list))



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
