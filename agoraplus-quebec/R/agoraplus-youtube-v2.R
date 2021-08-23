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
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r")
  
  # Install missing packages
  new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]

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

if (!exists("scriptname")) scriptname <- "agoraplus-youtube-v2.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, "file", Sys.getenv("LOG_PATH"))

# Script command line options:
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataframe by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataframe
# - rebuild : wipes out completely the dataframe and rebuilds it from scratch
# - skip : does not make any change to the dataframe
opt <- list(cache_mode = "rebuild", simple_mode = "rebuild", deep_mode = "rebuild", 
            dataframe_mode = "update", hub_mode = "update", download_data = FALSE)

if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

# Download HUB v1 data 
#clessnverse::loadAgoraplusHUBDatasets("quebec", opt, 
#                                      Sys.getenv('HUB_USERNAME'), 
#                                      Sys.getenv('HUB_PASSWORD'), 
#                                      Sys.getenv('HUB_URL'))


# Download HUB v2 data
if (opt$dataframe_mode %in% c("update","refresh")) {
  clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
  dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "press_conference", schema = "v2", 
                                                               location = "CA-QC",
                                                               download_data = opt$download_data,
                                                               token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfInterventions)) dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type = "press_conference", schema = "v2", location = "CA-QC")
  
  if (opt$download_data) { 
    dfInterventions <- dfInterventions[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location",
                                          "data.eventID", "data.eventDate", "data.eventStartTime", "data.eventEndTime", 
                                          "data.eventTitle", "data.eventSubTitle", "data.interventionSeqNum", "data.objectOfBusinessID", 
                                          "data.objectOfBusinessRubric", "data.objectOfBusinessTitle", "data.objectOfBusinessSeqNum", 
                                          "data.subjectOfBusinessID", "data.subjectOfBusinessTitle", "data.subjectOfBusinessHeader", 
                                          "data.subjectOfBusinessSeqNum", "data.subjectOfBusinessProceduralText", 
                                          "data.subjectOfBusinessTabledDocID", "data.subjectOfBusinessTabledDocTitle", 
                                          "data.subjectOfBusinessAdoptedDocID", "data.subjectOfBusinessAdoptedDocTitle", 
                                          "data.speakerID", "data.speakerFirstName", "data.speakerLastName", "data.speakerFullName", 
                                          "data.speakerGender", "data.speakerType", "data.speakerCountry", "data.speakerIsMinister", 
                                          "data.speakerParty", "data.speakerPolGroup", "data.speakerDistrict", "data.speakerMedia", 
                                          "data.interventionID", "data.interventionDocID", "data.interventionDocTitle", 
                                          "data.interventionType", "data.interventionLang", "data.interventionWordCount", 
                                          "data.interventionSentenceCount", "data.interventionParagraphCount", "data.interventionText", 
                                          "data.interventionTextFR", "data.interventionTextEN")]
  } else {
    dfInterventions <- dfInterventions[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location")]
  }
  
} else {
  clessnverse::logit(scriptname, "Not retreiving interventions from hub because hub_mode is rebuild or skip", logger)
  dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type="press_conference", schema = "v2", location = "CA-QC")
}

# Download v2 Cache
if (opt$dataframe_mode %in% c("update","refresh")) {
  dfCache2 <- clessnverse::loadAgoraplusCacheDf(type = "press_conference", schema = "v2",
                                                location = "CA-QC",
                                                download_data = FALSE,
                                                token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfCache2)) dfCache2 <- clessnverse::createAgoraplusCacheDf(type = "parliament_debate", schema = "v2", location = "CA-QC")
  
  if (opt$download_data) { 
    dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location", 
                            "data.eventID", "data.rawContent")]
  } else {
    dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location")]
  }
  
} else {
  clessnverse::logit(scriptname, "Not retreiving cache from hub because hub_mode is rebuild or skip", logger)
  dfCache2 <- clessnverse::createAgoraplusCacheDf(type="press_conference", schema = "v2", location = "CA-QC")
}

# Download v2 MPs information
dfMPs <-  clessnverse::loadAgoraplusPersonsDf(type = "mp", schema = "v2",
                                              location = "CA-QC",
                                              download_data = TRUE,
                                              token = Sys.getenv('HUB_TOKEN'))

# Download v2 public service personnalities information
dfMPs <- dfMPs %>% 
  rbind(clessnverse::loadAgoraplusPersonsDf(type = "public_service", schema = "v2",
                                            location = "CA-QC",
                                            download_data = TRUE,
                                            token = Sys.getenv('HUB_TOKEN')
  )
  )

# Download v2 journalists information
dfJournalists <- clessnverse::loadAgoraplusPersonsDf(type = "journalist", schema = "v2",
                                                     location = "CA",
                                                     download_data = TRUE,
                                                     token = Sys.getenv('HUB_TOKEN')
)

dfMPs <- dfMPs %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ", extra = "merge")
dfJournalists <- dfJournalists %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ", extra = "merge")



# Load all objects used for ETL including V1 HUB MPs
clessnverse::loadETLRefData(username = Sys.getenv('HUB_USERNAME'), 
                            password = Sys.getenv('HUB_PASSWORD'), 
                            url = Sys.getenv('HUB_URL'))


###############################################################################
# Data source
#
# connect to the dataSource : the files deposited by the python youtube 
# transcriptor, in dropbox in clessn-blend/_SharedFolder_clessn-blend/to_hub_ready
# Each file must have been cleaned and formatted manually  before beind put
# in that folder and will be parsefd individually and put in the HUB agora tables
#
data_root_folder <- "clessn-blend/_SharedFolder_clessn-blend"
data_input_folder <- paste(data_root_folder, "/to_hub/ready", sep="")
data_output_folder <- paste(data_root_folder, "/to_hub/done", sep = "")
#filelist <- list.files(data_input_folder)

#For dropbox API
clessnverse::logit(scriptname, "reading drop box token", logger)
token <- Sys.getenv("DROPBOX_TOKEN")
filelist_df <- clessnverse::dbxListDir(dir=paste("/",data_input_folder, sep=""), token=token)


if (nrow(filelist_df) == 0) {
  clessnverse::logit(scriptname, paste("no file to process in ",data_root_folder,'/',data_input_folder,sep=''), logger)
  logger <- clessnverse::logclose(logger)
  #stop("This not an error - this is normal behaviour - program stopped because no youtube transcription to process in to_hub/ready folder in dropbox", call. = FALSE)
  invokeRestart("abort")
} else {
  filelist <- filelist_df$objectName
}






###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it o extract the
# press conference content
#

# Hack here to focus only on one conf only:
#filelist <- as.list("2021-03-02-en-Lv2Q1utCt70.txt")

i=1

for (filename in filelist) {
  if (opt$hub_mode != "skip") clessnhub::refresh_token(configuration$token, configuration$url)
  current_id <- stringr::str_match(filename, "^.{10}(.*).txt")[2]
  
  clessnverse::logit(scriptname, paste("Conf", i, "de", length(filelist), filename, sep = " "), logger) 
  cat("\nConf", i, "de", length(filelist), filename, "\n", sep = " ")
  
  if ( !(current_id %in% dfCache2$key) ) {
    # Read and parse HTML from the URL directly
    clessnverse::logit(scriptname, paste("downloading", paste(data_input_folder, filename, sep='/'), "from dropbox"), logger)
    clessnverse::dbxDownloadFile(paste('/', data_input_folder, '/', filename, sep=''), getwd(),token)
    clessnverse::logit(scriptname, paste("reading", filename, "from cwd"), logger)
    doc_youtube <- readLines(filename)
    doc_youtube.original <- paste(paste(doc_youtube, "\n\n", sep=""), collapse = ' ')
    doc_youtube <- doc_youtube[doc_youtube!=""]
    cached_html <- FALSE
    current_url <- stringr::str_match(doc_youtube[3], "^URL   : (.*)")[2]
    clessnverse::logit(scriptname, paste(current_id, "not cached"), logger)
  } else{ 
    # Retrieve the XML structure from dfCache and Parse
    filter <- clessnhub::create_filter(key = event_id, type = "youtube_press_conference", schema = "v2", metadata = list("location"="CA-QC"))
    doc_youtube <- clessnhub::get_items('agoraplus_cache', filter = filter)
    doc_youtube <- doc_youtube$data.rawContent
    doc_youtube <- stringr::str_split(doc_youtube, '\n\n')[[1]]
    doc_youtube <- doc_youtube[doc_youtube!=""]
    doc_youtube.original <- doc_youtube
    cached_html <- TRUE
    current_url <- stringr::str_match(doc_youtube[3], "^URL   : (.*)")[2]
    clessnverse::logit(scriptname, paste(current_id, "cached"), logger)
  }
    
  version.finale <- TRUE

  if ( version.finale && 
       ( ((opt$simple_mode == "update" && !(current_id %in% dfSimple$eventID) ||
           opt$simple_mode == "refresh" ||
           opt$simple_mode == "rebuild") ||
          (opt$deep_mode == "update" && !(current_id %in% dfDeep$eventID) ||
           opt$deep_mode == "refresh" ||
           opt$deep_mode == "rebuild")) ||
         ((opt$hub_mode == "refresh" ||
           opt$hub_mode == "update") && current_id %in% dfSimple$eventID))
      ) {      
      ###############################
      # Columns of the simple dataset
      date <- NA
      time <- NA
      title <- NA
      subtitle <- NA
      end_time <- NA
      doc_text <- NA
      
      # Extract SourceType
      current_source <- "Conférences de Presse YouTube"

      # Extract date of the conference
      date <- stringr::str_match(doc_youtube[1], "^DATE  : (.*)")[2]

      # Extract start time of the conference
      time <- NA

      # Figure out the end time of the conference
      end_time <- NA
      
      # Title and subtitle of the conference  
      title <- stringr::str_match(doc_youtube[2], "^TITLE : (.*)")[2]
      subtitle <- NA

        
      # Extract all the paragraphs (HTML tag is p, starting at
      # the root of the document). Unlist flattens the list to
      # create a character vector.
      doc_text <- doc_youtube
                            
                            
      # Replace all \n by spaces and clean leading and trailing spaces
      # and clean the conference vector of unneeded paragraphs
      doc_text <- gsub('\\n',' ', doc_text)
      doc_text <- sub("^\\s+", "", doc_text)
      doc_text <- sub("\\s+$", "", doc_text)

      doc_text <- doc_text[doc_text!=""]
      doc_text <- doc_text[5:length(doc_text)]
      
      
      
      # Remove consecutive spaces (cleaning)
      doc_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", doc_text, perl=TRUE)
    

      ####################################
      # The colums of the detailed dataset
      first_name <- NA
      last_name <- NA
      full_name <- NA
      gender <- NA
      type <- NA
      party <- NA
      circ <- NA
      is_minister <- NA
      media <- NA
      language <- NA
      speech_type <- NA
      speech <- NA
      
      speaker <- data.frame()
      periode_de_questions <- FALSE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      seqnum <- 1
  
      event_paragraph_count <- length(doc_text)
      event_sentence_count <- clessnverse::countVecSentences(doc_text)
      
      if (length(doc_text) > 0) {
        
        for (j in 1:length(doc_text)) {
          
          cat("Conf:", i, "Paragraph:", j, "Intervention", seqnum, "\r", sep = " ")
          
          # Is this a new speaker taking the stand?  If so there is typically a : at the begining of the sentence
          # And the Sentence starts with the Title (M. Mme etc) and the last name of the speaker
          intervention_start <- substr(doc_text[j],1,40)
          
          if ( (grepl("^M\\.\\s+(.*?):", intervention_start) || 
                grepl("^Mme\\s+(.*?):", intervention_start) || 
                grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?):", intervention_start) ||
                grepl("^Titre(.*?):", intervention_start) ||
                grepl("^Journaliste(.*?):", intervention_start) ||
                grepl("^Modérat(.*?):", intervention_start) ||
                grepl("^Une\\svoix(.*?):", intervention_start) ||
                grepl("^Des\\svoix(.*?):", intervention_start)) &&
               !grepl(",", stringr::str_match(intervention_start, "^(.*):")) ) {
            # It's a new person speaking
            first_name <- NA
            last_name <- NA
            full_name <- NA
            gender <- NA
            type <- NA
            party <- NA
            circ <- NA
            is_minister <- NA
            media <- NA
            speech_type <- NA
            speech <- NA
            speaker <- data.frame()
            
            speech_paragraph_count <- 1
            speech_sentence_count <- 0
            speech_word_count <- 0
            
            # let's rule out the president first
            if ( stringr::str_detect(intervention_start, "^Modérateur\\s?:\\s+") ) { ### MODERATEUR ###
              is_minister <- 0
              speech_type <- "modération"
              type <- "modérateur"

              if (  j>1 && TRUE %in% stringr::str_detect(doc_text[j], patterns_periode_de_questions) )
                periode_de_questions <- TRUE
            } #stringr::str_detect(intervention_start, "^Modérateur\\s?:\\s+")

            
            if ( stringr::str_detect(intervention_start, "^M\\.(.*)(\\s+)?:\\s+") ||
                 stringr::str_detect(intervention_start, "^Mme(.*)(\\s+)?:\\s+") ) {
              # A ce stade on cherche à identifier l'intervenant et savoir
              # si c'est un député ou si c'est un journaliste
              
              # il faut voir si c'est un homme ou une femme
              # et ensuite extirper le prénom et le nom (entre parnethèse)
              
              if ( stringr::str_detect(intervention_start, "^M\\.(.*):") ) gender_femme <- 0
              else gender_femme <- 1
              
              last_name <- stringr::str_match(intervention_start, "^((M\\.)|(Mme))(.*)?\\s+(.*)\\s+\\((.*)\\)(\\s+)?:")[6]
              first_name <- stringr::str_match(intervention_start, "^((M\\.)|(Mme))(.*)?\\s+(.*)\\s+\\((.*)\\)(\\s+)?:")[7]
              
              # on cherche s'il est dans la liste de référence des députés
              speaker <- filter(deputes, (tolower(lastName1) == tolower(last_name) | tolower(lastName2) == tolower(last_name)) & isFemale == gender_femme)
              
              if ( nrow(speaker) > 0 ) { ### DÉPUTÉ ###
                is_minister <- speaker$isMinister[1]
                
                last_name <- paste(na.omit(speaker[1,]$lastName1), na.omit(speaker[1,]$lastName2), sep = " ")
                last_name <- trimws(last_name, which = c("both"))
                if (length(last_name) == 0) last_name <- NA
                first_name <- speaker[1,]$firstName
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
                is_minister <- speaker$isMinister[1]
                
                if ( !is.na(dfDeep$speakerSpeechType[nrow(dfDeep)]) && 
                     (j>1 && dfDeep$speakerSpeechType[nrow(dfDeep)] == "question") ||
                     periode_de_questions
                   )
                  speech_type <- "réponse"
                else
                  speech_type <- "allocution"
                
              } else { ### JOURNALISTE ###
                # On n'a pas trouvé first_name last_name dans députés 
                # on devrait pouvoir le trouver dans les journalistes
                speaker <- filter(journalists, tolower(paste(first_name, last_name, sep = " ")) == tolower(fullName))
                
                if ( nrow(speaker) > 0 ) {
                  # we have a JOURNALIST
                  
                  gender <- if ( speaker[1,]$isFemale ) "F" else "M"
                  type <- "journaliste"
                  party <- NA
                  circ <- NA
                  media <- speaker[1,]$source
                } 
              } # if (nrow(speaker) > 0)
            }  # stringr::str_detect(intervention_start, "^M\\.(.*):\\s+") || stringr::str_detect(intervention_start, "^Mme(.*):\\s+")

            
            if ( stringr::str_detect(intervention_start, "^Journaliste(\\s+)?:\\s+") ) { ### Journaliste ###
              is_minister <- NA
              type <- "journaliste"
            } #stringr::str_detect(intervention_start, "^Journaliste(\\s+)?:\\s+")
            
            if ( (!is.na(type) && periode_de_questions && type == "journaliste") ||
                 (is.na(type) && periode_de_questions)
                 ) 
              speech_type <- "question"

            speech <- substr(doc_text[j], unlist(gregexpr(":", intervention_start))+1, nchar(doc_text[j]))
            speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
    
          } else {
            # It's the same person as in the previous paragraph speaking
            # We will append it to the same row instead of creating an extra row for a new paragraph
            speech <- paste(speech,"\n\n",doc_text[j], sep="")
            speech_paragraph_count <- speech_paragraph_count + 1
          }
          
          if ( !(language %in% c("en","fr")) ) {
            language <- textcat::textcat(stringr::str_replace_all(speech, "[[:punct:]]", ""))
            if ( !(language %in% c("english","french")) ) { 
              language <- NA
            }
            else language <- substr(language,1,2)
          }
          
          speech_sentence_count <- clessnverse::countSentences(paste(speech, collapse = ' '))
          speech_word_count <- clessnverse::countWords(paste(speech, collapse = ' '))

          if (is.na(first_name) && is.na(last_name)) 
            full_name <- NA
          else 
            full_name <- trimws(paste(na.omit(first_name), na.omit(last_name), sep = " "),which = "both")
          
          # If the next speaker is different or if it's the last record, then let's commit this observation into the dataset
          
          if ( (grepl("^M\\.\\s+(.*?):", substr(doc_text[j+1],1,40)) || 
                grepl("^Mme\\s+(.*?):", substr(doc_text[j+1],1,40)) || 
                grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?):", substr(doc_text[j+1],1,40)) ||
                grepl("^Titre(.*?):", substr(doc_text[j+1],1,40)) ||
                grepl("^Journaliste(.*?):", substr(doc_text[j+1],1,40)) ||
                grepl("^Modérat(.*?):", substr(doc_text[j+1],1,40)) ||
                grepl("^Une\\svoix(.*?):", substr(doc_text[j+1],1,40)) ||
                grepl("^Des\\svoix(.*?):", substr(doc_text[j+1],1,40))) &&
               !grepl(",", stringr::str_match(substr(doc_text[j+1],1,40), "^(.*):")) &&
               !is.na(doc_text[j]) || 
               (j == length(doc_text)-1 && is.na(doc_text[j+1]))
            ) {
            
            # Update Deep
            row_to_commit <- data.frame(uuid = "",
                                        created = "",
                                        modified = "",
                                        metadata = "",
                                        eventID = current_id,
                                        interventionSeqNum = seqnum,
                                        speakerFirstName = first_name,
                                        speakerLastName = last_name,
                                        speakerFullName = full_name,
                                        speakerGender = gender,
                                        speakerIsMinister = is_minister,
                                        speakerType = type,
                                        speakerParty = party,
                                        speakerCirconscription = circ,
                                        speakerMedia = media,
                                        speakerSpeechType = speech_type,
                                        speakerSpeechLang = language,
                                        speakerSpeechWordCount = speech_word_count,
                                        speakerSpeechSentenceCount = speech_sentence_count,
                                        speakerSpeechParagraphCount = speech_paragraph_count,
                                        speakerSpeech = speech,
                                        speakerTranslatedSpeech = NA,
                                        stringsAsFactors = FALSE)
            
            dfDeep <- clessnverse::commitDeepRows(row_to_commit, dfDeep, 'agoraplus_warehouse_intervention_items', opt$deep_mode, opt$hub_mode)
            
            seqnum <- seqnum + 1
            
            first_name <- NA
            last_name <- NA
            full_name <- NA
            gender <- NA
            type <- NA
            party <- NA
            circ <- NA
            media <- NA
            speech_type <- NA
            speech <- NA
            
            speaker <- data.frame()
          } #If the next speaker is different or if it's the last record
        } # for (j in 1:length(doc_text))
      

      # Join all the elements of the character vector into a single
      # character string, separated by spaces for the simple dataSet
      collapsed.doc_text <- paste(paste(doc_text, "\n\n", sep=""), collapse = ' ')
      
      # Update the cache
      row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = current_id, eventHtml = doc_youtube.original, stringsAsFactors = FALSE)
      dfCache <- clessnverse::commitCacheRows(row_to_commit, dfCache, 'agoraplus_warehouse_cache_items', opt$cache_mode, opt$hub_mode)
      
      # Update Simple
      row_to_commit <- data.frame(uuid = "",
                                  created = "",
                                  modified = "",
                                  metadata = "",
                                  eventID = current_id,
                                  eventSourceType = current_source,
                                  eventURL = current_url,
                                  eventDate = as.character(date), 
                                  eventStartTime = as.character(time), 
                                  eventEndTime = as.character(end_time), 
                                  eventTitle = title, 
                                  eventSubtitle = subtitle, 
                                  eventSentenceCount = event_sentence_count,
                                  eventParagraphCount = event_paragraph_count,
                                  eventContent = collapsed.doc_text,
                                  eventTranslatedContent = NA,
                                  stringsAsFactors = FALSE)
      
      dfSimple <- clessnverse::commitSimpleRows(row_to_commit, dfSimple, 'agoraplus_warehouse_event_items', opt$simple_mode, opt$hub_mode)
      
    } # if (length(doc_text) > 0)
      
  } # version finale
  i <- i + 1
  
  
  clessnverse::logit(scriptname, paste("removing", filename, "from cwd"), logger)
  file.remove(filename)
  
  clessnverse::logit(scriptname, paste("moving", filename, "from", data_input_folder, "to", data_output_folder, "in dropbox"), logger)
  clessnverse::dbxMoveFile(source = paste(data_input_folder, filename, sep='/'), 
                           destination = paste(data_output_folder, filename, sep='/'), 
                           token = token, 
                           overwrite = TRUE)
  #file.rename(paste(data_input_folder, filename, sep = "/"), paste(data_output_folder, filename, sep = "/"))
  
} #for (filename in filelist)


clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)

