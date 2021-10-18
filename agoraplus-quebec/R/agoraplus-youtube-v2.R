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
# Function : installPackages
# This function installs all packages requires in this script and all the
# scripts called by this one
#

rm_accent <- function(str,pattern="all") {
  if(!is.character(str))
    str <- as.character(str)
  
  pattern <- unique(pattern)
  
  if(any(pattern=="Ç"))
    pattern[pattern=="Ç"] <- "ç"
  
  symbols <- c(
    acute = "áéíóúÁÉÍÓÚýÝ",
    grave = "àèìòùÀÈÌÒÙ",
    circunflex = "âêîôûÂÊÎÔÛ",
    tilde = "ãõÃÕñÑ",
    umlaut = "äëïöüÄËÏÖÜÿ",
    cedil = "çÇ"
  )
  
  nudeSymbols <- c(
    acute = "aeiouAEIOUyY",
    grave = "aeiouAEIOU",
    circunflex = "aeiouAEIOU",
    tilde = "aoAOnN",
    umlaut = "aeiouAEIOUy",
    cedil = "cC"
  )
  
  accentTypes <- c("´","`","^","~","¨","ç")
  
  if(any(c("all","al","a","todos","t","to","tod","todo")%in%pattern)) 
    return(chartr(paste(symbols, collapse=""), paste(nudeSymbols, collapse=""), str))
  
  for(i in which(accentTypes%in%pattern))
    str <- chartr(symbols[i],nudeSymbols[i], str) 
  
  return(str)
} # </rm_accent <- function(str,pattern="all")>

###############################################################################
#   Globals
#
#   scriptname
#   logger
#
installPackages()
library(dplyr)

if (!exists("scriptname")) scriptname <- "agoraplus-youtube-v2.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit("scraper.log", c("file", "hub"), Sys.getenv("LOG_PATH"))

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
                                                               location = "CA-QC", format = "video",
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
  dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type="press_conference", schema = "v2")
}

# Download v2 Cache
if (opt$dataframe_mode %in% c("update","refresh")) {
  dfCache2 <- clessnverse::loadAgoraplusCacheDf(type = "press_conference", schema = "v2",
                                                location = "CA-QC",
                                                download_data = opt$download_data,
                                                token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfCache2)) dfCache2 <- clessnverse::createAgoraplusCacheDf(type = "press_conference", schema = "v2")
  
  if (opt$download_data) { 
    dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location", 
                            "data.eventID", "data.rawContent")]
  } else {
    dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location")]
  }
  
} else {
  clessnverse::logit(scriptname, "Not retreiving cache from hub because hub_mode is rebuild or skip", logger)
  dfCache2 <- clessnverse::createAgoraplusCacheDf(type="press_conference", schema = "v2")
}

# Download v2 MPs information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(country="CA")#, province_or_state="QC", institution="National Assembly of Quebec")
filter <- clessnhub::create_filter(type="mp", schema="v2", metadata=metadata_filter)  
dfMPs <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

# Download v2 public service personnalities information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(country="CA", province_or_state="QC")
filter <- clessnhub::create_filter(type="public_service", schema="v2", metadata=metadata_filter)  
dfPublicService <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

dfMPs <- dfMPs %>% full_join(dfPublicService)


# Download v2 journalists information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(country="CA", province_or_state="QC")
filter <- clessnhub::create_filter(type="journalist", schema="v2", metadata=metadata_filter)  
dfJournalists <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

dfMPs <- dfMPs %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ", extra = "merge")
dfJournalists <- dfJournalists %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ", extra = "merge")



# Load all objects used for ETL including V1 HUB MPs
clessnverse::loadETLRefData()

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
  event_id <- paste("yt", stringr::str_match(filename, "^.{11}(.*).txt")[2], sep="")
  
  clessnverse::logit(scriptname, paste("Conf", i, "de", length(filelist), filename, sep = " "), logger) 
  cat("\nConf", i, "de", length(filelist), filename, "\n", sep = " ")
  
  if ( !(event_id %in% dfCache2$key) ) {
    # Read and parse HTML from the URL directly
    clessnverse::logit(scriptname, paste("downloading", paste(data_input_folder, filename, sep='/'), "from dropbox"), logger)
    clessnverse::dbxDownloadFile(paste('/', data_input_folder, '/', filename, sep=''), getwd(),token)
    clessnverse::logit(scriptname, paste("reading", filename, "from cwd"), logger)
    doc_youtube <- readLines(filename)
    Encoding(doc_youtube) <- "UTF-8"
    doc_youtube.original <- paste(paste(doc_youtube, "\n\n", sep=""), collapse = ' ')
    doc_youtube <- doc_youtube[doc_youtube!=""]
    cached_html <- FALSE
    current_url <- stringr::str_match(doc_youtube[3], "^URL   : (.*)")[2]
    clessnverse::logit(scriptname, paste(event_id, "not cached"), logger)
  } else{ 
    # Retrieve the XML structure from dfCache and Parse
    filter <- clessnhub::create_filter(key = event_id, type = "youtube_press_conference", schema = "v2", metadata = list("location"="CA-QC"))
    doc_youtube <- clessnhub::get_items('agoraplus_cache', filter = filter)
    doc_youtube <- doc_youtube$data.rawContent
    doc_youtube <- stringr::str_split(doc_youtube, '\n\n')[[1]]
    doc_youtube <- doc_youtube[doc_youtube!=""]
    Encoding(doc_youtube) <- "UTF-8"
    doc_youtube.original <- doc_youtube
    cached_html <- TRUE
    current_url <- stringr::str_match(doc_youtube[3], "^URL   : (.*)")[2]
    clessnverse::logit(scriptname, paste(event_id, "cached"), logger)
  }
    
  version.finale <- TRUE

  if ( version.finale && 
       ( ((opt$simple_mode == "update" && !(event_id %in% dfSimple$eventID) ||
           opt$simple_mode == "refresh" ||
           opt$simple_mode == "rebuild") ||
          (opt$deep_mode == "update" && !(event_id %in% dfDeep$eventID) ||
           opt$deep_mode == "refresh" ||
           opt$deep_mode == "rebuild")) ||
         ((opt$hub_mode == "refresh" ||
           opt$hub_mode == "update") && event_id %in% dfSimple$eventID))
      ) {      
      ###############################
      # Columns of the simple dataset
      event_date <- NA
      event_start_time <- NA
      event_end_time <- NA
      event_title <- NA
      event_subtitle <- NA

      doc_text <- NA
      
      # Extract SourceType
      event_source_type <- "Conférences de Presse YouTube"

      # Extract date of the conference
      event_date <- stringr::str_match(doc_youtube[1], "^DATE  : (.*)")[2]

      # Extract start time of the conference
      event_start_time <- NA

      # Figure out the end time of the conference
      event_end_time <- NA
      
      # Title and subtitle of the conference  
      event_title <- stringr::str_match(doc_youtube[2], "^TITLE : (.*)")[2]
      event_subtitle <- NA
      
      # URL of the video 
      event_url <- stringr::str_match(doc_youtube[3], "^URL   : (.*)")[2]

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
      speaker_first_name <- NA
      speaker_last_name <- NA
      speaker_full_name <- NA
      speaker_gender <- NA
      speaker_type <- NA
      speaker_party <- NA
      speaker_district <- NA
      speaker_is_minister <- NA
      speaker_media <- NA
      language <- NA
      intervention_type <- NA
      last_intervention_type <- NA
      intervention_text <- NA
      
      gender_femme <- 0
      speaker <- data.frame()
      periode_de_questions <- FALSE
      no_questions_asked_yet <- TRUE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      intervention_seqnum <- 1
  
      event_paragraph_count <- length(doc_text)
      event_sentence_count <- clessnverse::countVecSentences(doc_text)
      speech_paragraph_count <- 0

      cat("\n")      

      for (j in 1:length(doc_text)) {
        cat("Conf:", i, "Paragraph:", j, "Intervention", intervention_seqnum, "\r", sep = " ")
        
        # Is this a new speaker taking the stand?  If so there is typically a : at the begining of the sentence
        # And the Sentence starts with the Title (M. Mme etc) and the last name of the speaker
        paragraph_start <- substr(doc_text[j],1,55)
        
        if ( length(strsplit(paragraph_start, ":")[[1]]) == 1 ) {
          # There is no : in the beginning of the paragraph => it is probably a continuity of the same intervention
          paragraph_start <- paragraph_start
        } else {
          # Could be a new intervention => we take the  first token before the first : to see if that is the case
          paragraph_start <- paste(strsplit(paragraph_start, ":")[[1]][1], ":", sep='')
        }
        
        next_paragraph_start <- substr(doc_text[j+1],1,55)
        
        if ( length(strsplit(next_paragraph_start, ":")[[1]]) == 1 ) {
          # There is no : in the beginning of the paragraph => it is probably a continuity of the same intervention
          next_paragraph_start <- next_paragraph_start
        } else {
          # Could be a new intervention => we take the  first token before the first : to see if that is the case
          next_paragraph_start <- paste(strsplit(next_paragraph_start, ":")[[1]][1], ":", sep='')
        }
        
        if ( TRUE %in% stringr::str_detect(paragraph_start, patterns_intervenants) &&
             !grepl(",", stringr::str_match(paragraph_start, "^(.*):")[1]) &&
             stringr::str_detect(paragraph_start, "^(.*):") &&
             !grepl("cette transcription est une version préliminaire", tolower(doc_text[j])) ) {
          # It's a new person speaking
          
          # Skip if this intervention already is in the dataset and if we're not refreshing it
          matching_row <- which(dfInterventions$key == paste(event_id, intervention_seqnum, sep="-"))
          if ( length(matching_row) > 0 && opt$dataframe_mode != "refresh" ) {
            if ((TRUE %in% stringr::str_detect(next_paragraph_start, patterns_intervenants)) &&
                !grepl(",", stringr::str_match(next_paragraph_start, "^(.*):")[1]) &&
                 stringr::str_detect(next_paragraph_start, "^(.*):") &&
                 !is.na(doc_text[j]) || 
                (j == length(doc_text) && is.na(doc_text[j+1]))) {
              intervention_seqnum <- intervention_seqnum + 1
            }
            intervention_text <- substr(doc_text[j], unlist(gregexpr(":", paragraph_start))+1, nchar(doc_text[j]))
            intervention_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", intervention_text, perl=TRUE)
            matching_row <- NULL
            next
          }
          
          speaker_first_name <- NA
          speaker_last_name <- NA
          speaker_full_name <- NA
          speaker_gender <- NA
          speaker_type <- NA
          speaker_party <- NA
          speaker_district <- NA
          speaker_is_minister <- NA
          speaker_media <- NA
          intervention_type <- NA
          language <- NA
          intervention_text <- NA
          gender_femme <- 0
          speaker <- data.frame()
          
          speech_paragraph_count <- 1
          speech_sentence_count <- 0
          speech_word_count <- 0
          
          # let's rule out the moderator first
          if ( stringr::str_detect(paragraph_start, "^Modérateur\\s?:") ) {
            ### MODERATEUR ###
            
            speaker_first_name <- "Modérateur"
            speaker_last_name <- "Modérateur"
            speaker_gender <- NA
            speaker_is_minister <- 0
            speaker_type <- "moderator"
            speaker_party <- NA
            speaker_district <- NA
            speaker_media <- NA
            intervention_type <- "modération"    

            if (  j>1 && TRUE %in% stringr::str_detect(tolower(doc_text[j]), patterns_periode_de_questions) ) periode_de_questions <- TRUE
          } #stringr::str_detect(paragraph_start, "^Modérateur\\s?:\\s+")

          
          if ( stringr::str_detect(paragraph_start, "^M\\.(.*)(\\s+)?:") ||
               stringr::str_detect(paragraph_start, "^Mme(.*)(\\s+)?:") ) {
            # A ce stade on cherche à identifier l'intervenant et savoir
            # si c'est un député ou si c'est un journaliste
            
            # il faut voir si c'est un homme ou une femme
            # et ensuite extirper le prénom et le nom (entre parnethèse)
            
            if ( stringr::str_detect(paragraph_start, "^M\\.(.*):") ) {
              gender_femme <- 0
            } else {
              gender_femme <- 1
            }
            
            speaker_last_name <- stringr::str_match(paragraph_start, "^((M\\.)|(Mme))(.*)?\\s+(.*)\\s+\\((.*)\\)(\\s+)?:")[6]
            speaker_first_name <- stringr::str_match(paragraph_start, "^((M\\.)|(Mme))(.*)?\\s+(.*)\\s+\\((.*)\\)(\\s+)?:")[7]
            
            # on cherche s'il est dans la liste de référence des députés
            speaker <- dplyr::filter(dfMPs, (trimws(tolower(rm_accent(data.lastName1))) == trimws(tolower(rm_accent(speaker_last_name))) | 
                                               trimws(tolower(rm_accent(data.lastName2))) == trimws(tolower(rm_accent(speaker_last_name)))) & 
                                             trimws(tolower(rm_accent(data.firstName))) == trimws(tolower(rm_accent(speaker_first_name))) &
                                             data.isFemale == gender_femme &
                                             (type == "mp" | type == "public_service"))
            
            
            if ( nrow(speaker) > 0 ) { ### DÉPUTÉ ###
              speaker_is_minister <- speaker$data.isMinister[1]
              
              speaker_last_name <- paste(na.omit(speaker$data.lastName1[1]), na.omit(speaker$data.lastName2[1]), sep = " ")
              speaker_last_name <- trimws(speaker_last_name, which = c("both"))
              if (length(speaker_last_name) == 0) speaker_last_name <- NA
              
              speaker_first_name <- speaker$data.firstName[1]
              speaker_gender <- if ( speaker$data.isFemale[1] == 1 ||  speaker$data.isFemale[1] == "1" ) "F" else "M"
              if ( tolower(speaker$type[1]) == "public_service" ) {
                speaker_type <- tolower(speaker$type[1])
                speaker_party <- NA
                speaker_district <- NA
              } else {
                speaker_type <-  tolower(speaker$type[1])
                speaker_party <- speaker$data.currentParty[1]
                speaker_district <- speaker$data.currentDistrict[1]
              }
              
              speaker_media <- NA
              speaker_is_minister <- speaker$data.isMinister[1]
              
              if (no_questions_asked_yet) {
                intervention_type <- "allocution"
              } else {
                #if ( periode_de_questions || substr(doc_text[j-1], nchar(doc_text[j-1]), nchar(doc_text[j-1])) == "?" ) {
                if (last_intervention_type == "question" ) {
                  intervention_type <- "réponse"
                }
                else {
                  intervention_type <- "commentaire"
                }
              }
              
            } else { ### JOURNALISTE ###
              # On n'a pas trouvé first_name last_name dans députés 
              # on devrait pouvoir le trouver dans les journalistes
              #speaker <- filter(journalists, tolower(paste(first_name, last_name, sep = " ")) == tolower(fullName))
              
              speaker <- dplyr::filter(dfJournalists, trimws(rm_accent(tolower(paste(speaker_last_name, speaker_first_name, sep = ", ")))) == trimws(rm_accent(tolower(data.fullName))))
              
              if ( nrow(speaker) > 0 ) {
                # we have a JOURNALIST
                
                speaker_gender <- if ( speaker$data.isFemale[1] == 1 || speaker$data.isFemale[1] == "1" ) "F" else "M"
                speaker_type <- speaker$type[1]
                speaker_party <- NA
                speaker_district <- NA
                speaker_media <- speaker$data.currentMedia[1]
                
                if ( periode_de_questions ) {
                  intervention_type <- "question"
                  no_questions_asked_yet <- FALSE
                } else {
                  if ( grepl("\\?$",doc_text[j]) ) {
                    intervention_type <- "question" 
                    no_questions_asked_yet <- FALSE
                  } else { 
                    intervention_type <- "commentaire"
                  }
                }
              
              } # if (nrow(speaker) > 0) ### JOURNALIST ###
            } # if (nrow(speaker) > 0) ### DÉPUTÉ ###
          }  # stringr::str_detect(paragraph_start, "^M\\.(.*):\\s+") || stringr::str_detect(paragraph_start, "^Mme(.*):\\s+")

          
          if ( stringr::str_detect(tolower(paragraph_start), "^journaliste(\\s+)?(.*):") ) { ### Journaliste ###
            speaker_is_minister <- NA
            speaker_type <- "journalist"
            if (stringr::str_detect(tolower(paragraph_start), "^journaliste(\\s+)?\\((.*)\\)(\\s+)?:")) {
              speaker_media <- stringr::str_to_title(stringr::str_match(tolower(paragraph_start), "^journaliste(\\s+)?\\((.*)\\)(\\s+)?:")[3])
            }
          } #stringr::str_detect(paragraph_start, "^Journaliste(\\s+)?:\\s+")
          
          
          if ( is.na(intervention_type) ) intervention_type <- "commentaire"
          if ( !is.na(intervention_type) &&  grepl("\\?$",doc_text[j]) ) intervention_type <- "question"
          
          #if ( (!is.na(speaker_type) && periode_de_questions && speaker_type == "journaliste") ||
          #     (is.na(speaker_type) && periode_de_questions) ) { 
          #  intervention_type <- "question"
          #}

          intervention_text <- substr(doc_text[j], unlist(gregexpr(":", paragraph_start))+1, nchar(doc_text[j]))
          intervention_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", intervention_text, perl=TRUE)
  
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          intervention_text <- paste(intervention_text,"\n\n",doc_text[j], sep="")
          speech_paragraph_count <- speech_paragraph_count + 1
        }
        
        language <- textcat::textcat(stringr::str_replace_all(intervention_text, "[[:punct:]]", ""))
        if ( !(language %in% c("english","french")) ) { 
          language <- "fr"
        } else {
          language <- substr(language,1,2)
        }
        
        speech_paragraph_count <- stringr::str_count(intervention_text, "\\n\\n")+1
        speech_sentence_count <- clessnverse::countSentences(paste(intervention_text, collapse = ' '))
        speech_word_count <- clessnverse::countWords(paste(intervention_text, collapse = ' '))

        if (is.na(speaker_first_name) && is.na(speaker_last_name)) {
          speaker_full_name <- NA
        } else { 
          speaker_full_name <- trimws(paste(na.omit(speaker_last_name), na.omit(speaker_first_name),  sep = ", "),which = "both")
        }
        
        # If the next speaker is different or if it's the last record, then let's commit this observation into the dataset
        
        if ( ((TRUE %in% stringr::str_detect(next_paragraph_start, patterns_titres)) &&
              !grepl(",", stringr::str_match(next_paragraph_start, "^(.*):")[1])) &&
             stringr::str_detect(next_paragraph_start, "^(.*):") &&
             !is.na(doc_text[j]) || 
             (j == length(doc_text) && is.na(doc_text[j+1])) ) {
          
          # Update Deep
          row_to_commit <- data.frame(uuid = "",
                                      created = "",
                                      modified = "",
                                      metadata = "",
                                      eventID = event_id,
                                      interventionSeqNum = intervention_seqnum,
                                      speakerFirstName = speaker_first_name,
                                      speakerLastName = speaker_last_name,
                                      speakerFullName = speaker_full_name,
                                      speakerGender = speaker_gender,
                                      speakerIsMinister = speaker_is_minister,
                                      speakerType = speaker_type,
                                      speakerParty = speaker_party,
                                      speakerCirconscription = speaker_district,
                                      speakerMedia = speaker_media,
                                      speakerSpeechType = intervention_type,
                                      speakerSpeechLang = language,
                                      speakerSpeechWordCount = speech_word_count,
                                      speakerSpeechSentenceCount = speech_sentence_count,
                                      speakerSpeechParagraphCount = speech_paragraph_count,
                                      speakerSpeech = intervention_text,
                                      speakerTranslatedSpeech = NA,
                                      stringsAsFactors = FALSE)
          
          ###dfDeep <- clessnverse::commitDeepRows(row_to_commit, dfDeep, 'agoraplus_warehouse_intervention_items', opt$deep_mode, opt$hub_mode)
          
          # Update intervention in hub v2
          v2_row_to_commit <- data.frame(eventID = event_id,
                                         eventDate = event_date,
                                         eventStartTime = event_start_time,
                                         eventEndTime = event_end_time,
                                         eventTitle = event_title,
                                         eventSubTitle = event_subtitle,
                                         interventionSeqNum = intervention_seqnum,
                                         objectOfBusinessID = NA,
                                         objectOfBusinessRubric = NA,
                                         objectOfBusinessTitle = NA,
                                         objectOfBusinessSeqNum = NA,
                                         subjectOfBusinessID = NA,
                                         subjectOfBusinessTitle = NA,
                                         subjectOfBusinessHeader = NA,
                                         subjectOfBusinessSeqNum = NA,
                                         subjectOfBusinessProceduralText = NA,
                                         subjectOfBusinessTabledDocID = NA,
                                         subjectOfBusinessTabledDocTitle = NA,
                                         subjectOfBusinessAdoptedDocID = NA,
                                         subjectOfBusinessAdoptedDocTitle = NA,
                                         speakerID = NA,
                                         speakerFirstName = speaker_first_name,
                                         speakerLastName = speaker_last_name,
                                         speakerFullName = speaker_full_name,
                                         speakerGender = speaker_gender,
                                         speakerType = speaker_type,
                                         speakerCountry = "CA",
                                         speakerIsMinister = speaker_is_minister,
                                         speakerParty = speaker_party,
                                         speakerPolGroup = NA,
                                         speakerDistrict = speaker_district,
                                         speakerMedia = speaker_media,
                                         interventionID = paste(gsub("yt", "", event_id),intervention_seqnum,sep=''),
                                         interventionDocID = NA,
                                         interventionDocTitle = NA,
                                         interventionType = intervention_type,
                                         interventionLang = language,
                                         interventionWordCount = speech_word_count,
                                         interventionSentenceCount = speech_sentence_count,
                                         interventionParagraphCount = speech_paragraph_count,
                                         interventionText = intervention_text,
                                         interventionTextFR = NA,
                                         interventionTextEN = NA,
                                         stringsAsFactors = FALSE)
          
          v2_row_to_commit <- v2_row_to_commit %>% mutate(across(everything(), as.character))
          
          v2_metadata_to_commit <- list("url"=event_url,"format"="video","location"="CA-QC")
          
          dfInterventions <- clessnverse::commitAgoraplusInterventions(dfDestination = dfInterventions, 
                                                                       type = "press_conference", schema = "v2",
                                                                       metadata = v2_metadata_to_commit,
                                                                       data = v2_row_to_commit,
                                                                       opt$dataframe_mode, opt$hub_mode)
          
          intervention_seqnum <- intervention_seqnum + 1
          matching_row <- NULL
          
          last_intervention_type <- intervention_type
          intervention_text <- NA
          speech_paragraph_count <- 0
          speech_sentence_count <- 0
          speech_word_count <- 0
          
          speaker_first_name <- NA
          speaker_last_name <- NA
          speaker_full_name <- NA
          speaker_gender <- NA
          speaker_type <- NA
          speaker_party <- NA
          speaker_district <- NA
          speaker_media <- NA
          intervention_type <- NA
          intervention_text <- NA
          language <- NA
          
          speaker <- data.frame()
          
          speech_paragraph_count <- 0
          speech_sentence_count <- 0
          speech_word_count <- 0
        } #If the next speaker is different or if it's the last record
      } # for (j in 1:length(doc_text))
      

    # Join all the elements of the character vector into a single
    # character string, separated by spaces for the simple dataSet
    collapsed.doc_text <- paste(paste(doc_text, "\n\n", sep=""), collapse = ' ')
    
    # Update the cache
    row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = event_id, eventHtml = doc_youtube.original, stringsAsFactors = FALSE)
    ###dfCache <- clessnverse::commitCacheRows(row_to_commit, dfCache, 'agoraplus_warehouse_cache_items', opt$cache_mode, opt$hub_mode)
    
    # Update cache dans hub v2
    v2_row_to_commit <- data.frame(eventID = event_id, rawContent = doc_youtube.original, stringsAsFactors = FALSE)
    v2_metadata_to_commit <- list("url"=event_url, "format"="video", "location"="CA-QC")
    
    dfCache2 <- clessnverse::commitAgoraplusCache(dfDestination = dfCache2, type = "press_conference", schema = "v2",
                                                  metadata = v2_metadata_to_commit,
                                                  data = v2_row_to_commit,
                                                  opt$dataframe_mode, opt$hub_mode)
    
    
    # Update Simple
    row_to_commit <- data.frame(uuid = "",
                                created = "",
                                modified = "",
                                metadata = "",
                                eventID = event_id,
                                eventSourceType = event_source_type,
                                eventURL = current_url,
                                eventDate = as.character(event_date), 
                                eventStartTime = as.character(event_start_time), 
                                eventEndTime = as.character(event_end_time), 
                                eventTitle = event_title, 
                                eventSubtitle = event_subtitle, 
                                eventSentenceCount = event_sentence_count,
                                eventParagraphCount = event_paragraph_count,
                                eventContent = collapsed.doc_text,
                                eventTranslatedContent = NA,
                                stringsAsFactors = FALSE)
    
    ###dfSimple <- clessnverse::commitSimpleRows(row_to_commit, dfSimple, 'agoraplus_warehouse_event_items', opt$simple_mode, opt$hub_mode)
      
  } # version finale
  i <- i + 1
  
  
  clessnverse::logit(scriptname, paste("removing", filename, "from cwd"), logger)
  file.remove(filename)
  
  clessnverse::logit(scriptname, paste("moving", filename, "from", data_input_folder, "to", data_output_folder, "in dropbox"), logger)
  clessnverse::dbxMoveFile(source = paste(data_input_folder, filename, sep='/'), 
                           destination = paste(data_output_folder, filename, sep='/'), 
                           token = token, 
                           overwrite = TRUE)

} #for (filename in filelist)


clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)

