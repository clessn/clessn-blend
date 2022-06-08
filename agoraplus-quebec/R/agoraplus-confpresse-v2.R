###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                           agora-plus-confpresse                             #
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

if (!exists("scriptname")) scriptname <- "agoraplus-confpresse-v2.R"

# Script command line options:
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataframe by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataframe
# - rebuild : wipes out completely the dataframe and rebuilds it from scratch
# - skip : does not make any change to the dataframe
#opt <- list(dataframe_mode = "refresh", hub_mode = "refresh", log_output = "file,console,hub", download_data = TRUE)

if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit("scraper", opt$log_output, Sys.getenv("LOG_PATH"))

# Download HUB v1 data 
#clessnverse::loadAgoraplusHUBDatasets("quebec", opt, 
#                                      Sys.getenv('HUB_USERNAME'), 
#                                      Sys.getenv('HUB_PASSWORD'), 
#                                      Sys.getenv('HUB_URL'))


# Download HUB v2 data
if (opt$dataframe_mode %in% c("update","refresh")) {
  clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
  dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "press_conference", schema = "v2", 
                                                               location = "CA-QC", format = "html",
                                                               download_data = opt$download_data,
                                                               token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfInterventions)) dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type = "press_conference", schema = "v2")
  
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
                                                download_data = FALSE,
                                                token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfCache2)) dfCache2 <- clessnverse::createAgoraplusCacheDf(type = "parliament_debate", schema = "v2")
  
  dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location")]
  
} else {
  clessnverse::logit(scriptname, "Not retreiving cache from hub because hub_mode is rebuild or skip", logger)
  dfCache2 <- clessnverse::createAgoraplusCacheDf(type="press_conference", schema = "v2")
}

# Download v2 MPs information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(country="CA", province_or_state="QC", institution="National Assembly of Quebec")
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


clessnverse::loadETLRefData()

###############################################################################
# Data source
#
# connect to the dataSource : the provincial parliament web site 
# get the index page containing the URLs to all the press conference
# to extract those URLS and get them individually in order to parse
# each press conference
#
base_url <- "http://www.assnat.qc.ca"
content_url <- "/fr/actualites-salle-presse/conferences-points-presse/index.html"

# Pour rouler le script sur une base quotidienne et aller chercher les débats récents Utiliser le ligne ci-dessous
data <- xml2::read_html(paste(base_url,content_url,sep=""))

# Pour rouler le script sur les conf de presse choisies (ex: celles de 2020) utiliser la ligne ci-dessous
#data <- xml2::read_html("/Users/patrick/Dropbox/clessn-blend/_SharedFolder_clessn-blend/data/ConférencesEtPointsDePresse-AssembléeNationaleDuQuébec-2020.html")

urls <- rvest::html_nodes(data, 'li.icoHTML a')
# To obtain the list of conferences available in the FIRST search results page (default page)
list_urls <- rvest::html_attr(urls, 'href')

# Hack here to focus only on one press conf :
#list_urls <-c("/fr/actualites-salle-presse/conferences-points-presse/ConferencePointPresse-74207.html")











###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it o extract the
# press conference content
#
for (i in 94:length(list_urls)) {
  
  event_url <- paste(base_url,list_urls[i],sep="")
  #event_url <- list_urls[i]
  event_id <- paste("cp", stringr::str_sub(event_url,100,104), sep='')
  
  clessnverse::logit(scriptname, paste("Conf", i, "de", length(list_urls),sep = " "), logger)
  cat("\nConf", i, "de", length(list_urls),"\n")
  

  # Make sure the data comes from the pres conf (we know that from the URL)
  if (grepl("actualites-salle-presse", event_url)) {     
    ###
    # If the data is not cache we get the raw html from assnat.qc.ca
    # if it is cached (we scarped it before), we prefer not to bombard
    # the website with HTTP_GET requests and ise the cached version
    ###
    if ( !(event_id %in% dfCache2$key) ) {
      # Read and parse HTML from the URL directly
      doc_html <- RCurl::getURL(event_url)
      doc_html.original <- doc_html
      parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
      cached_html <- FALSE
      clessnverse::logit(scriptname, paste(event_id, "not cached"), logger)
    } else{
     # Retrieve the XML structure from dfCache and Parse
     filter <- clessnhub::create_filter(key = event_id, type = "press_conference", schema = "v2", metadata = list("location"="CA-QC"))
     doc_html <- clessnhub::get_items('agoraplus_cache', filter = filter)
     doc_html <- doc_html$data.rawContent
     doc_html.original <- doc_html
     parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
     cached_html <- TRUE
     clessnverse::logit(scriptname, paste(event_id, "cached"), logger)
    }
      
    # Dissect the text based on html tags
    doc_h1 <- XML::xpathApply(parsed_html, '//h1', XML::xmlValue)
    doc_h2 <- XML::xpathApply(parsed_html, '//h2', XML::xmlValue)
    doc_h3 <- XML::xpathApply(parsed_html, '//h3', XML::xmlValue)
    doc_span <- XML::xpathApply(parsed_html, '//span', XML::xmlValue)
    doc_span <- unlist(doc_span)
    
    # Valide la version : préliminaire ou finale
    if ( length(grep("version finale", tolower(doc_h2))) > 0 ) {
      version_finale <- TRUE
      clessnverse::logit(scriptname, "version finale", logger)
      cat("version finale")
    } else {
      version_finale <- FALSE
      clessnverse::logit(scriptname, "version préliminaire", logger)
      cat("version préliminaire")
    }
  
    #if ( version_finale && 
    if ( ((opt$dataframe_mode == "update" && !(TRUE %in% grepl(event_id, dfInterventions$key)) ||
           opt$dataframe_mode == "refresh" ||
           opt$dataframe_mode == "rebuild")) ||
         ((opt$hub_mode == "refresh" || opt$hub_mode == "update") && 
          TRUE %in% grepl(event_id, dfInterventions$key)) ) {
      
      ###############################
      # Columns related to the event
      event_date <- NA
      event_start_time <- NA
      event_title <- NA
      event_subtitle <- NA
      event_end_time <- NA
      doc_text <- NA
      
      # Extract SourceType    
      event_source_type <- doc_span[32]
      event_source_type <- gsub("\n","",event_source_type)
      event_source_type <- gsub("\r","",event_source_type)
      event_source_type <- sub("^\\s+", "", event_source_type)
      event_source_type <- sub("\\s+$", "", event_source_type)
      
      # Extract date of the conference
      date_time <- doc_h3[6]
      event_date_time_text <- clessnverse::splitWords(date_time[1])
      event_date <- gsub(",", "", event_date_time_text)
      day_of_week <- days_fr[which(days_fr %in% event_date_time_text)]
      days_of_week_position <- grep(day_of_week, event_date_time_text)
      datestr <- paste(event_date[days_of_week_position+1],months_en[match(tolower(event_date[days_of_week_position+2]),months_fr)],event_date[days_of_week_position+3])
      event_date <- as.Date(datestr, format = "%d %B %Y")
      
      
      # Title and subtitle of the conference  
      event_title <- doc_h1[2]
      event_title <- gsub("\n","",event_title)
      event_title <- gsub("\r","",event_title)
      event_title <- sub("^\\s+", "", event_title)
      event_title <- sub("\\s+$", "", event_title)
      
      event_subtitle <- doc_h2[4]
      event_subtitle <- gsub("\n","",event_subtitle)
      event_subtitle <- gsub("\r","",event_subtitle)
      event_subtitle <- sub("^\\s+", "", event_subtitle)
      event_subtitle <- sub("\\s+$", "", event_subtitle)
      
        
      # Extract all the paragraphs (HTML tag is p, starting at
      # the root of the document). Unlist flattens the list to
      # create a character vector.
      doc_text <- unlist(XML::xpathApply(parsed_html, '//p', XML::xmlValue))
      doc_text <- gsub('\u00a0',' ', doc_text)
      
      
      # Replace all \n by spaces and clean leading and trailing spaces
      # and clean the conference vector of unneeded paragraphs
      
      doc_text <- gsub('\\n',' ', doc_text)
      doc_text <- sub("^\\s+", "", doc_text)
      doc_text <- sub("\\s+$", "", doc_text)
      doc_text[1] <- NA
      doc_text[2] <- NA
      doc_text[3] <- NA
      doc_text[4] <- NA
      doc_text <- na.omit(doc_text)  
      
      if (grepl("cette transcription est une version préliminaire", tolower(doc_text[2]))) {
        doc_text[1] <- NA
        doc_text[2] <- NA
        doc_text[3] <- NA
        doc_text <- na.omit(doc_text) 
      }

      # Extract start time of the conference
      if (stringr::str_detect(tolower(doc_text[1]), "heures")) {
        doc_text[1] <- gsub("\\(|\\)", "", doc_text[1])
        doc_text[1] <- gsub("\\s\\s+", " ", doc_text[1])
        hour <- strsplit(doc_text[1], " ")[[1]][1]
        hour <- clessnverse::convertTextToNumberFR(hour)[[2]][1]
        if (nchar(hour) == 1) hour <- paste("0",hour,sep='')
        if (clessnverse::countWords(doc_text[1]) > 2) {
          minute <- strsplit(doc_text[1], " ")[[1]][3]
          if (length(strsplit(doc_text[1], " ")[[1]]) > 3 && strsplit(doc_text[1], " ")[[1]][4] == "et") {
            minute <- paste(minute, "et", strsplit(doc_text[1], " ")[[1]][5], sep = ' ')
          }
          if (!is.na(minute)) minute <- clessnverse::convertTextToNumberFR(minute)[[2]][1] else minute <- "00"
          if (nchar(minute) == 1) minute <- paste("0",minute,sep='')
        } else {
          minute <- "00"
        }
        event_start_time <- paste(event_date, " ", hour,":",minute,":00",sep='')
      }
      
      event_start_time <- strptime(event_start_time, "%Y-%m-%d %H:%M")
      
      # Figure out the end time of the conference
      if ( doc_text[length(doc_text)] != "" && !grepl("Cliquez", doc_text[length(doc_text)]) ) event_end_time <- doc_text[length(doc_text)] else event_end_time <- doc_text[length(doc_text)-1]
      #event_end_time <- if (doc_text[length(doc_text)] == "") doc_text[length(doc_text)-1] else doc_text[length(doc_text)]
      event_end_time <- gsub("\\(",'', event_end_time)
      event_end_time <- gsub("\\)",'', event_end_time)
      event_end_time <- clessnverse::splitWords(event_end_time) 
      
      if ( event_end_time[length(event_end_time)] == "heures" || event_end_time[length(event_end_time)] == "h" ) {
        event_end_time[length(event_end_time)] <- ":"
        event_end_time[length(event_end_time)+1] <- "00"
      }
      
      event_end_time <- paste(event_end_time[length(event_end_time)-2],":",event_end_time[length(event_end_time)])
      event_end_time <- gsub(" ", "", event_end_time)
      event_end_time <- strptime(paste(event_date,event_end_time), "%Y-%m-%d %H:%M")
      
      # We no longer need the last line nor the first line
      doc_text[length(doc_text)] <- NA
      doc_text[1] <- NA
      doc_text <- na.omit(doc_text)  
      
      
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
      
      event_paragraph_count <- length(doc_text) - 1
      event_sentence_count <- clessnverse::countVecSentences(doc_text) - 1
      speech_paragraph_count <- 0
      
      cat("\n")
      
      for (j in 1:length(doc_text)) {
        cat(j, "\n")
        
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
          if ( grepl("modérat", tolower(paragraph_start)) ||
               grepl("président", tolower(paragraph_start)) ) { ### MODERATEUR ###
  
            speaker_first_name <- "Modérateur"
            speaker_last_name <- "Modérateur"
            speaker_gender <- NA
            speaker_is_minister <- 0
            speaker_type <- "moderator"
            speaker_party <- NA
            speaker_district <- NA
            speaker_media <- NA
            intervention_type <- "modération"
            
            if (  1 %in% match(patterns_periode_de_questions, tolower(c(paragraph_start)),FALSE) ) periode_de_questions <- TRUE
            
          } else {  ### DÉPUTÉ or JOURNALIST ###
            
            if ( stringr::str_detect(paragraph_start, "^M\\.(.*):") ||
                 stringr::str_detect(paragraph_start, "^Mme(.*):") ) {
              
              # il faut voir si c'est un homme ou une femme qui prend la parole
              if ( !stringr::str_detect(paragraph_start, "^Mme(.*):") ) {
                gender_femme <- 0
              } else { 
                gender_femme <- 1
              }
              
              # il faut voir maintenant s'il y a quelque chose entre parenthèses :
              # c'est soit la circonscription du député, soit le prénom du journaliste
              if ( !is.na(stringr::str_match(paragraph_start, "^M(.*)\\s+(.*)\\s+\\((.*)\\)(.*)?(\\s+)?:")[3]) ) {
                # We have a string of type "M. | Mme string1 (string2) :" 
                # With string2 = first name or circonscription
                speaker_last_name <- stringr::str_match(paragraph_start, "^M(.*)\\s+(.*)\\s+\\((.*)\\)(.*)?(\\s+)?:")[3]
                speaker_first_name <- stringr::str_match(paragraph_start, "^M(.*)\\s+(.*)\\s+\\((.*)\\)(.*)?(\\s+)?:")[4]
                
                # Is the first name a first name or the district within round brackets ?
                if ( nrow(dplyr::filter(dfMPs, data.currentDistrict == speaker_first_name & (type == "mp" | type == "public_service"))) > 0 ) {
                  speaker_district <- speaker_first_name
                  speaker_first_name <- NA
                  speaker <- dplyr::filter(dfMPs, (tolower(data.lastName1) == tolower(speaker_last_name) | 
                                                       tolower(data.lastName2) == tolower(speaker_last_name)) & 
                                                      tolower(data.currentDistrict) == tolower(speaker_district) & 
                                                      data.isFemale == gender_femme & 
                                                      (type == "mp" | type == "public_service"))
                } else {
                  speaker <- dplyr::filter(dfMPs, (tolower(data.lastName1) == tolower(speaker_last_name) | 
                                                       tolower(data.lastName2) == tolower(speaker_last_name)) & 
                                                      tolower(data.firstName) == tolower(speaker_first_name) & 
                                                      (type == "mp" | type == "public_service"))
                }
                
              } else {
                if ( !is.na(stringr::str_match(paragraph_start, "^M(me|\\.)\\s+((\\w+)|(\\w+-\\w+)|(\\w+\\s\\w+)|(\\w+\\'\\w+))(\\s+)?:")[3]) ) {
                  # We have a string of type "M. | Mme string :" with string = last_name
                  speaker_last_name <- stringr::str_match(paragraph_start, "^M(me|\\.)\\s+((\\w+)|(\\w+-\\w+)|(\\w+\\s\\w+)|(\\w+\\'\\w+))(\\s+)?:")[3]
                  speaker_first_name <- NA
                  ln1 <- clessnverse::splitWords(speaker_last_name)[1]
                  ln2 <- clessnverse::splitWords(speaker_last_name)[2]
                  if (is.na(ln2)) {
                    speaker <- dplyr::filter(dfMPs, (tolower(data.lastName1) == tolower(ln1) | tolower(data.lastName2) == tolower(ln1)) & data.isFemale == gender_femme)
                  } else {
                    speaker <- dplyr::filter(dfMPs, (tolower(data.lastName1) == tolower(ln1) & tolower(data.lastName2) == tolower(ln2)) & data.isFemale == gender_femme)
                  }
                  ln1 <- NA
                  ln2 <- NA
                }
              }
            } else {
              # Journalist most likely
            }
            
            if ( nrow(speaker) > 0 ) { ### DÉPUTÉ ###
                  speaker_last_name <- paste(na.omit(speaker$data.lastName1[1]), na.omit(speaker$data.lastName2[1]), sep = " ")
                  speaker_last_name <- trimws(speaker_last_name, which = c("both"))
                  if (length(speaker_last_name) == 0) speaker_last_name <- NA
                  
                  speaker_first_name <- speaker$data.firstName[1]
                  speaker_gender <- dplyr::case_when(is.na(speaker_gender) && (speaker$data.isFemale[1] == 1 || speaker$data.isFemale[1] == "1")  || gender_femme == 1 ~ "F",
                                      is.na(speaker_gender) && !(speaker$data.isFemale[1] == 0 || speaker$data.isFemale[1] == "0") || gender_femme == 0 ~ "M")
                  if ( tolower(speaker$type[1]) == "public_service" ) {
                    speaker_type <- "public_service"
                    speaker_party <- NA
                    speaker_district <- NA
                  } else {
                    speaker_type <- "mp"
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
  
            } else { ### JOURNALIST ###
              
              if ( !is.na(speaker_first_name) ){
                speaker <- dplyr::filter(dfJournalists, tolower(paste(speaker_first_name, speaker_last_name, sep = " ")) == tolower(data.fullName))
                if (nrow(speaker) == 0) {
                  speaker <- dplyr::filter(dfJournalists, tolower(paste(speaker_last_name, speaker_first_name, sep = ", ")) == tolower(data.fullName))
                }
              }
              else {
                if (!is.na(speaker_last_name))
                  speaker <- dplyr::filter(dfJournalists, stringr::str_detect(tolower(speaker_last_name), tolower(data.fullName)))
              }
              
              if ( nrow(speaker) > 0 ) {
                # we have a JOURNALIST
                
                speaker_gender <- dplyr::case_when(is.na(speaker_gender) && (speaker$data.isFemale[1] == 1 || speaker$data.isFemale[1] == "1")  || gender_femme == 1 ~ "F",
                                    is.na(speaker_gender) && !(speaker$data.isFemale[1] == 0 || speaker$data.isFemale[1] == "0") || gender_femme == 0 ~ "M")
                
                speaker_type <- "journalist"
                speaker_party <- NA
                speaker_district <- NA
                speaker_media <- speaker$data.currentMedia
              } else {
                if ( stringr::str_detect(paragraph_start, "Journaliste :(.*)") ){
                  speaker_first_name <- NA
                  speaker_last_name <- NA
                  speaker_gender <- NA
                  speaker_type <- "journalist"
                  speaker_party <- NA
                  speaker_district <- NA
                  speaker_media <- NA
                } else {
                  # ATTENTION : here we have not been able to identify
                  # Neither the moderator, nor a politician, nor a journalist
                  if (is.na(speaker_first_name)) speaker_first_name <- clessnverse::splitWords(stringr::str_match(paragraph_start, "^(.*):"))[1]
                  if (is.na(speaker_last_name)) speaker_last_name <- clessnverse::splitWords(stringr::str_match(paragraph_start, "^(.*):"))[2]
                  
                  speaker_gender <- dplyr::case_when(is.na(speaker_gender) && (speaker$data.isFemale[1] == 1 || speaker$data.isFemale[1] == "1")  || gender_femme == 1 ~ "F",
                                      is.na(speaker_gender) && !(speaker$data.isFemale[1] == 0 || speaker$data.isFemale[1] == "0") || gender_femme == 0 ~ "M")
                }
              }
              
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
            }
          } # fin  ### DÉPUTÉ or JOURNALIST ###
          
          intervention_text <- substr(doc_text[j], unlist(gregexpr(":", paragraph_start))+1, nchar(doc_text[j]))
          intervention_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", intervention_text, perl=TRUE)
          
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          if (!grepl("version non révisée", doc_text[j])) {
            intervention_text <- paste(intervention_text,"\n\n",doc_text[j], sep="")
            speech_paragraph_count <- speech_paragraph_count + 1
          }
        }
        
        language <- textcat::textcat(stringr::str_replace_all(intervention_text, "[[:punct:]]", ""))
        if ( !(language %in% c("english","french")) ) { 
          language <- "fr"
        } else {
          language <- substr(language,1,2)
        }
        
        speech_sentence_count <- clessnverse::countSentences(paste(intervention_text, collapse = ' '))
        speech_word_count <- clessnverse::countWords(intervention_text)
        speech_paragraph_count <- stringr::str_count(intervention_text, "\\n\\n")+1
        
        if (is.na(speaker_first_name) && is.na(speaker_last_name)) {
          speaker_full_name <- NA
        } else {
          speaker_full_name <- trimws(paste(na.omit(speaker_first_name), na.omit(speaker_last_name), sep = " "),which = "both")
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
                                         interventionID = paste(gsub("cp", "", event_id),intervention_seqnum,sep=''),
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
          
          v2_metadata_to_commit <- list("url"=event_url,"format"="html","location"="CA-QC")
          
          cat(intervention_seqnum, "\n")
          
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
          
        } #If the next speaker is different or if it's the last record
      } # for (j in 1:length(doc_text)) : loop back to the next intervention
      

      # Join all the elements of the character vector into a single
      # character string, separated by spaces for the simple dataSet
      collapsed_doc_text <- paste(paste(doc_text, "\n\n", sep=""), collapse = ' ')
      collapsed_doc_text <- stringr::str_replace_all(
        string = collapsed_doc_text, pattern = "\n\n NA\n\n", replacement = "")
      
      # Update the cache
      row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = event_id, eventHtml = doc_html.original, stringsAsFactors = FALSE)
      ###dfCache <- clessnverse::commitCacheRows(row_to_commit, dfCache, 'agoraplus_warehouse_cache_items', opt$cache_mode, opt$hub_mode)
 
      # Update cache dans hub v2
      v2_row_to_commit <- data.frame(eventID = event_id, rawContent = doc_html.original, stringsAsFactors = FALSE)
      v2_metadata_to_commit <- list("url"=event_url, "format"="html", "location"="CA-QC")
      
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
                                  eventURL = event_url,
                                  eventDate = as.character(event_date), 
                                  eventStartTime = as.character(event_start_time), 
                                  eventEndTime = as.character(event_end_time), 
                                  eventTitle = event_title, 
                                  eventSubtitle = event_subtitle, 
                                  eventSentenceCount = event_sentence_count,
                                  eventParagraphCount = event_paragraph_count,
                                  eventContent = collapsed_doc_text,
                                  eventTranslatedContent = NA,
                                  stringsAsFactors = FALSE)

      ###dfSimple <- clessnverse::commitSimpleRows(row_to_commit, dfSimple, 'agoraplus_warehouse_event_items', opt$simple_mode, opt$hub_mode)
      
      #clessnverse::logit(scriptname, paste("commited event", event_id, "from", event_date,"containing", intervention_seqnum, "interventions", sep=' '), logger)
      
    } # version finale
    
  } #if (grepl("actualites-salle-presse", event_url))
  
} #for (i in 1:nrow(result))

clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
