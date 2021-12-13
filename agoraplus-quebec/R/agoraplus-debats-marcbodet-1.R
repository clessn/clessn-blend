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
                         "tidytext", 
                         "tibble",
                         "devtools",
                         "textcat",
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

if (!exists("scriptname")) scriptname <- "agoraplus-debats-marcbodet-1.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit("scraper", c("file", "hub"), Sys.getenv("LOG_PATH"))

# Script command line options:
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataframe by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataframe
# - rebuild : wipes out completely the dataframe and rebuilds it from scratch
# - skip : does not make any change to the dataframe
opt <- list(cache_mode = "rebuild", simple_mode = "rebuild", deep_mode = "rebuild", 
            dataframe_mode = "rebuild", hub_mode = "skip", download_data = FALSE)

if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}


# Download HUB v2 data
if (opt$dataframe_mode %in% c("update","refresh")) {
  clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
  dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "parliament_debate_archive", schema = "v1", 
                                                               location = "CA-QC", format = "html",
                                                               download_data = opt$download_data,
                                                               token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfInterventions)) dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type = "parliament_debate_archive", schema = "v1")
  
  if (opt$download_data) { 
    dfInterventions <- dfInterventions[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location", 
                                          "metadata.parliament_number", "metadata.parliament_session", 
                                          "data.eventID", "data.eventDate", "data.eventStartTime", "data.eventEndTime", 
                                          "data.eventTitle", "data.eventSubTitle", "data.interventionSeqNum", "data.objectOfBusinessID", 
                                          "data.objectOfBusinessRubric", "data.objectOfBusinessTitle", "data.objectOfBusinessSeqNum", 
                                          "data.subjectOfBusinessID", "data.subjectOfBusinessTitle", "data.subjectOfBusinessHeader", 
                                          "data.subjectOfBusinessSeqNum", "data.subjectOfBusinessProceduralText", 
                                          "data.subjectOfBusinessTabledDocID", "data.subjectOfBusinessTabledDocTitle", 
                                          "data.subjectOfBusinessAdoptedDocID", "data.subjectOfBusinessAdoptedDocTitle", 
                                          "data.speakerID", "data.speakerFirstName", "data.speakerLastName", "data.speakerFullName", 
                                          "data.speakerGender", "data.speakerType", "data.speakerCountry", "data.speakerIsMinister", 
                                          "data.speakerParty", "data.speakerPolGroup", "data.speakerDistrict", "data.interventionID", 
                                          "data.interventionDocID", "data.interventionDocTitle", "data.interventionType", 
                                          "data.interventionLang", "data.interventionWordCount", "data.interventionSentenceCount", 
                                          "data.interventionParagraphCount", "data.interventionText", "data.interventionTextFR", 
                                          "data.interventionTextEN")]
  } else {
    dfInterventions <- dfInterventions[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location", 
                                          "metadata.parliament_number", "metadata.parliament_session")]
  }

} else {
  clessnverse::logit(scriptname, "Not retreiving interventions from hub because hub_mode is rebuild or skip", logger)
  dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type="parliament_debate_archive", schema = "v1")
}


clessnverse::loadETLRefData()


###############################################################################
# Data source
#
# connect to the dataSource : the provincial parliament web site 
# get the index page containing the URLs to all the national assembly debates
# to extract those URLS and get them individually in order to parse
# each debate
#
base_url <- "http://www.assnat.qc.ca"
content_url <- "/fr/travaux-parlementaires/journaux-debats.html"

# Pour rouler le script sur une base quotidienne et aller chercher les débats récents Utiliser le ligne ci-dessous
#doc_html <- RCurl::getURL(paste(base_url,content_url,sep=""))

# Hack here Pour obtenir l'historique des débats depuis le début de l'année 2020 enlever le commentaire dans le ligne ci-dessous
doc_html <- RCurl::getURL("file:///Users/patrick/Dev/CLESSN/clessn-blend/agoraplus-quebec/journaux-debats-12-1.html")

parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
doc_urls <- XML::xpathSApply(parsed_html, "//a/@href")
list_urls <- doc_urls[grep("assemblee-nationale/\\d\\d-\\d/journal-debats", doc_urls)]

# Hack here to scrape only one debate
#list_urls <- c("/fr/travaux-parlementaires/assemblee-nationale/42-1/journal-debats/20210330/294413.html")


###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it to extract the
# press conference content
#
for (i in 1:length(list_urls)) {
  
  #if (opt$hub_mode != "skip") clessnhub::refresh_token(configuration$token, configuration$url)
  if (opt$hub_mode != "skip") clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
  
  event_url <- paste(base_url,list_urls[i],sep="")
  event_id <- paste("dp", 
                    gsub("[[:punct:]]", "", 
                         paste(stringr::str_sub(event_url,71,74), 
                               stringr::str_sub(event_url,90,105),sep='')
                         ),
                    sep='')
  
  clessnverse::logit(scriptname, paste("Debate", i, "of", length(list_urls),sep = " "), logger)
  cat("\nDebat", i, "de", length(list_urls),"\n")
  
  
  # Make sure the data comes from the debats parlementaires (we know that from the URL)
  if (grepl("/travaux-parlementaires/assemblee-nationale/", event_url)) {     
    ###
    # If the data is not cache we get the raw html from assnat.qc.ca
    # if it is cached (we scarped it before), we prefer not to bombard
    # the website with HTTP_GET requests and ise the cached version
    ###
    # Read and parse HTML from the URL directly
    doc_html <- RCurl::getURL(event_url)
    doc_html.original <- doc_html
    doc_html <- stringr::str_replace_all(doc_html, "<a name=\"_Toc([:digit:]{8})\">([:alpha:])",
                                "<a name=\"_Toc\\1\">Titre: \\2")
    doc_html <- stringr::str_replace_all(doc_html, "<a name=\"_Toc([:digit:]{8})\"></a>\n  <a name=\"Page([:digit:]{5})\"></a>([:alpha:])",
                                "<a name=\"_Toc(\\1)\"></a>\n  <a name=\"Page\\2\"></a>Titre: \\3")
    parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
    cached_html <- FALSE
    clessnverse::logit(scriptname, paste(event_id, "not cached"), logger)

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
  
    if ( version_finale && 
         ( ((opt$simple_mode == "update" && !(event_id %in% dfSimple$eventID) ||
             opt$simple_mode == "refresh" ||
             opt$simple_mode == "rebuild") ||
            (opt$deep_mode == "update" && !(event_id %in% dfDeep$eventID) ||
             opt$deep_mode == "refresh" ||
             opt$deep_mode == "rebuild") ||
           (opt$dataframe_mode == "update" && !(event_id %in% dfInterventions$data.eventID) ||
            opt$dataframe_mode == "refresh" ||
            opt$dataframe_mode == "rebuild")) ||
         ((opt$hub_mode == "refresh" ||
             opt$hub_mode == "update") && event_id %in% dfSimple$eventID))
    ) {
      
      ###############################
      # Columns related to the event
      event_date <- NA
      event_start_time <- NA
      event_title <- NA
      event_subtitle <- NA
      event_end_time <- NA
      doc_text <- NA
      
      # Extract SourceType
      event_source_type <- doc_span[34]
      event_source_type <- gsub("\n","",event_source_type)
      event_source_type <- gsub("\r","",event_source_type)
      event_source_type <- sub("^\\s+", "", event_source_type)
      event_source_type <- sub("\\s+$", "", event_source_type)
      
      # Extract date of the conference
      date_time <- doc_h2[6]
      
      date_time <- gsub("\n","",date_time)
      date_time <- gsub("\r","",date_time)
      date_time <- sub("^\\s+", "", date_time)
      date_time <- sub("\\s+$", "", date_time)
      
      event_date <- stringr::word(date_time[1],2:5)
      event_date <- gsub(",", "", event_date)
      day_of_week <- event_date[1]
      datestr <- paste(event_date[2],months_en[match(tolower(event_date[3]),months_fr)],event_date[4])
      event_date <- as.Date(datestr, format = "%d %B %Y")
      
      # Title and subtitle of the conference  
      event_title <- doc_h1[2]
      event_title <- gsub("\n","",event_title)
      event_title <- gsub("\r","",event_title)
      event_title <- sub("^\\s+", "", event_title)
      event_title <- sub("\\s+$", "", event_title)
      
      event_subtitle <- doc_h2[6]
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

      start.index <- grep(patterns_time_digits_or_text_fr, tolower(doc_text))
      
      for (x in 0:(start.index-1)) doc_text[x] <- NA
      
      doc_text <- na.omit(doc_text)  

      # Extract start time of the conference
      if (stringr::str_detect(tolower(doc_text[1]), patterns_time_text_fr)) {
        doc_text[1] <- gsub("\\(|\\)", "", doc_text[1])
        doc_text[1] <- gsub("\\s\\s+", " ", doc_text[1])
        hour <- strsplit(doc_text[1], " ")[[1]][1]
        hour <- clessnverse::convertTextToNumberFR(hour)[[2]][1]
        if (nchar(hour) == 1) hour <- paste("0",hour,sep='')
        if (clessnverse::countWords(doc_text[1]) > 2) {
          minute <- strsplit(doc_text[1], " ")[[1]][3]
          if (strsplit(doc_text[1], " ")[[1]][4] == "et") {
            minute <- paste(minute, "et", strsplit(doc_text[1], " ")[[1]][5], sep = ' ')
          }
          minute <- clessnverse::convertTextToNumberFR(minute)[[2]][1]
          if (nchar(minute) == 1) minute <- paste("0",minute,sep='')
        } else {
          minute <- "00"
        }
        event_start_time <- paste(event_date, " ", hour,":",minute,":00",sep='')
      } 
      
      if (stringr::str_detect(tolower(doc_text[1]), patterns_time_digits)) {
        event_start_time <- doc_text[1]
        event_start_time <- gsub("\\(",'', event_start_time)
        event_start_time <- gsub("\\)",'', event_start_time)
        event_start_time <- clessnverse::splitWords(event_start_time)
        
        if ( event_start_time[length(event_start_time)] == "heures" || event_start_time[length(event_start_time)] == "h" ) {
          event_start_time[length(event_start_time)] <- ":"
          event_start_time[length(event_start_time)+1] <- "00"
        }
        
        event_start_time <- paste(event_start_time[length(event_start_time)-2],":",event_start_time[length(event_start_time)])
        event_start_time <- gsub(" ", "", event_start_time)
        event_start_time <- strptime(paste(event_date,event_start_time), "%Y-%m-%d %H:%M")
      }

      # Get rid of the first line which contained the start time
      doc_text[1] <- NA
      doc_text <- na.omit(doc_text)  

      # Figure out the end time of the conference
      event_end_time <- doc_text[which(stringr::str_detect( doc_text, "^\\(Fin de la séance à"))]
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
      
      # Remove consecutive spaces (cleaning)
      doc_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", doc_text, perl=TRUE)
    

      ####################################
      # The colums of the detailed dataset
      oob_rubric <- NA
      oob_title <- NA
      sob_title <- NA
      sob_procedural_text <- NA
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
      intervention_text <- NA
      
      parliament_number <- stringr::str_sub(event_url,71,72)
      parliament_session <- stringr::str_sub(event_url,74,74)
      
      gender_femme <- 0
      speaker <- data.frame()
      periode_de_questions <- FALSE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      intervention_seqnum <- 1
  
      event_paragraph_count <- length(doc_text) - 1
      event_sentence_count <- clessnverse::countVecSentences(doc_text) - 1
      speech_paragraph_count <- 0
      
      for (j in 1:length(doc_text)) {
        cat(j, "\r")
        
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
        if ( length(strsplit(next_paragraph_start, ":")[[1]]) == 1) {
          # There is no : in the beginning of the paragraph => it is probably a continuity of the same intervention
          next_paragraph_start <- next_paragraph_start
        } else {
          # Could be a new intervention => we take the  first token before the first : to see if that is the case
          next_paragraph_start <- paste(strsplit(next_paragraph_start, ":")[[1]][1], ":", sep='')
        }
        
        if ( grepl("(^\\•\\s\\()|(^\\()", paragraph_start) ) {
          sob_procedural_text <- doc_text[j]
          sob_procedural_text <- gsub("(^\\•\\s\\()|(^\\()|\\)\\s\\•$|^\\(|\\)$", "", sob_procedural_text)
          if (j < length(doc_text)) next
        } 
        
        if ( grepl("^Titre:", paragraph_start) ) {
          title <- trimws(gsub("^Titre:", "", doc_text[j]), "both")
          if ( TRUE %in% stringr::str_detect(title, patterns_oob_rubric) ) {
            oob_rubric <- title
          } else {
            if ( TRUE %in% stringr::str_detect(title, patterns_oob_title) ) {
              oob_title <- title
            } else {
              if ( !grepl("^M\\.|^Mme", title) ) {
                sob_title <- title
              }
            }
          }
          next
        }
        
        
        if ( TRUE %in% stringr::str_detect(paragraph_start, patterns_intervenants) &&
             !grepl(",", stringr::str_match(paragraph_start, "^(.*):")[1]) &&
             stringr::str_detect(paragraph_start, "^(.*):")
           ) {
          # It's a new person speaking
          
          # Skip if this intervention already is in the dataset and if we're not refreshing it
          matching_row <- which(dfInterventions$key == paste(event_id, intervention_seqnum, sep="-"))
          if (length(matching_row) > 0 && opt$dataframe_mode != "refresh") {
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
          
          #if ((TRUE %in% stringr::str_detect(next_paragraph_start, patterns_intervenants)) &&
          #    !grepl(",", stringr::str_match(next_paragraph_start, "^(.*):")[1]) &&
          #    stringr::str_detect(next_paragraph_start, "^(.*):") &&
          #    !is.na(doc_text[j]) || 
          #    (j == length(doc_text) && is.na(doc_text[j+1]))) {
            speaker_first_name <- NA
            speaker_last_name <- NA
            speaker_full_name <- NA
            speaker_gender <- NA
            speaker_type <- NA
            speaker_party <- NA
            speaker_district <- NA
            speaker_is_minister <- NA
            
            intervention_type <- NA
            intervention_text <- NA
            language <- NA
            sob_procedural_text <- NA
            speaker <- data.frame()
            
            speech_paragraph_count <- 1
            speech_sentence_count <- 0
            speech_word_count <- 0
          #}
          
          # let's rule out the president first
          if ( grepl("^(le|la)(\\s+)?(vice(\\s|\\-))?président(.*):", tolower(paragraph_start)) ||
               grepl("^(m.|mme)(.*)\\((président|présidente)(.*)\\)(.*):", tolower(paragraph_start))
             ) { ### MODERATEUR ###
  
            speaker_first_name <- NA
            
            # On enlève les parenthèses
            if ( grepl("Mme", paragraph_start) || grepl("M\\.", paragraph_start) ) { 
              speaker_last_name <- clessnverse::removeSpeakerTitle(stringr::str_match(paragraph_start, "\\((.*)\\)\\s+:")[2])
            }
            
            if ( grepl("^président(e?)", tolower(speaker_last_name)) ) {
              speaker_type <- speaker_last_name
              speaker_last_name <-  stringr::str_match(paragraph_start, "^(M\\.|Mme)\\s+(.*)\\s+\\((.*)\\)\\s+:")[3]
              if ( grepl("^M\\.", paragraph_start) ) {
                gender_femme <- 0
              } else {
                gender_femme <- 1
              }
              ln1 <- stringr::word(speaker_last_name, 1)
              ln2 <- stringr::word(speaker_last_name, 2)
              if (is.na(ln2)) {
                #speaker <- dplyr::filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender_femme)
                speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) | tolower(data.lastName2) == tolower(ln1)) & 
                                           data.isFemale == gender_femme & (type == "mp" | type == "public_service"))
              } else {
                #speaker <- dplyr::filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender_femme)
                speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) & tolower(data.lastName2) == tolower(ln2)) & 
                                           data.isFemale == gender_femme & (type == "mp" | type == "public_service"))
              }
              ln1 <- NA
              ln2 <- NA
            }
            
            if ( grepl("^le(\\s+)?président(.*):", tolower(paragraph_start)) ||
                 grepl("^la(\\s+)?présidente(.*):", tolower(paragraph_start)) ) {
              speaker_type <- "president"
              speaker_last_name <- "Paradis"
              speaker_first_name <- "François"
              speaker_gender <- "M"
              speaker <- dplyr::filter(dfPersons, tolower(data.firstName) == tolower(speaker_first_name) & 
                                         (tolower(data.lastName1) == tolower(speaker_last_name) | tolower(data.lastName2) == tolower(speaker_last_name)) &
                                         (type == "mp" | type == "public_service"))
              
            } else {
              if ( grepl("^le(\\s+)?vice-président(.*):", tolower(paragraph_start)) ||
                   grepl("^la(\\s+)?vice-présidente(.*):", tolower(paragraph_start)) ) {
                speaker_type <- "vice-president"
                
                if ( grepl("mme(.*):", tolower(paragraph_start)) ) gender_femme <- 1 else gender_femme <- 0
                    
                speaker_last_name <- clessnverse::removeSpeakerTitle(stringr::str_match(paragraph_start, "\\((.*)\\)\\s+:")[2])
                
                ln1 <- stringr::word(speaker_last_name, 1)
                ln2 <- stringr::word(speaker_last_name, 2)
                if (is.na(ln2)) {
                  speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) | tolower(data.lastName2) == tolower(ln1)) &
                                             data.isFemale == gender_femme & (type == "mp" | type == "public_service"))
                } else {
                  speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) & tolower(data.lastName2) == tolower(ln2)) & 
                                             data.isFemale == gender_femme & (type == "mp" | type == "public_service"))
                }
                ln1 <- NA
                ln2 <- NA
    
              }
            }
            
            if (  1 %in% match(patterns_periode_de_questions, tolower(c(paragraph_start)),FALSE) ) periode_de_questions <- TRUE
            
            if ( nrow(speaker) > 0 ) { 
              speaker_last_name <- paste(na.omit(speaker[1,]$data.lastName1), na.omit(speaker[1,]$data.lastName2), sep = " ")
              speaker_last_name <- trimws(speaker_last_name, which = c("both"))
              if (length(speaker_last_name) == 0) speaker_last_name <- NA
              speaker_first_name <- speaker$data.firstName[1]
              speaker_gender <- if ( speaker$data.isFemale[1] == 1 || speaker$data.isFemale[1] == "1") "F" else "M"
              if ( tolower(speaker$type[1]) == "public_service" ) {
                speaker_type <- "public_service"
                speaker_party <- NA
                speaker_district <- NA
              } else {
                if (is.na(speaker_type) || speaker_type == "" || length(speaker_type) == 0) speaker_type <- "mp"
                speaker_party <- speaker[1,]$data.currentParty
                speaker_district <- speaker[1,]$data.currentDistrict
              }
              
              speaker_is_minister <- speaker$data.isMinister[1]
            } # ( nrow(speaker) > 0 ) 
            
            intervention_type <- "modération"
            intervention_text <- substr(doc_text[j], unlist(gregexpr(":", paragraph_start))+1, nchar(doc_text[j]))
            intervention_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", intervention_text, perl=TRUE)
            
            
          } else {  ### DÉPUTÉ ou SECRÉTAIRE ###
            gender_femme <- 0
            
            if ( grepl("Mme", stringr::str_match(paragraph_start, "(.+)\\s+:")[1]) || 
                 grepl("M\\.", stringr::str_match(paragraph_start, "(.+)\\s+:")[1]) ) {
              
              if ( grepl("\\(", stringr::str_match(paragraph_start, "(.+)\\s+:")[1]) ) {
                # Is the last name or speaker district in parenthesis
                district_or_lastname <- clessnverse::removeSpeakerTitle(stringr::str_match(paragraph_start, "\\((.*)\\)")[2])
                
                # Check if it is a real district or if it is his last name that was between ()
                if (district_or_lastname %in% dfPersons$data.currentDistrict) {
                  speaker_district <- district_or_lastname
                } else {
                  speaker_first_name <- district_or_lastname
                }
                
                if ( grepl("Mme", stringr::str_match(paragraph_start, "(.+)\\s+:"))[1] ) {
                  speaker_last_name <- stringr::str_match(paragraph_start, "Mme\\s+(.*?)\\s+\\(")[2]
                  gender_femme <- 1
                }
                else {
                  speaker_last_name <- stringr::str_match(paragraph_start, "M\\.\\s+(.*?)\\s+\\(")[2]
                  gender_femme <- 0
                }
                
              } else {
                
                if ( grepl("Mme", stringr::str_match(paragraph_start, "(.+)\\s+:"))[1] ) {
                  speaker_last_name <- stringr::str_match(paragraph_start, "Mme\\s+(.*?)\\s+:")[2]
                  gender_femme <- 1
                } else {
                  speaker_last_name <- stringr::str_match(paragraph_start, "M\\.\\s+(.*?)\\s+:")[2]
                  gender_femme <- 0
                }
              }
              speaker_last_name <- sub("^\\s+", "", speaker_last_name)
              speaker_last_name <- sub("\\s+$", "", speaker_last_name)
              
              ln1 <- stringr::word(speaker_last_name, 1)
              ln2 <- stringr::word(speaker_last_name, 2)
              
              #speaker_first_name <- NA
              
              if ( is.na(speaker_district) ) {
               if (is.na(ln2)) {
                  speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) | tolower(data.lastName2) == tolower(ln1)) & 
                                             data.isFemale == gender_femme & (type == "mp" | type == "public_service"))
                } else {
                  speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) & tolower(data.lastName2) == tolower(ln2)) & 
                                             data.isFemale == gender_femme & (type == "mp" | type == "public_service"))
                }
              } else {
                if (is.na(ln2)) {
                  speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) | tolower(data.lastName2) == tolower(ln1)) & 
                                             tolower(data.currentDistrict) == tolower(speaker_district) & (type == "mp" | type == "public_service"))
                } else {
                  speaker <- dplyr::filter(dfPersons, (tolower(data.lastName1) == tolower(ln1) & tolower(data.lastName2) == tolower(ln2)) & 
                                             tolower(data.currentDistrict) == tolower(speaker_district) & (type == "mp" | type == "public_service"))
                }
              }
              
              ln1 <- NA
              ln2 <- NA
            }
            
            if ( nrow(speaker) > 0 ) { 
                  # we have a politician
                  speaker_last_name <- paste(na.omit(speaker[1,]$data.lastName1), na.omit(speaker[1,]$data.lastName2), sep = " ")
                  speaker_last_name <- trimws(speaker_last_name, which = c("both"))
                  if (length(speaker_last_name) == 0) speaker_last_name <- NA
                  speaker_first_name <- speaker[1,]$data.firstName
                  speaker_gender <- if ( speaker[1,]$data.isFemale == 1 ) "F" else "M"
                  if ( tolower(speaker[1,]$type) == "public_service" ) {
                    speaker_type <- "public_service"
                    speaker_party <- NA
                    speaker_district <- NA
                  } else {
                    speaker_type <- "mp"
                    speaker_party <- speaker[1,]$data.currentParty
                    speaker_district <- speaker[1,]$data.currentDistrict
                  }
                  
                  speaker_is_minister <- speaker$data.isMinister[1]
            } else {
              # ATTENTION : here we have not been able to identify
              # Neither the moderator, nor a politician, nor a journalist
              if (is.na(speaker_first_name)) speaker_first_name <- clessnverse::splitWords(stringr::str_match(paragraph_start, "^(.*):"))[1]
              if (is.na(speaker_last_name)) speaker_last_name <- clessnverse::splitWords(stringr::str_match(paragraph_start, "^(.*):"))[2]
            } # ( nrow(speaker) > 0 ) 

                        
            if (j == 1) {
              intervention_type <- "allocution"
            } else {
              if ( periode_de_questions || substr(doc_text[j-1], nchar(doc_text[j-1]), nchar(doc_text[j-1])) == "?" ) {
                intervention_type <- "réponse"
              } else {
                intervention_type <- "allocution"
              }
            }
          }

            
          if ( grepl("secrétaire", tolower(paragraph_start)) ) {
            speaker_first_name <- NA
            speaker_last_name <- NA
            speaker_gender <- NA
            speaker_type <- "secretary or vice-secretary"
            speaker_party <- NA
            speaker_district <- NA
            speaker <- data.frame()
            intervention_type <- "modération"
          }
          
          if ( grepl("voix\\s:", tolower(paragraph_start)) ) {
            speaker_first_name <- "Des"
            speaker_last_name <- "Voix"
            speaker_gender <- NA
            gender_femme <- 0
          }
          
          intervention_text <- substr(doc_text[j], unlist(gregexpr(":", paragraph_start))+1, nchar(doc_text[j]))
          intervention_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", intervention_text, perl=TRUE)
          
          
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          if (is.na(intervention_text)) {
            intervention_text <- ""
            intervention_text <- paste(intervention_text, doc_text[j], sep="")
          } else {
            intervention_text <- paste(intervention_text,"\n\n",doc_text[j], sep="")
          }
          speech_paragraph_count <- speech_paragraph_count + 1
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
                                         objectOfBusinessRubric = oob_rubric,
                                         objectOfBusinessTitle = oob_title,
                                         objectOfBusinessSeqNum = NA,
                                         subjectOfBusinessID = NA,
                                         subjectOfBusinessTitle = sob_title,
                                         subjectOfBusinessHeader = NA,
                                         subjectOfBusinessSeqNum = NA,
                                         subjectOfBusinessProceduralText = sob_procedural_text,
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
                                         interventionID = paste(gsub("dp", "", event_id),intervention_seqnum,sep=''),
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
          
          v2_metadata_to_commit <- list("url"=event_url, "format"="html", "location"="CA-QC",
                                     "parliament_number"=parliament_number, "parliament_session"=parliament_session)

          dfInterventions <- clessnverse::commitAgoraplusInterventions(dfDestination = dfInterventions, 
                                                                       type = "parliament_debate", schema = "v2",
                                                                       metadata = v2_metadata_to_commit,
                                                                       data = v2_row_to_commit,
                                                                       opt$dataframe_mode, opt$hub_mode)
          
          intervention_seqnum <- intervention_seqnum + 1
          matching_row <- NULL
          
          sob_procedural_text <- NA
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
      
      clessnverse::logit(scriptname, paste("commited event", event_id, "from", event_date,"containing", intervention_seqnum, "interventions", sep=' '), logger)
      
    } # version finale
    
  } #if (grepl("actualites-salle-presse", event_url))
  
} #for (i in 1:nrow(result))

clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
