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
      devtools::install_github(new_packages[p], upgrade = "never", quiet =FALSE, build = FALSE)
      
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

if (!exists("scriptname")) scriptname <- "agoraplus-confpresse.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, "file", Sys.getenv("LOG_PATH"))

#opt <- list(cache_mode = "update",simple_mode = "update",deep_mode = "update", 
#            dataframe_mode = "update", hub_mode = "update")


if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (opt$backend_type == "HUB") 
  clessnverse::loadAgoraplusHUBDatasets("quebec", opt, 
                                        Sys.getenv('HUB_USERNAME'), 
                                        Sys.getenv('HUB_PASSWORD'), 
                                        Sys.getenv('HUB_URL'))


# Load all objects used for ETL
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
data <- xml2::read_html(paste(base_url,content_url,sep=""))
urls <- rvest::html_nodes(data, 'li.icoHTML a')

# To obtain the list of conferences available in the FIRST search results page (default page)
list_urls <- rvest::html_attr(urls, 'href')

# Hack here to focus only on one press conf :
#list_urls <-c("/fr/actualites-salle-presse/conferences-points-presse/ConferencePointPresse-70135.html")











###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it o extract the
# press conference content
#



for (i in 1:20) {
  if (opt$backend_type == "HUB") clessnhub::refresh_token(configuration$token, configuration$url)
  current_url <- paste(base_url,list_urls[i],sep="")
  current_id <- stringr::str_replace_all(list_urls[i], "[[:punct:]]", "")
  
  clessnverse::logit(paste("Conf", i, "de", length(list_urls),sep = " "), logger)
  cat("\nConf", i, "de", length(list_urls),"\n")
  

  # Make sure the data comes from the pres conf (we know that from the URL)
  if (grepl("actualites-salle-presse", current_url)) {     
    # If the data is not cache we get the raw html from assnat.qc.ca
    # if it is cached (we scarped it before), we prefer not to bombard
    # the website with HTTP_GET requests and ise the cached version
    if ( !(current_id %in% dfCache$eventID) ) {
      # Read and parse HTML from the URL directly
      doc_html <- getURL(current_url)
      parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
      cached_html <- FALSE
      clessnverse::logit(paste(current_id, "not cached"), logger)
    } else{ 
      # Retrieve the XML structure from dfCache and Parse
      doc_html <- dfCache$eventHtml[which(dfCache$eventID==current_id)]
      parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
      cached_html <- TRUE
      clessnverse::logit(paste(current_id, "cached"), logger)
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
      clessnverse::logit("version finale", logger)
      cat("version finale")
    } else {
      version_finale <- FALSE
      clessnverse::logit("version préliminaire", logger)
      cat("version préliminaire")
    }
  
    #if ( version_finale && 
    if (( ((opt$simple_mode == "update" && !(current_id %in% dfSimple$eventID) ||
           opt$simple_mode == "refresh" ||
           opt$simple_mode == "rebuild") ||
          (opt$deep_mode == "update" && !(current_id %in% dfDeep$eventID) ||
           opt$deep_mode == "refresh" ||
           opt$deep_mode == "rebuild")) ||
         ((opt$hub_mode == "refresh" ||
           opt$hub_mode == "update") && current_id %in% dfSimple$eventID)
      )) {
      
      ###############################
      # Columns of the simple dataset
      date <- NA
      time <- NA
      title <- NA
      subtitle <- NA
      end_time <- NA
      doc_text <- NA
      
      # Extract SourceType    
      current_source <- doc_span[32]
      current_source <- gsub("\n","",current_source)
      current_source <- gsub("\r","",current_source)
      current_source <- sub("^\\s+", "", current_source)
      current_source <- sub("\\s+$", "", current_source)
      
      # Extract date of the conference
      date_time <- doc_h3[6]
      date <- word(date_time[1],2:5)
      date <- gsub(",", "", date)
      day_of_week <- date[1]
      datestr <- paste(date[2],months_en[match(tolower(date[3]),months_fr)],date[4])
      date <- as.Date(datestr, format = "%d %B %Y")
      
      # Extract start time of the conference
      time <- word(date_time[1],6:8)
      if (time[3] == "") time[3] <- "00"
      time <- paste(time[1], ":", time[3])
      time <- gsub(" ", "", time)
      time <- strptime(paste(date,time), "%Y-%m-%d %H:%M")
      
      serial <- (time)
      
      # Title and subtitle of the conference  
      title <- doc_h1[2]
      title <- gsub("\n","",title)
      title <- gsub("\r","",title)
      title <- sub("^\\s+", "", title)
      title <- sub("\\s+$", "", title)
      
      subtitle <- doc_h2[4]
      subtitle <- gsub("\n","",subtitle)
      subtitle <- gsub("\r","",subtitle)
      subtitle <- sub("^\\s+", "", subtitle)
      subtitle <- sub("\\s+$", "", subtitle)
      
        
      # Extract all the paragraphs (HTML tag is p, starting at
      # the root of the document). Unlist flattens the list to
      # create a character vector.
      doc_text <- unlist(XML::xpathApply(parsed_html, '//p', xmlValue))
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
      doc_text[5] <- NA
      doc_text <- na.omit(doc_text)  
      
      # Figure out the end time of the conference
      end_time <- doc_text[length(doc_text)]
      end_time <- gsub("\\(",'', end_time)
      end_time <- gsub("\\)",'', end_time)
      end_time <- words(end_time)
      
      if ( end_time[length(end_time)] == "heures" ) {
        end_time[length(end_time)] <- ":"
        end_time[length(end_time)+1] <- "00"
      }
      
      end_time <- paste(end_time[length(end_time)-2],":",end_time[length(end_time)])
      end_time <- gsub(" ", "", end_time)
      end_time <- strptime(paste(date,end_time), "%Y-%m-%d %H:%M")
      
      # We no longer need the last line
      doc_text[length(doc_text)] <- NA
      
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
      speech_type <- NA
      speech <- NA
      
      speaker <- data.frame()
      periode_de_questions <- FALSE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      seqnum <- 1
      event_paragraph_count <- length(doc_text) - 1
      event_sentence_count <- clessnverse::countVecSentences(doc_text) - 1
      
      #pb_chap <- utils::txtProgressBar(min = 0,      # Minimum value of the progress bar
      #                                 max = length(doc_text), # Maximum value of the progress bar
      #                                 style = 3,    # Progress bar style (also available style = 1 and style = 2)
      #                                 width = 80,  # Progress bar width. Defaults to getOption("width")
      #                                 char = "=")   # Character used to create the bar      
      
      for (j in 1:length(doc_text))  {
        #setTxtProgressBar(pb_chap, j)
        cat(j, "\r")
        
        # Skip if this intervention already is in the dataset
        #if (nrow(dfDeep[dfDeep$eventID == current_id & dfDeep$interventionSeqNum == seqnum,]) > 0 &&
        #    opt$deep_mode != "refresh") {
        #  seqnum <- seqnum+1
        #  next
        #}
        
        # Is this a new speaker taking the stand?  If so there is typically a : at the begining of the sentence
        # And the Sentence starts with the Title (M. Mme etc) and the last name of the speaker
        speech_paragraph_count <- 0
        intervention_start <- substr(doc_text[j],1,40)
  
        if ( (grepl("^M\\.\\s+(.*?)\\s+:", intervention_start) || 
              grepl("^Mme\\s+(.*?)\\s+:", intervention_start) || 
              grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?)\\s+:", intervention_start) ||
              grepl("^Titre(.*?):", intervention_start) ||
              grepl("^Journaliste(.*?):", intervention_start) ||
              grepl("^Modérat(.*?):", intervention_start) ||
              grepl("^Une\\svoix(.*?):", intervention_start) ||
              grepl("^Des\\svoix(.*?):", intervention_start)) &&
             !grepl(",", stringr::str_match(intervention_start, "^(.*):")) &&
             !grepl("cette transcription est une version préliminaire", tolower(doc_text[j])) ) {
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
          
          gender_femme <- 0
          speaker <- data.frame()
          
          speech_paragraph_count <- 1
          speech_sentence_count <- 0
          speech_word_count <- 0
          
          # let's rule out the moderator first
          if ( grepl("modérat", tolower(intervention_start)) ||
               grepl("président", tolower(intervention_start)) ) { ### MODERATEUR ###
  
            first_name <- "Modérateur"
            last_name <- "Modérateur"
            gender <- NA
            type <- "modérateur"
            party <- NA
            circ <- NA
            media <- NA
            speech_type <- "modération"
            speech <- substr(doc_text[j], unlist(gregexpr(":", intervention_start))+1, nchar(doc_text[j]))
            speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
            
            if (  1 %in% match(patterns_periode_de_questions, tolower(c(intervention_start)),FALSE) )
              periode_de_questions <- TRUE
            
          } else {  ### DÉPUTÉ or JOURNALIST ###
            
            if ( stringr::str_detect(intervention_start, "^M\\.(.*):") ||
                 stringr::str_detect(intervention_start, "^Mme(.*):") ) {
              # il faut voir maintenant s'il y a quelque chose entre parenthèses :
              # c'est soit la circonscription du député, soit le prénom du journaliste
              if ( !stringr::str_detect(intervention_start, "^Mme(.*):") ) {
                gender_femme <- 0
              } else { 
                gender_femme <- 1
              }
              
              if ( !is.na(stringr::str_match(intervention_start, "^M(.*)\\s+(.*)\\s+\\((.*)\\)\\s+:")[3]) ) {
                # We have a string of type "M. | Mme string1 (string2) :" avec string 2 = 
                # first name or circonscription
                last_name <- stringr::str_match(intervention_start, "^M(.*)\\s+(.*)\\s+\\((.*)\\)\\s+:")[3]
                first_name <- stringr::str_match(intervention_start, "^M(.*)\\s+(.*)\\s+\\((.*)\\)\\s+:")[4]
                
                # Is the first name a first name or the circonscription?
                if ( nrow(filter(deputes, currentDistrict == first_name)) > 0 ) {
                  circ <- first_name
                  first_name <- NA
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(last_name) | tolower(lastName2) == tolower(last_name)) & tolower(currentDistrict) == tolower(circ) & isFemale == gender_femme)
                } else {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(last_name) | tolower(lastName2) == tolower(last_name)) & tolower(firstName) == tolower(first_name))
                }
                
              } else {
                if ( !is.na(stringr::str_match(intervention_start, "^M(me|\\.)\\s+((\\w+)|(\\w+-\\w+)|(\\w+\\'\\w+))\\s+:")[3]) ) {
                  # We have a string of type "M. | Mme string :" with string = last_name
                  last_name <- stringr::str_match(intervention_start, "^M(me|\\.)\\s+((\\w+)|(\\w+-\\w+)|(\\w+\\'\\w+))\\s+:")[3]
                  first_name <- NA
                  ln1 <- word(last_name, 1)
                  ln2 <- word(last_name, 2)
                  if (is.na(ln2)) {
                    speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender_femme)
                  } else {
                    speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender_femme)
                  }
                  ln1 <- NA
                  ln2 <- NA
                }
              }
            } else {
              # Journalist most likely
            }
            
            if ( nrow(speaker) > 0 ) { ### DÉPUTÉ ###
                  ##cat("we have a politician", last_name, "\n")
              
                  last_name <- paste(na.omit(speaker[1,]$lastName1), na.omit(speaker[1,]$lastName2), sep = " ")
                  last_name <- trimws(last_name, which = c("both"))
                  if (length(last_name) == 0) last_name <- NA
                  first_name <- speaker[1,]$firstName
                  #gender <- if ( is.na(gender) && speaker[1,]$isFemale ) "F" else "M"
                  #gender <- case_when(is.na(gender) && speaker[1,]$isFemale  || gender_femme == 1 ~ "F",
                  #                    is.na(gender) && !speaker[1,]$isFemale || gender_femme == 0 ~ "M")
                  gender <- case_when(is.na(gender) && speaker$isFemale[1] == 1  || gender_femme == 1 ~ "F",
                                      is.na(gender) && !speaker$isFemale[1] == 0 || gender_femme == 0 ~ "M")
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
                  
                  if (j == 1)
                    speech_type <- "allocution"
                  else
                    if ( periode_de_questions || substr(doc_text[j-1], nchar(doc_text[j-1]), nchar(doc_text[j-1])) == "?" ) 
                      speech_type <- "réponse"
                  else
                      speech_type <- "commentaire"
  
            } else { ### JOURNALIST ###
              
              if ( !is.na(first_name) ){
                speaker <- filter(journalists, tolower(paste(first_name, last_name, sep = " ")) == tolower(fullName))
              }
              else{
                if (!is.na(last_name))
                  speaker <- filter(journalists, stringr::str_detect(tolower(last_name), tolower(fullName)))
              }
              
              if ( nrow(speaker) > 0 ) {
                # we have a JOURNALIST
                
                #gender <- case_when(is.na(gender) && speaker[1,]$isFemale  || gender_femme == 1 ~ "F",
                #                    is.na(gender) && !speaker[1,]$isFemale || gender_femme == 0 ~ "M")
                gender <- case_when(is.na(gender) && speaker$isFemale[1] == 1  || gender_femme == 1 ~ "F",
                                    is.na(gender) && !speaker$isFemale[1] == 0 || gender_femme == 0 ~ "M")
                
                #gender <- if ( is.na(gender) && speaker[1,]$isFemale ) "F" else "M"
                type <- "journaliste"
                party <- NA
                circ <- NA
                media <- speaker[1,]$source
              } else {
                if ( stringr::str_detect(intervention_start, "Journaliste :(.*)") ){
                  first_name <- NA
                  last_name <- NA
                  gender <- NA
                  type <- "journaliste"
                  party <- NA
                  circ <- NA
                  media <- NA
                } else {
                  # ATTENTION : here we have not been able to identify
                  # Neither the moderator, nor a politician, nor a journalist
                  if (is.na(first_name)) first_name <- words(stringr::str_match(intervention_start, "^(.*):"))[1]
                  if (is.na(last_name)) last_name <- words(stringr::str_match(intervention_start, "^(.*):"))[2]
                  #gender <- case_when(is.na(gender) && speaker[1,]$isFemale  || gender_femme == 1 ~ "F",
                  #                    is.na(gender) && !speaker[1,]$isFemale || gender_femme == 0 ~ "M")
                  gender <- case_when(is.na(gender) && speaker$isFemale[1] == 1  || gender_femme == 1 ~ "F",
                                      is.na(gender) && !speaker$isFemale[1] == 0 || gender_femme == 0 ~ "M")
                }
              }
              
              if ( periode_de_questions ) speech_type <- "question"
              else
              if ( grepl("?",doc_text[j]) ) speech_type <- "question" else speech_type <- "commentaire"
            }
            
            
            speech <- substr(doc_text[j], unlist(gregexpr(":", intervention_start))+1, nchar(doc_text[j]))
            speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
            
          }
          
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          if (!grepl("version non révisée", doc_text[j])) {
            speech <- paste(speech,"\n\n",doc_text[j], sep="")
            speech_paragraph_count <- speech_paragraph_count + 1
          }
        }
        
        language <- textcat(stringr::str_replace_all(speech, "[[:punct:]]", ""))
        if ( !(language %in% c("english","french")) ) { 
          language <- NA
        }
        else language <- substr(language,1,2)
        
        speech_sentence_count <- clessnverse::countSentences(paste(speech, collapse = ' '))
        speech_word_count <- length(words(removePunctuation(paste(speech, collapse = ' '))))
        speech_paragraph_count <- stringr::str_count(speech, "\\n\\n")+1
        
        if (is.na(first_name) && is.na(last_name)) 
          full_name <- NA
        else 
          full_name <- trimws(paste(na.omit(first_name), na.omit(last_name), sep = " "),which = "both")
        
        # If the next speaker is different or if it's the last record, then let's commit this observation into the dataset  
        if ( ((grepl("^M\\.\\s+(.*?)\\s+:", substr(doc_text[j+1],1,40)) || 
               grepl("^Mme\\s+(.*?)\\s+:", substr(doc_text[j+1],1,40)) || 
               grepl("^(Le|La)\\s+(Modérat.*?|Président.*?|Vice-Président.*?)\\s+:", substr(doc_text[j+1],1,40)) ||
               grepl("^Titre(.*?):", substr(doc_text[j+1],1,40)) ||
               grepl("^Journaliste(.*?):", substr(doc_text[j+1],1,40)) ||
               grepl("^Modérat(.*?):", substr(doc_text[j+1],1,40)) ||
               grepl("^Une\\svoix(.*?):", substr(doc_text[j+1],1,40)) ||
               grepl("^Des\\svoix(.*?):", substr(doc_text[j+1],1,40))) &&
              !grepl(",", stringr::str_match(substr(doc_text[j+1],1,40), "^(.*):"))) &&
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
      } # for (j in 1:length(doc_text)) : loop back to the next intervention
      

      # Join all the elements of the character vector into a single
      # character string, separated by spaces for the simple dataSet
      collapsed_doc_text <- paste(paste(doc_text, "\n\n", sep=""), collapse = ' ')
      collapsed_doc_text <- stringr::str_replace_all(
        string = collapsed_doc_text, pattern = "\n\n NA\n\n", replacement = "")
      
      
      # Update the cache
      row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = current_id, eventHtml = doc_html, stringsAsFactors = FALSE)
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
                                  eventContent = collapsed_doc_text,
                                  eventTranslatedContent = NA,
                                  stringsAsFactors = FALSE)

      dfSimple <- clessnverse::commitSimpleRows(row_to_commit, dfSimple, 'agoraplus_warehouse_event_items', opt$simple_mode, opt$hub_mode)
      
    } # version finale
    
  } #if (grepl("actualites-salle-presse", current_url))
  
} #for (i in 1:nrow(result))


clessnverse::logit(paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
