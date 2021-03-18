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
      devtools::install_github(new_packages[p], upgrade = "never", quiet =TRUE, build = FALSE)
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

if (!exists("scriptname")) scriptname <- "agoraplus-debats.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, "file", Sys.getenv("LOG_PATH"))

# opt <- list(cache_update = "update",simple_update = "update",deep_update = "update",
#             hub_update = "update",csv_update = "skip",backend_type = "HUB")

# Pour la PROD
# Sys.setenv(HUB_URL = "https://clessn.apps.valeria.science")
# Sys.setenv(HUB_USERNAME = "patrickponcet")
# Sys.setenv(HUB_PASSWORD = "s0ci4lResQ")

#Pour le DEV
# Sys.setenv(HUB_URL = "https://dev-clessn.apps.valeria.science")
# Sys.setenv(HUB_USERNAME = "test")
# Sys.setenv(HUB_PASSWORD = "soleil123")

if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (opt$backend_type == "HUB") 
  clessnverse::loadAgoraplusHUBDatasets("quebec", opt, 
                                        Sys.getenv('HUB_USERNAME'), 
                                        Sys.getenv('HUB_PASSWORD'), 
                                        Sys.getenv('HUB_URL'))

if (opt$backend_type == "CSV")
  clessnverse::loadAgoraplusCSVDatasets("quebec", opt, "../clessn-blend/_SharedFolder_clessn-blend/data/")

# Load all objects used for ETL
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
doc_html <- getURL(paste(base_url,content_url,sep=""))

# Hack here Pour obtenir l'historique des débats depuis le début de l'année 2020 enlever le commentaire dans le ligne ci-dessous
#doc_html <- getURL("file:///Users/patrick/Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/JournalDebatsHistorique2020.html")

parsed_html <- htmlParse(doc_html, asText = TRUE)
doc_urls <- xpathSApply(parsed_html, "//a/@href")
list_urls <- doc_urls[grep("assemblee-nationale/42-1/journal-debats", doc_urls)]









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
  if (opt$backend_type == "HUB") clessnhub::refresh_token(configuration$token, configuration$url)
  current_url <- paste(base_url,list_urls[i],sep="")
  current_id <- str_replace_all(list_urls[i], "[[:punct:]]", "")
  
  clessnverse::logit(paste("Debate", i, "of", length(list_urls),sep = " "), logger)
  cat("\nDebat", i, "de", length(list_urls),"\n")
  
  
  # Make sure the data comes from the debats parlementaires (we know that from the URL)
  if (grepl("/travaux-parlementaires/assemblee-nationale/", current_url)) {     
    ###
    # If the data is not cache we get the raw html from assnat.qc.ca
    # if it is cached (we scarped it before), we prefer not to bombard
    # the website with HTTP_GET requests and ise the cached version
    ###
    if ( !(current_id %in% dfCache$eventID) ) {
      # Read and parse HTML from the URL directly
      doc_html <- getURL(current_url)
      doc_html.original <- doc_html
      doc_html <- str_replace_all(doc_html, "<a name=\"_Toc([:digit:]{8})\">([:alpha:])",
                                  "<a name=\"_Toc\\1\">Titre : \\2")
      doc_html <- str_replace_all(doc_html, "<a name=\"Page([:digit:]{5})\"></a>([:alpha:])",
                                  "<a name=\"Page\\1\"></a>Titre : \\2")
      parsed_html <- htmlParse(doc_html, asText = TRUE)
      cached_html <- FALSE
      clessnverse::logit(paste(current_id, "not cached"), logger)
    } else{ 
      # Retrieve the XML structure from dfCache and Parse
      doc_html <- dfCache$eventHtml[which(dfCache$eventID==current_id)]
      doc_html.original <- doc_html
      doc_html <- str_replace_all(doc_html, "<a name=\"_Toc([:digit:]{8})\">([:alpha:])",
                                  "<a name=\"_Toc\\1\">Titre : \\2")
      doc_html <- str_replace_all(doc_html, "<a name=\"Page([:digit:]{5})\"></a>([:alpha:])",
                                  "<a name=\"Page\\1\"></a>Titre : \\2")
      parsed_html <- htmlParse(doc_html, asText = TRUE)
      cached_html <- TRUE
      clessnverse::logit(paste(current_id, "cached"), logger)
    }
    
    # Dissect the text based on html tags
    doc_h1 <- xpathApply(parsed_html, '//h1', xmlValue)
    doc_h2 <- xpathApply(parsed_html, '//h2', xmlValue)
    doc_h3 <- xpathApply(parsed_html, '//h3', xmlValue)
    doc_span <- xpathApply(parsed_html, '//span', xmlValue)
    doc_span <- unlist(doc_span)
    
    # Valide la version : préliminaire ou finale
    if ( length(grep("version finale", tolower(doc_h2))) > 0 ) {
      version_finale <- TRUE
      clessnverse::logit("version finale", logger)
      cat("version finale")
    }
    else {
      version_finale <- FALSE
      clessnverse::logit("version préliminaire", logger)
      cat("version préliminaire")
    }
  
    if ( version_finale && 
         ( ((opt$simple_update == "update" && !(current_id %in% dfSimple$eventID) ||
             opt$simple_update == "refresh" ||
             opt$simple_update == "rebuild") ||
            (opt$deep_update == "update" && !(current_id %in% dfDeep$eventID) ||
             opt$deep_update == "refresh" ||
             opt$deep_update == "rebuild")) ||
           ((opt$hub_update == "refresh" ||
             opt$hub_update == "update") && current_id %in% dfSimple$eventID))
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
      current_source <- doc_span[34]
      current_source <- gsub("\n","",current_source)
      current_source <- gsub("\r","",current_source)
      current_source <- sub("^\\s+", "", current_source)
      current_source <- sub("\\s+$", "", current_source)
      
      # Extract date of the conference
      date_time <- doc_h2[6]
      
      date_time <- gsub("\n","",date_time)
      date_time <- gsub("\r","",date_time)
      date_time <- sub("^\\s+", "", date_time)
      date_time <- sub("\\s+$", "", date_time)
      
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
      
      # Title and subtitle of the conference  
      title <- doc_h1[2]
      title <- gsub("\n","",title)
      title <- gsub("\r","",title)
      title <- sub("^\\s+", "", title)
      title <- sub("\\s+$", "", title)
      
      subtitle <- doc_h2[6]
      subtitle <- gsub("\n","",subtitle)
      subtitle <- gsub("\r","",subtitle)
      subtitle <- sub("^\\s+", "", subtitle)
      subtitle <- sub("\\s+$", "", subtitle)
      
        
      # Extract all the paragraphs (HTML tag is p, starting at
      # the root of the document). Unlist flattens the list to
      # create a character vector.
      doc_text <- unlist(xpathApply(parsed_html, '//p', xmlValue))
      doc_text <- gsub('\u00a0',' ', doc_text)
      
      # Replace all \n by spaces and clean leading and trailing spaces
      # and clean the conference vector of unneeded paragraphs
      
      doc_text <- gsub('\\n',' ', doc_text)
      doc_text <- sub("^\\s+", "", doc_text)
      doc_text <- sub("\\s+$", "", doc_text)

      start.index <- grep("ajournement", tolower(doc_text))[2]
      
      for (x in 0:start.index+1) doc_text[x] <- NA
      
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
      
      gender_femme <- 0
      speaker <- data.frame()
      periode_de_questions <- FALSE
      
      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      seqnum <- 1
  
      event_paragraph_count <- length(doc_text) - 1
      event_sentence_count <- clessnverse::countVecSentences(doc_text) - 1

      # pb_chap <- utils::txtProgressBar(min = 0,      # Minimum value of the progress bar
      #                                  max = length(doc_text), # Maximum value of the progress bar
      #                                  style = 3,    # Progress bar style (also available style = 1 and style = 2)
      #                                  width = 80,  # Progress bar width. Defaults to getOption("width")
      #                                  char = "=")   # Character used to create the bar      
      
      for (j in 1:length(doc_text)) {
        # setTxtProgressBar(pb_chap, j)
        cat(j, "\r")
        
        # Skip if this intervention already is in the dataset
        if (nrow(dfDeep[dfDeep$eventID == current_id & dfDeep$interventionSeqNum == seqnum,]) > 0 &&
            opt$deep_update != "refresh") {
          seqnum <- seqnum+1
          next
        }
        
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
              grepl("^une\\svoix(.*?):", tolower(intervention_start)) ||
              grepl("^des\\svoix(.*?):", tolower(intervention_start))) &&
             !grepl(",", str_match(intervention_start, "^(.*):")) ) {
          # It's a new person speaking
          first_name <- NA
          last_name <- NA
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
          
          #cat("New person", doc_text[j], "\n")
          
          # let's rule out the president first
          if ( grepl("président(.*):", tolower(intervention_start)) ) { ### MODERATEUR ###
  
            first_name <- NA
            
            # On enlève les parenthèses
            if ( grepl("Mme", intervention_start) || grepl("M\\.", intervention_start) ) 
              last_name <- clessnverse::removeSpeakerTitle(str_match(intervention_start, "\\((.*)\\)\\s+:")[2])
            
            if ( grepl("le président(.*):", tolower(intervention_start)) ||
                 grepl("la présidente(.*):", tolower(intervention_start)) ) {
              type <- "président(e)"
              last_name <- "Paradis"
              first_name <- "François"
              gender <- "M"
              #speaker <- subset(deputes, grepl(paste(tolower(first_name),tolower(last_name),sep=" "),tolower(nom)))
              speaker <- dplyr::filter(deputes, tolower(firstName) == tolower(first_name) & (tolower(lastName1) == tolower(last_name) | tolower(lastName2) == tolower(last_name)))
              
            } else {
              if ( grepl("le vice-président(.*):", tolower(intervention_start)) ||
                   grepl("la vice-présidente(.*):", tolower(intervention_start)) ) {
                type <- "vice-président(e)"
                
                if ( grepl("mme(.*):", tolower(intervention_start)) )
                  gender_femme <- 1
                else 
                  gender_femme <- 0
                    
                last_name <- clessnverse::removeSpeakerTitle(str_match(intervention_start, "\\((.*)\\)\\s+:")[2])
                
                #speaker <- subset(deputes, grepl(tolower(last_name), tolower(nom)) & grepl(gender_femme, femme))
                ln1 <- word(last_name, 1)
                ln2 <- word(last_name, 2)
                if (is.na(ln2)) {
                  speaker <- dplyr::filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender_femme)
                } else {
                  speaker <- dplyr::filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender_femme)
                }
                ln1 <- NA
                ln2 <- NA
    
              }
            }
            
            media <- NA
            
            if (  1 %in% match(patterns_periode_de_questions, tolower(c(intervention_start)),FALSE) )
              periode_de_questions <- TRUE
            
            if ( nrow(speaker) > 0 ) { 
              last_name <- paste(na.omit(speaker[1,]$lastName1), na.omit(speaker[1,]$lastName2), sep = " ")
              last_name <- trimws(last_name, which = c("both"))
              if (length(last_name) == 0) last_name <- NA
              first_name <- speaker[1,]$firstName
              gender <- if ( speaker[1,]$isFemale == 1) "F" else "M"
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
            } # ( nrow(speaker) > 0 ) 
            
            speech_type <- "modération"
            speech <- substr(doc_text[j], unlist(gregexpr(":", intervention_start))+1, nchar(doc_text[j]))
            speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
            
            
          } else {  ### DÉPUTÉ ou SECRÉTAIRE ###
            gender_femme <- 0
            
            if ( grepl("Mme", str_match(intervention_start, "(.+)\\s+:")[1]) || 
                 grepl("M\\.", str_match(intervention_start, "(.+)\\s+:")[1]) ) {
              
              if ( grepl("\\(", str_match(intervention_start, "(.+)\\s+:")[1]) ) {
                
                circ <- clessnverse::removeSpeakerTitle(str_match(intervention_start, "\\((.*)\\)")[2])
                
                if ( grepl("Mme", str_match(intervention_start, "(.+)\\s+:"))[1] ) {
                  last_name <- str_match(intervention_start, "Mme\\s+(.*?)\\s+\\(")[2]
                  gender_femme <- 1
                }
                else {
                  last_name <- str_match(intervention_start, "M\\.\\s+(.*?)\\s+\\(")[2]
                  gender_femme <- 0
                }
                
              } else {
                
                if ( grepl("Mme", str_match(intervention_start, "(.+)\\s+:"))[1] ) {
                  last_name <- str_match(intervention_start, "Mme\\s+(.*?)\\s+:")[2]
                  gender_femme <- 1
                }
                else {
                  last_name <- str_match(intervention_start, "M\\.\\s+(.*?)\\s+:")[2]
                  gender_femme <- 0
                }
                
              }
              last_name <- sub("^\\s+", "", last_name)
              last_name <- sub("\\s+$", "", last_name)
              
              ln1 <- word(last_name, 1)
              ln2 <- word(last_name, 2)
              
              first_name <- NA
              
              if ( is.na(circ) ) {
               if (is.na(ln2)) {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) | tolower(lastName2) == tolower(ln1)) & isFemale == gender_femme)
                } else {
                  speaker <- filter(deputes, (tolower(lastName1) == tolower(ln1) & tolower(lastName2) == tolower(ln2)) & isFemale == gender_femme)
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
                  last_name <- paste(na.omit(speaker[1,]$lastName1), na.omit(speaker[1,]$lastName2), sep = " ")
                  last_name <- trimws(last_name, which = c("both"))
                  if (length(last_name) == 0) last_name <- NA
                  first_name <- speaker[1,]$firstName
                  gender <- if ( speaker[1,]$isFemale == 1 ) "F" else "M"
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
            } else {
              # ATTENTION : here we have not been able to identify
              # Neither the moderator, nor a politician, nor a journalist
              if (is.na(first_name)) first_name <- words(str_match(intervention_start, "^(.*):"))[1]
              if (is.na(last_name)) last_name <- words(str_match(intervention_start, "^(.*):"))[2]
            } # ( nrow(speaker) > 0 ) 

                        
            if (j == 1)
              speech_type <- "allocution"
            else
              if ( periode_de_questions || substr(doc_text[j-1], nchar(doc_text[j-1]), nchar(doc_text[j-1])) == "?" ) 
                speech_type <- "réponse"
              else
                speech_type <- "allocution"
          }

            
          if ( grepl("titre :", tolower(intervention_start)) ) {
            first_name <- NA
            last_name <- NA
            gender <- NA
            type <- NA
            party <- NA
            circ <- NA
            media <- NA
            speaker <- data.frame()
            speech_type <- "titre"
            speech <- stringr::str_match(speech, "^Titre\\s:\\s(.*)")[2]
          }
          
            
          if ( grepl("secrétaire", tolower(intervention_start)) ) {
            first_name <- NA
            last_name <- NA
            gender <- NA
            type <- "secrétaire ou secrétaire adjoint(e)"
            party <- NA
            circ <- NA
            media <- NA
            speaker <- data.frame()
            speech_type <- "modération"
          }
          
          if ( grepl("voix\\s:", tolower(intervention_start)) ) {
            first_name <- "Des"
            last_name <- "Voix"
            gender <- NA
            gender_femme <- 0
          }
          
          speech <- substr(doc_text[j], unlist(gregexpr(":", intervention_start))+1, nchar(doc_text[j]))
          speech <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", speech, perl=TRUE)
          
          
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          speech <- paste(speech,"\n\n",doc_text[j], sep="")
          speech_paragraph_count <- speech_paragraph_count + 1
        }
        
        language <- textcat(str_replace_all(speech, "[[:punct:]]", ""))
        if ( !(language %in% c("english","french")) ) { 
          language <- NA
        }
        else language <- substr(language,1,2)

        speech_sentence_count <- clessnverse::countSentences(paste(speech, collapse = ' '))
        speech_word_count <- length(words(removePunctuation(paste(speech, collapse = ' '))))
        speech_paragraph_count <- str_count(speech, "\\n\\n")+1

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
              !grepl(",", str_match(substr(doc_text[j+1],1,40), "^(.*):"))) &&
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
          
          dfDeep <- clessnverse::commitDeepRows(row_to_commit, dfDeep, 'agoraplus_warehouse_intervention_items', opt$deep_update, opt$hub_update)
  
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
      collapsed_doc_text <- str_replace_all(
        string = collapsed_doc_text, pattern = "\n\n NA\n\n", replacement = "")
      
      # Update the cache
      row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = current_id, eventHtml = doc_html, stringsAsFactors = FALSE)
      dfCache <- clessnverse::commitCacheRows(row_to_commit, dfCache, 'agoraplus_warehouse_cache_items', opt$cache_update, opt$hub_update)
      
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
      
      dfSimple <- clessnverse::commitSimpleRows(row_to_commit, dfSimple, 'agoraplus_warehouse_event_items', opt$simple_update, opt$hub_update)
      
    } # version finale
    
  } #if (grepl("actualites-salle-presse", current_url))
  
} #for (i in 1:nrow(result))




if (opt$csv_update != "skip" && opt$backend_type == "CSV") { 
  write.csv2(dfCache, file=paste(base_csv_folder,"dfCacheAgoraPlus.csv",sep=''), row.names = FALSE)
  write.csv2(dfDeep, file = paste(base_csv_folder,"dfDeepAgoraPlus.csv",sep=''), row.names = FALSE)
  write.csv2(dfSimple, file = paste(base_csv_folder,"dfSimpleAgoraPlus.csv",sep=''), row.names = FALSE)
}

clessnverse::logit(paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
