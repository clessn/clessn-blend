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
                         "countrycode",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r",
                         "ropensci/gender",
                         "lmullen/genderdata")
  
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


safe_GET <- purrr::safely(httr::GET)



###############################################################################
#   Globals
#
#   scriptname
#   logger
#
#installPackages()
library(dplyr)

status <- 0
debate_count <- 0
intervention_count <- 0
final_message <- ""

if (!exists("scriptname")) scriptname <- "parliament_mash_europe"

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

# Script command line options:
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataframe by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataframe
# - rebuild : wipes out completely the dataframe and rebuilds it from scratch
# - skip : does not make any change to the dataframe
#opt <- list(dataframe_mode = "rebuild", hub_mode = "update", download_data = FALSE, translate=TRUE)
#opt <- list(dataframe_mode = "rebuild", log_output = c("console"), hub_mode = "update", download_data = FALSE, translate=TRUE)


if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

# Download HUB v2 data
if (opt$dataframe_mode %in% c("update","refresh")) {
  clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
  dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "parliament_debate", schema = "v2", 
                                                               location = "EU", format = "html",
                                                               download_data = opt$download_data,
                                                               token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfInterventions)) dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type = "parliament_debate", schema = "v2")
  
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
  dfInterventions <- clessnverse::createAgoraplusInterventionsDf(type="parliament_debate", schema = "v2")
}


# Download v3 MPs information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(institution="European Parliament")
filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=metadata_filter)  
dfPersons <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

# Load all objects used for ETL including V1 HUB MPs
clessnverse::loadETLRefData()
dfCountryLanguageCodes <- clessnverse::loadCountryLanguageCodes(path = '/clessn-blend/_SharedFolder_clessn-blend/data', file = 'countrylanguagecodes-eu.csv', token  = Sys.getenv('DROPBOX_TOKEN'))

intervention_types_no_translate <- c("green deal", "de facto", "made in europe", "fake news", "on behalf of", "member of", "rapporteur", "author", "business as usual", "blue card", "blue-card")
intervention_types_manual_translate  <- c("predsedajúci", "ombudsman", "skriftlig", "schriftelijk", "par écrit", "kirjalikult", "european ombudsman", "namens de", "u ime kluba", "président", "president", "písemně", "membre de la commission", "au nom du groupe", "przewodnicząca", "elnök", "för", "im namen der", "der präsident", "en nombre del grupo", "în numele grupului", "a nome del gruppo", "em nome do grupo", "réponse «carton bleu»", "question «carton bleu»", "pregunta de «tarjeta azul»", "respuesta de «tarjeta azul»", "die präsidentin", "autor", '"blauwe kaart"-antwoord', "pytanie zadane przez podniesienie niebieskiej kartki", "per iscritto", "schriftlich", "rakstiski", "pisno", "na piśmie", "írásban", "pitanje postavljeno podizanjem plave kartice", "în scris", "písomne", "kirjallinen", "por escrito", "в писмена форма", "za skupinu", "verfasser", "auteure", "w imieniu grupy", "raštu", "Πρόεδρο", "γραπτώςς", 'risposta a una domanda "cartellino blu"', 'antwort auf eine frage nach dem verfahren der „blauen karte“', "întrebare adresată conform procedurii „cartonașului albastru”", "γραπτώς", "ερώτηση με γαλάζια κάρτα", "blåt-kort-spørgsmål", "blåt-kort-svar", "πρόεδρος", "puhemies", "berichterstatter",'réponse "carton bleu"', 'question "carton bleu"', '“blauwe kaart”-vraag', '“blauwe kaart”-antwoord', "thar ceann an Ghrúpa", "członek Komisji", 'resposta segundo o procedimento "cartão azul"', 'pergunta segundo o procedimento "cartão azul"')
intervention_types_manual_translated <- c("president", "Ombudsman", "In Writing", "In Writing", "In Writing", "In Writing", "European Ombudsman", "On Behalf Of The", "On Behalf Of The", "President", "President", "In Writing", "Member of the Commission", "On Behalf Of The", "President", "President", "On Behalf Of The", "On Behalf Of The", "President", "On Behalf Of The", "On Behalf Of The", "On Behalf Of The", "On Behalf Of The", "Blue Card Answer", "Blue Card Question", "Blue Card Question", "Blue Card Answer", "President", "Author", "Blue Card Answer", "Blue Card Question", "In Writing", "In Writing", "In Writing", "In Writing", "In Writing", "In Writing", "Blue Card Question", "In Writing", "In Writing", "In Writing", "In Writing", "In Writing", "On Behalf Of The", "Author", "Author", "On Behalf Of The", "In Writing", "President", "In Writing", "Blue Card Answer", "Blue Card Answer", "Blue Card Question", "In Writing", "Blue Card Question", "Blue Card Question", "Blue Card Answer", "President", "President", "President", "Blue Card Answer", "Blue Card Question", "Blue Card Question", "Blue Card Answer", "On Behalf Of The", "Member of the Commission", "Blue Card Answer", "Blue Card Question")


###############################################################################
# Data source
#
# connect to the dataSource : the provincial parliament web site 
# get the index page containing the URLs to all the national assembly debates
# to extract those URLS and get them individually in order to parse
# each debate
#

#scraping_method <- "DateRange"
scraping_method <- "FrontPage"

#start_date <- "2019-12-01"
start_date <- "2022-01-27"
#num_days <- as.integer(as.Date(Sys.time()) - as.Date(start_date))
num_days <- 31
start_parliament <- 9
num_parliaments <- 1


if (scraping_method == "FrontPage") {
  base_url <- "https://www.europarl.europa.eu"
  content_url <- "/plenary/en/debates-video.html#sidesForm"
  
  #source_page <- xml2::read_html(paste(base_url,content_url,sep=""))
  source_page <- NULL
  i_get_attempt <- 1

  while (i_get_attempt < 20 && is.null(source_page)){
    tryCatch(
      {
        source_page <- httr::GET(paste(base_url,content_url,sep=""))
      },
      error = function(e) {
        clessnverse::logit(scriptname, paste("cannot get", paste(base_url,content_url,sep=""), "index web page"), logger)
        status <<- 1
        if (final_message == "") {
          final_message <- paste("cannot get", paste(base_url,content_url,sep=""), "index web page")
        } else {
          final_message <- paste(final_message, "\n", paste("cannot get", paste(base_url,content_url,sep=""), "index web page"))
        }
        quit(status=status)
      },
      finally={
        i_get_attempt <- i_get_attempt + 1
      }
    )
  }
  source_page_html <- XML::htmlParse(source_page, useInternalNodes = TRUE)

  urls <- XML::xpathSApply(source_page_html, "//a/@href")
  #urls <- rvest::html_nodes(source_page, 'a')
  
  urls <- urls[grep("\\.xml", urls)]
  urls <- urls[grep("https", urls)]
  
  #urls_list <- rvest::html_attr(urls, 'href')

  urls_list <- as.list(urls)
}

if (scraping_method == "DateRange") {
  date_vect <- seq( as.Date(start_date), by=1, len=num_days)
  
  urls_list <- list()
  nb_deb <- 0
  
  for (d in date_vect) {
    for (p in c(start_parliament:(start_parliament+num_parliaments))) {
      url <- paste("https://www.europarl.europa.eu/doceo/document/CRE-", p, "-", as.character(as.Date(d, "1970-01-01")), "_EN.xml",sep= '')
      r <- httr::GET(url)
      if (r$status_code == 200) {
        urls_list <- c(urls_list, url)
        nb_deb <- nb_deb + 1
      }
    }
  }
}


clessnverse::logit(scriptname = scriptname, message = paste("list of urls containing", length(urls_list), "debates"), logger = logger)


if (length(urls_list) == 0) stop("Exiting normally because no new debate has been found")
# Hack here to use another data source
#urls_list <- urls_list[1:8]
#urls_list[9] <- "https://www.europarl.europa.eu/doceo/document/CRE-9-2020-02-10_FR.xml" 
#urls_list[10] <- "https://www.europarl.europa.eu/doceo/document/CRE-9-2020-02-11_FR.xml"
#urls_list[11] <- "https://www.europarl.europa.eu/doceo/document/CRE-9-2020-02-12_FR.xml"
#urls_list <- list("https://www.europarl.europa.eu/doceo/document/CRE-9-2020-02-12_FR.xml",
#               "https://www.europarl.europa.eu/doceo/document/CRE-9-2020-06-18_FR.xml",
#               "https://www.europarl.europa.eu/doceo/document/CRE-9-2020-10-19_FR.xml",
#               "https://www.europarl.europa.eu/doceo/document/CRE-9-2021-01-20_FR.xml")


###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the assnat website and 
# start parsing it to extract the debates content
#
for (i in 1:length(urls_list)) {
  event_url <- urls_list[[i]]
  event_id <- stringr::str_match(event_url, "\\CRE-(.*)\\.")[2]
  event_id <- stringr::str_replace_all(event_id, "[[:punct:]]", "")
  
  parliament_number <- stringr::str_match(event_url, "CRE-(.*)-((?:19|20)[0-9]{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])(.*)$")[2]
  
  clessnverse::logit(scriptname, paste("Debate", i, "of", length(urls_list), sep = " "), logger)
  clessnverse::logit(scriptname, event_url, logger)

  event_check <- NULL
  tryCatch(
    {event_check <- clessnhub::get_item('agoraplus_interventions', paste(event_id, "-1", sep=""))},
    error = function(e) {event_check <- NULL}, 
    finally = {}
  )

  if (!is.null(event_check) && stringr::str_detect(event_check$key, event_id) ||
    TRUE %in% stringr::str_detect(dfInterventions$key, event_id)) {
    clessnverse::logit(scriptname, paste("Debate", i, "of", length(urls_list), "event_id=", event_id, "ALREADY EXISTS.  Skipping...", sep = " "), logger)
    next
  } else {
    debate_count <- debate_count + 1 
  }


  i_get_attempt <- 1
  r <- NULL
  while(i_get_attempt < 20 && is.null(r)) {
    tryCatch(
      {
        r <- httr::GET(event_url)
      },
      error = function(e) {
        clessnverse::logit(scriptname,paste("cannot get", event_url, "event web page"), logger)
        status <- 1
        if (final_message == "") {
          final_message <- paste("cannot get", event_url, "event web page")
        } else {
          final_message <- paste(final_message, "\n", paste("cannot get", event_url, "event web page"))
        }
      },
      finally = {
        i_get_attempt <- i_get_attempt + 1
      }
    )
  }

  if (r$status_code == 200) {
    doc_html <- httr::content(r)
    doc_xml <- XML::xmlTreeParse(doc_html, useInternalNodes = TRUE)
    top_xml <- XML::xmlRoot(doc_xml)
    head_xml <- top_xml[[1]]
    core_xml <- top_xml[[2]]
    clessnverse::logit(scriptname, event_id, logger)
  } else {
    clessnverse::logit(scriptname,paste("error", r$status_code, "getting", event_url, "event web page", logger))
    status <- 2
    if (final_message == "") {
      final_message <- paste("cannot get", event_url, "event web page")
    } else {
      final_message <- paste(final_message, "\n", paste("cannot get", event_url, "event web page"))
    }
    next
  }

  # Get the length of all branches of the XML document
  core_xml_nbchapters <- length(names(core_xml))

    
  ###############################
  # Columns of the simple dataset
  #event_source_type <- xpathApply(source_page_html, '//title', XML::xmlValue)[[1]]
  event_source_type <- "Débats et vidéos | Plénière | Parlement européen"
  event_url <- event_url
  
  event_date <- substr(XML::xmlGetAttr(core_xml[[2]][["TL-CHAP"]], "VOD-START"),1,10)
  if (length(event_date) == 0) event_date <- substr(XML::xmlGetAttr(core_xml[[2]][["NUMERO"]], "VOD-START"),1,10)

  event_start_time <- substr(XML::xmlGetAttr(core_xml[[2]][["TL-CHAP"]], "VOD-START"),12,19)
  if (length(event_start_time) == 0) event_start_time <- substr(XML::xmlGetAttr(core_xml[[2]][["NUMERO"]], "VOD-START"),12,19)

  event_end_time <- substr(XML::xmlGetAttr(core_xml[[core_xml_nbchapters]][["TL-CHAP"]], "VOD-END"),12,19)
  if (length(event_end_time) == 0) event_end_time <- substr(XML::xmlGetAttr(core_xml[[core_xml_nbchapters]][["NUMERO"]], "VOD-END"),12,19)
  
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
  full_name_native <- NA
  speaker_gender <- NA
  speaker_is_minister <- NA
  speaker_type <- NA
  speaker_party <- NA
  speaker_polgroup <- NA
  speaker_district <- NA
  speaker_country <- NA
  speaker_media <- NA
  intervention_type <- NA
  intervention_lang <- NA
  intervention_word_count <- NA
  intervention_sentence_count <- NA
  intervention_paragraph_count <- NA
  intervention_text <- NA
  intervention_translated_text <- NA
  
  dfSpeaker <- data.frame()

  ########################################################
  # Go through the xml document chapter by chapter
  # and strip out any relevant info
  intervention_seqnum <- 0
  
  #pb_chap <- txtProgressBar(min = 0,      # Minimum value of the progress bar
  #                        max = core_xml_nbchapters, # Maximum value of the progress bar
  #                        style = 3,    # Progress bar style (also available style = 1 and style = 2)
  #                        width = 80,   # Progress bar width. Defaults to getOption("width")
  #                        char = "=")   # Character used to create the bar
  
  event_content <- ""
  event_translated_content <- ""
  
  current_speaker_full_name <- ""
  
  for (j in 1:core_xml_nbchapters) {
    #cat(" chapter progress\r")
    #setTxtProgressBar(pb_chap, j)
    
    # New chapter
    chapter_node <- core_xml[[j]]
    chapter_number <- XML::xmlGetAttr(chapter_node, "NUMBER")
    chapter_title <- XML::xmlValue(XML::xpathApply(chapter_node, "//CHAPTER/TL-CHAP[@VL='EN']")[j])
    
    if (!is.null(chapter_node[["TL-CHAP"]][[2]])) {
      # Here there is a URL within the title of the chapter => most likely a linked document 
      chapter_tabled_docid <- stringr::str_split(XML::xmlGetAttr(chapter_node[["TL-CHAP"]][[2]], "redmap-uri"), "/")[[1]][3]
      
      tabled_document_url <- paste("https://www.europarl.europa.eu/doceo/document/", chapter_tabled_docid, "_EN.html", sep = "")
      tabled_document_html <- RCurl::getURL(tabled_document_url)
      tabled_document_html_table <- XML::readHTMLTable(tabled_document_html)
      
      list1_id <- grep("Texts adopted", tabled_document_html_table)[1]
      list2_id <- grep("Texts adopted", tabled_document_html_table[[list1_id]])[2]
      chapter_adopted_docid <- tabled_document_html_table[[list1_id]][[list2_id]][which(grepl("Texts adopted",tabled_document_html_table[[list1_id]][[list2_id]]))]
      if (!is.null(chapter_adopted_docid)) {
        chapter_adopted_docid <- stringr::str_split(chapter_adopted_docid, " ")[[1]][length(stringr::str_split(chapter_adopted_docid, " ")[[1]])]
      } else {
        chapter_adopted_docid <- NA
      }
      
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
          
          speaker_first_name <- NA
          speaker_last_name <- NA
          speaker_full_name <- NA
          full_name_native <- NA
          speaker_gender <- NA
          speaker_is_minister <- NA
          speaker_type <- NA
          speaker_mepid <- NA
          speaker_party <- NA
          speaker_polgroup <- NA
          speaker_district <- NA
          speaker_country <- NA
          speaker_media <- NA
          chapter_tabled_docid <- NA
          chapter_adopted_docid <- NA
          intervention_type <- NA
          intervention_lang <- NA
          intervention_word_count <- NA
          intervention_sentence_count <- NA
          intervention_paragraph_count <- NA
          intervention_text <- ""
          intervention_translated_text <- NA      
          
          dfSpeaker <- data.frame()
          df_persons_row <- 0
          add_speaker_to_the_hub <- FALSE
          update_speaker_in_the_hub <- FALSE
          
          # Strip out the speaker info
          intervention_node <- core_xml[[j]][[k]]
          speaker_node <- intervention_node[["ORATEUR"]]
          
          speaker_full_name <- XML::xmlGetAttr(speaker_node, "LIB")
          speaker_full_name <- gsub("\u00a0", "", speaker_full_name)
          speaker_full_name <- stringr::str_to_title(speaker_full_name)
          speaker_last_name <- trimws(stringr::str_split(speaker_full_name, "\\|")[[1]][[2]], "both")
          speaker_first_name <- trimws(stringr::str_split(speaker_full_name, "\\|")[[1]][[1]], "both")
          speaker_full_name <- trimws(stringr::str_remove(speaker_full_name, "\\|\\s"))
          speaker_full_name <- stringr::str_squish(speaker_full_name)
  
          speaker_gender <- paste("", gender::gender(clessnverse::splitWords(speaker_first_name)[1])$gender, sep = "")
          if ( speaker_gender == "" ) speaker_gender <- NA
          
          #Get the speaker from the hub
          #If not, get it from the parliament search 
          #If not use the data in the xml structure without properties such as political party etc
          
          dfSpeaker <- dfPersons[which(tolower(dfPersons$data.fullName) == tolower(speaker_full_name)),]
          df_persons_row <- which(tolower(dfPersons$data.fullName) == tolower(speaker_full_name))
          
          # If we did not find the speaker fullname in the hub, we can try looking for the native spelling of his name also
          if ( nrow(dfSpeaker) == 0 ) {
            dfSpeaker <- dfPersons[which(tolower(dfPersons$data.fullNameNative) == tolower(speaker_full_name)),]
            df_persons_row <- which(tolower(dfPersons$data.fullNameNative) == tolower(speaker_full_name))
            keep_full_name_native <- FALSE
          } else {
            keep_full_name_native <- TRUE
          }
        
          if (nrow(dfSpeaker) == 0) {
            # Not found in hub
            clessnverse::logit(scriptname = scriptname, message = paste("searching for", speaker_full_name, "in the parliament database"), logger = logger)

            dfSpeaker <- clessnverse::getEuropeMepData(speaker_full_name)
            
            add_speaker_to_the_hub <- FALSE
            update_speaker_in_the_hub <- FALSE
            
            if (is.na(dfSpeaker$mepid)) {
              # Not found in the parliament database
              # Not much to do...
              clessnverse::logit(scriptname = scriptname, message = paste("could not find", speaker_full_name, "in the parliament database"), logger = logger)

              speaker_country <- countrycode::codelist$country.name.en[which(countrycode::codelist$iso2c == XML::xmlGetAttr(speaker_node, "LG"))]
              if (length(speaker_country) == 0) speaker_country <- NA
              
              speaker_party <- case_when(XML::xmlGetAttr(speaker_node, "PP") != "NULL" ~  XML::xmlGetAttr(speaker_node, "PP"), TRUE ~ NA_character_)
              speaker_mepid <- case_when(XML::xmlGetAttr(speaker_node, "MEPID") != "0" ~  XML::xmlGetAttr(speaker_node, "MEPID"), TRUE ~ NA_character_)
              
              speaker_polgroup <- NA
              
              if (!is.null(XML::xmlGetAttr(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]], "NAME")) && 
                  XML::xmlGetAttr(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]], "NAME")=="I") {
                if ( grepl("winner|presidente?|president,\\s|president-in-office|chancellor|president\\sof|minister|his\\sholiness|secretary|king|ombudsman|chair\\sof\\sthe", clessnverse::rm_accent(tolower(XML::xmlValue(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]])))) ) {
                  speaker_type <- XML::xmlValue(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]])
                } else {
                  speaker_polgroup <- XML::xmlValue(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]])
                }
              }
              
              full_name_native <- NA
              dfSpeaker <- data.frame(mepid=speaker_mepid, fullname=speaker_full_name, country=speaker_country, polgroup=speaker_polgroup, party=speaker_party)

              add_speaker_to_the_hub <- TRUE
              
            } else {
              #### found in the parliament database ####
              #### Add it to the hub!
              add_speaker_to_the_hub <- TRUE
              clessnverse::logit(scriptname = scriptname, message = paste("found", speaker_full_name, "in the parliament database"), logger = logger)
              full_name_native <- stringr::str_to_title(dfSpeaker$fullname)
            } #is.na(dfSpeaker$mepid)
            
          } else {
            # Found in hub => convert data structure to match as if we went to find it from parliament web site
            # There might be more that one row in the result because of the html scraper
            # We'll take the one the most complete
            fullest_row <- min(rowSums(is.na(dfSpeaker)))
            fullest_row <- which(rowSums( is.na(dfSpeaker)) == fullest_row)
            if (length(fullest_row) > 1) fullest_row <- fullest_row[1]
            dfSpeaker <- dfSpeaker[fullest_row,]
            
            if (keep_full_name_native) full_name_native <- dfSpeaker$data.fullNameNative else full_name_native <- dfSpeaker$data.fullName
            
            names(dfSpeaker)[names(dfSpeaker)=="key"] <- "mepid"
            names(dfSpeaker)[names(dfSpeaker)=="data.currentParty"] <- "party"
            names(dfSpeaker)[names(dfSpeaker)=="data.fullName"] <- "fullname"
            names(dfSpeaker)[names(dfSpeaker)=="data.currentPolGroup"] <- "polgroup"
            names(dfSpeaker)[names(dfSpeaker)=="metadata.country"] <- "country"
            
            add_speaker_to_the_hub <- FALSE
            update_speaker_in_the_hub <- FALSE
            
            # Check if the data is complete - update the hub from the parliament database if not
            if ( is.na(dfSpeaker$party) || is.na(dfSpeaker$polgroup) || is.na(dfSpeaker$country) || dfSpeaker$fullname != dfSpeaker$data.fullNameNative ) {
              # We have an incomplete MP record - try and complete it from the parliament DB
              dfSpeakerCheck <- clessnverse::getEuropeMepData(speaker_full_name)

              if (!is.na(dfSpeakerCheck$polgroup) && dfSpeakerCheck$polgroup == "Renew Europe Group") dfSpeakerCheck$polgroup <- "Group Renew Europe"
              
              if ( sum(colSums(is.na(dfSpeakerCheck))) == ncol(dfSpeakerCheck) ) {
                # could not find anything better on the parliament web site
              } else {
                if ( dfSpeakerCheck$country != dfSpeaker$country || dfSpeakerCheck$polgroup != dfSpeaker$polgroup || dfSpeakerCheck$party != dfSpeaker$party ) {
                  update_speaker_in_the_hub <- TRUE
                  speaker_key <- dfSpeaker$mepid
                  dfSpeaker <- dfSpeakerCheck
                  dfSpeaker$mepid <- speaker_key
                } else {
                  update_speaker_in_the_hub <- FALSE
                }
              }
            }
          } #if (nrow(dfSpeaker) == 0) {
          
          #dfSpeaker$fullname <- stringr::str_to_title(dfSpeaker$fullname)
          
          if (update_speaker_in_the_hub || add_speaker_to_the_hub) {
            person_metadata_row <- list("source"="https://www.europarl.europa.eu/meps/fr/download/advanced/xml?name=",
                                        "country"=dfSpeaker$country,
                                        "institution"="European Parliament",
                                        "province_or_state"=dfSpeaker$country,
                                        "twitterAccountHasBeenScraped"="0")
            
            person_data_row <- list("fullName"=dfSpeaker$fullname,
                                    "fullNameNative" = full_name_native,
                                    "isFemale"= if (!is.na(speaker_gender) && !is.null(speaker_gender) && speaker_gender=="female") as.character(1) else as.character(0),
                                    "lastName"=speaker_last_name,
                                    "firstName"=speaker_first_name,
                                    "twitterID"=NA_character_,
                                    "isMinister"="0",
                                    "twitterName"=NA_character_,
                                    "currentParty"=dfSpeaker$party,
                                    "twitterHandle"=NA_character_,
                                    "currentMinister"=NA_character_,
                                    "currentPolGroup"=dfSpeaker$polgroup,
                                    "twitterLocation"=NA_character_,
                                    "twitterPostsCount"=NA_character_,
                                    "twitterProfileURL"=NA_character_,
                                    "twitterListedCount"=NA_character_,
                                    "twitterFriendsCount"=NA_character_,
                                    "currentFunctionsList"=NA_character_,
                                    "twitterFollowersCount"=NA_character_,
                                    "currentProvinceOrState"=dfSpeaker$country,
                                    "twitterAccountVerified"=NA_character_,
                                    "twitterProfileImageURL"=NA_character_,
                                    "twitterAccountCreatedAt"=NA_character_,
                                    "twitterAccountCreatedOn"=NA_character_,
                                    "twitterAccountProtected"=NA_character_,
                                    "twitterProfileBannerURL"=NA_character_,
                                    "twitterUpdateDateStamps"=NA_character_,
                                    "twitterUpdateTimeStamps"=NA_character_)
            
            
            if (add_speaker_to_the_hub) {
              speaker_key <- if (!is.na(dfSpeaker$mepid)) paste("EU-",dfSpeaker$mepid,sep='') else paste("EU-",digest::digest(speaker_full_name),sep='')
              clessnverse::logit(scriptname=scriptname, message=paste("adding", speaker_full_name,  "/",full_name_native, "-", speaker_key, "to the hub"), logger = logger)
              clessnverse::logit(scriptname=scriptname, message="speaker metadata:", logger = logger)
              clessnverse::logit(scriptname=scriptname, message=paste(person_metadata_row, collapse = " * "), logger = logger)
              clessnverse::logit(scriptname=scriptname, message="speaker data:", logger = logger)
              clessnverse::logit(scriptname=scriptname, message=paste(person_data_row, collapse = " * "), logger = logger)
              clessnverse::logit(scriptname=scriptname, message="\n", logger = logger)
              if (opt$hub_mode != "skip") clessnhub::create_item("persons", speaker_key, "mp", "v3", person_metadata_row, person_data_row)
              names(person_metadata_row) <- paste("metadata.", names(person_metadata_row),sep='')
              names(person_data_row) <- paste("data.", names(person_data_row),sep='')
              dfPersons <- dfPersons %>% rbind(cbind(data.frame(key=speaker_key, type="mp", schema="v3", uuid=""), as.data.frame(person_metadata_row), as.data.frame(person_data_row)))
            }
            
            if (update_speaker_in_the_hub) {
              speaker_key <- dfSpeaker$mepid

              clessnverse::logit(scriptname=scriptname, message=paste("updating", speaker_full_name, "/",full_name_native, "-", speaker_key, "in the hub"), logger = logger)
              clessnverse::logit(scriptname=scriptname, message="speaker metadata:", logger = logger)
              clessnverse::logit(scriptname=scriptname, message=paste(person_metadata_row, collapse = " * "), logger = logger)
              clessnverse::logit(scriptname=scriptname, message="speaker data:", logger = logger)
              clessnverse::logit(scriptname=scriptname, message=paste(person_data_row, collapse = " * "), logger = logger)
              clessnverse::logit(scriptname=scriptname, message="\n", logger = logger)
              if (opt$hub_mode != "skip") clessnhub::edit_item("persons", speaker_key, "mp", "v3", person_metadata_row, person_data_row)
              names(person_metadata_row) <- paste("metadata.", names(person_metadata_row),sep='')
              names(person_data_row) <- paste("data.", names(person_data_row),sep='')
              #dfPersons[df_persons_row,] <- cbind(data.frame(key=dfPersons$key[df_persons_row], type=dfPersons$type[df_persons_row], schema=dfPersons$schema[df_persons_row], dfPersons$uuid[df_persons_row]), as.data.frame(person_metadata_row), as.data.frame(person_data_row))
              dfPersons <- dfPersons %>% rows_update(cbind(data.frame(key=dfPersons$key[df_persons_row], 
                                                                      type=dfPersons$type[df_persons_row], 
                                                                      schema=dfPersons$schema[df_persons_row],
                                                                      uuid=dfPersons$uuid[df_persons_row]), 
                                                                      as.data.frame(person_metadata_row),
                                                                      as.data.frame(person_data_row)), by = "key")
            }
            
          } #if (update_speaker_in_the_hub || add_speaker_to_the_hub)
          
          if (!is.na(dfSpeaker$mepid)) {
            speaker_mepid <- dfSpeaker$mepid
            speaker_party <- dfSpeaker$party
            speaker_polgroup <- dfSpeaker$polgroup
            speaker_country <- dfSpeaker$country
          } else {
            speaker_mepid <- NA
            speaker_party <- case_when(XML::xmlGetAttr(speaker_node, "PP") != "NULL" ~  XML::xmlGetAttr(speaker_node, "PP"),
                                      TRUE ~ NA_character_)
            speaker_polgroup <- NA
            speaker_country <- countrycode::codelist$country.name.en[which(countrycode::codelist$iso2c == XML::xmlGetAttr(speaker_node, "LG"))]
            if (length(speaker_country) == 0) speaker_country <- NA
          }
          
          if (!is.na(speaker_polgroup) && stringr::str_detect(speaker_polgroup, ":")) speaker_polgroup <- trimws(stringr::str_split(speaker_polgroup, ":")[[1]][2])
          
          intervention_type <- trimws(gsub("\\.|\\,|\\s\\–", "", XML::xmlValue(speaker_node[["EMPHAS"]])))
          
          if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), tolower(speaker_full_name)) ) intervention_type <- NA
          if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "\\((.*)\\)$") ) intervention_type <- NA
          
          if ( !is.na(intervention_type) && intervention_type != "" && !(TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_no_translate)) &&
              (is.na(textcat::textcat(tolower(intervention_type))) || textcat::textcat(tolower(intervention_type)) != "english" || tolower(intervention_type) == "im namen der ppe-fraktion") ) {
            
            #if (opt$translate) { 
                if ( TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) ) {
                  t_index <- which(stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) == TRUE) 
                  intervention_type_translated <- intervention_types_manual_translated[[t_index]]
                  if (intervention_type_translated == "On Behalf Of The") {
                    group_name <- gsub(intervention_types_manual_translate[[t_index]], "", tolower(intervention_type))
                    group_name <- trimws(group_name)
                    group_name <- stringr::str_to_title(group_name)
                    intervention_type_translated <- paste(intervention_type_translated, group_name, "Group")
                  }
                  
                  #if ((i %% 5)== 0) 
                  clessnverse::logit(scriptname = scriptname, message = paste("self-translating intervention_type", intervention_type, "to", intervention_type_translated), logger = logger)
                  
                  intervention_type <- intervention_type_translated
                  intervention_type <- gsub("-Gruppen", "", intervention_type)
                  intervention_type <- gsub("-Fraktion", "", intervention_type)
                  intervention_type <- gsub("-Fractie", "", intervention_type)
                } else {
                  clessnverse::logit(scriptname = scriptname, message = paste("translating intervention_type", intervention_type), logger = logger)
                  intervention_type <- clessnverse::translateText(intervention_type, engine="azure", target_lang="en",fake=!opt$translate)[2]
                  #clessnverse::logit(scriptname = scriptname, message = paste("translated intervention_type", intervention_type), logger = logger)
                }
            #}
            
          }
          
          if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteure|rapporteur|representative")) ) {
            intervention_type <- NA
            speaker_type <- "Rapporteur"
          }
          
          if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "member")) ) {
            speaker_type <- intervention_type
            intervention_type <- NA
          }
          
          if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "winner|president,\\s|president-in-office|chancellor|president\\sof|minister|his\\sholiness|secretary|king|ombudsman|chair\\sof\\sthe") ) {
            speaker_type <- intervention_type
            intervention_type <- "Speech"
          }
          
          if ( !is.na(intervention_type) && tolower(intervention_type) == "president" ) {
            intervention_type <- "Moderation"
            speaker_type <- "President"
          } 
          
          if ( is.na(speaker_type) ) speaker_type <- "Member of the Commission"
          
          if ( !is.null(intervention_type) && !is.na(intervention_type) ) {
            intervention_type <- gsub("\\(", "", intervention_type)
            intervention_type <- gsub("\\)", "", intervention_type)
            intervention_type <- gsub("\"", "", intervention_type)
            intervention_type <- gsub("\\\\u2012", "", intervention_type)
            intervention_type <- gsub("\\\\ U2012", "", intervention_type)
            intervention_type <- gsub("blue-card", "Blue Card", intervention_type)
            intervention_type <- gsub("Blue-Card", "Blue Card", intervention_type)
            intervention_type <- gsub("Blue-card", "Blue Card", intervention_type)
            intervention_type <- stringr::str_squish(intervention_type)
            intervention_type <- trimws(intervention_type)
            intervention_type <- stringr::str_to_title(intervention_type)
          }
          
          if ( !is.null(speaker_type) && !is.na(speaker_type) ) {
            if (tolower(speaker_type) == "member of the commission") speaker_type <- "Member of the Commission"
          }
          
          speaker_district <- NA
          speaker_media <- NA    
          
          #intervention_type <- NA
          intervention_lang <- XML::xmlValue(speaker_node[["LG"]])
          intervention_word_count <- 0
          intervention_sentence_count <- 0
          intervention_paragraph_count <- 0
          intervention_text <- ""
          intervention_translated_text <- ""
          
          # Strip out the intervention by looping through paragraphs
          for (l in which(names(intervention_node) == "PARA")) {

            if (is.na(intervention_type)) {
              intervention_type <- trimws(XML::xmlValue(intervention_node[["PARA"]][["EMPHAS"]]))
              
              if ( !is.na(intervention_type) && intervention_type != "" && !(TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_no_translate)) &&
                  (is.na(textcat::textcat(tolower(intervention_type))) || textcat::textcat(tolower(intervention_type)) != "english" || tolower(intervention_type) == "im namen der ppe-fraktion") ) { 
                
                #if (opt$translate) { 
                  if ( TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) ) {
                    t_index <- which(stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) == TRUE) 
                    intervention_type_translated <- intervention_types_manual_translated[[t_index]]
                    if (intervention_type_translated == "On Behalf Of The") {
                      group_name <- gsub(intervention_types_manual_translate[[t_index]], "", tolower(intervention_type))
                      group_name <- trimws(group_name)
                      group_name <- stringr::str_to_title(group_name)
                      intervention_type_translated <- paste(intervention_type_translated, group_name, "Group")
                    }
                    
                    if ((i %% 5)== 0) clessnverse::logit(scriptname = scriptname, message = paste("self-translating", intervention_type, "to", intervention_type_translated), logger = logger)
                    
                    intervention_type <- intervention_type_translated
                    intervention_type <- gsub("-Gruppen", "", intervention_type)
                    intervention_type <- gsub("-Fraktion", "", intervention_type)
                    intervention_type <- gsub("-Fractie", "", intervention_type)
                  } else {
                    clessnverse::logit(scriptname = scriptname, message = paste("translating intervention_type", intervention_type), logger = logger)
                    intervention_type <- clessnverse::translateText(intervention_type, engine="azure", target_lang="en",fake=!opt$translate)[2]
                    #clessnverse::logit(scriptname = scriptname, message = paste("translated", intervention_type), logger = logger)
                  }
                #}
                
              }
              
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteure|rapporteur|representative")) ) {
                intervention_type <- NA
                speaker_type <- "Rapporteur"
              }
              
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "member")) ) {
                speaker_type <- intervention_type
                intervention_type <- NA
              }
              
              if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "winner|president,\\s|president-in-office|chancellor|president\\sof|minister|his\\sholiness|secretary|king|ombudsman|chair\\sof\\sthe") ) {
                speaker_type <- intervention_type
                intervention_type <- "Speech"
              }
              
              if ( !is.na(intervention_type) && tolower(intervention_type) == "president" ) {
                intervention_type <- "Moderation"
                speaker_type <- "President"
              } 
              
              if ( is.na(speaker_type) ) speaker_type <- "Member of the Commission"
              
            } #if (is.na(intervention_type)) {
            
            if ( !is.null(intervention_type) && !is.na(intervention_type) ) {
              intervention_type <- gsub("\\(", "", intervention_type)
              intervention_type <- gsub("\\)", "", intervention_type)
              intervention_type <- gsub("\"", "", intervention_type)
              intervention_type <- gsub("\\\\u2012", "", intervention_type)
              intervention_type <- gsub("\\\\ U2012", "", intervention_type)
              intervention_type <- gsub("blue-card", "Blue Card", intervention_type)
              intervention_type <- gsub("Blue-Card", "Blue Card", intervention_type)
              intervention_type <- gsub("Blue-card", "Blue Card", intervention_type)
              intervention_type <- stringr::str_squish(intervention_type)
              intervention_type <- trimws(intervention_type)
              intervention_type <- stringr::str_to_title(intervention_type)
            }
            
            if ( !is.null(speaker_type) && !is.na(speaker_type) ) {
              if (tolower(speaker_type) == "member of the commission") speaker_type <- "Member of the Commission"
            }
            
            intervention_text <- paste(intervention_text, XML::xmlValue(intervention_node[[l]]), sep = " ")
          } #for (l in which(names(intervention_node) == "PARA"))
          
          if (stringr::str_detect(intervention_text, "\\. – ")) {
            #intervention_text <- stringr::str_split(intervention_text, "\\. – ")[[1]][2]
            intervention_text <- gsub("\\. – ", "", intervention_text)
          }
          
          if (stringr::str_detect(intervention_text, "^– ")) {
            #intervention_text <- stringr::str_split(intervention_text, "^– ")[[1]][2]
            intervention_text <- gsub("^– ", "", intervention_text)
          }
          
          intervention_text <- trimws(intervention_text, "left")
          intervention_text <- gsub("^– ", "", intervention_text) 
          intervention_word_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), word, txt, token="words",format="text"))
          intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
          intervention_paragraph_count <- length(which(names(intervention_node) == "PARA"))
          
          # Translation
          if (grepl("\\\\", intervention_text)) intervention_text <- gsub("\\\\"," ", intervention_text)
          intervention_text <- gsub("^NA\n\n", "", intervention_text)
          intervention_text <- gsub("^\n\n", "", intervention_text)
          
          
          
          if (opt$translate) {
            if (textcat::textcat(intervention_text) != "english") {
              clessnverse::logit(scriptname = scriptname, message = paste("translating intervention_text", substr(intervention_text, 1, 50)), logger = logger)
              intervention_translation <- clessnverse::translateText(text=intervention_text, engine="azure", target_lang="en", fake=!opt$translate)
              intervention_translated_text <- intervention_translation[2]
              clessnverse::logit(scriptname = scriptname, message = paste("translated intervention_text", substr(intervention_translated_text, 1, 50)), logger = logger)
            } 
          } else {
            intervention_translation <- NA
            intervention_translated_text <- NA
          }
          
          if (!is.na(speaker_type)  && speaker_type == "breaking point") speaker_type <- NA
          if (!is.na(speaker_party) && stringr::str_detect(clessnverse::rm_accent(tolower(speaker_party)), "independent|independant")) speaker_party <- "Independent"
          if (is.null(intervention_translated_text)) intervention_translated_text <- NA
          
          # commit to dfDeep and the Hub
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
                                        subjectOfBusinessID = chapter_number,
                                        subjectOfBusinessTitle = chapter_title,
                                        subjectOfBusinessHeader = NA,
                                        subjectOfBusinessSeqNum = NA,
                                        subjectOfBusinessProceduralText = NA,
                                        subjectOfBusinessTabledDocID = chapter_tabled_docid,
                                        subjectOfBusinessTabledDocTitle = NA,
                                        subjectOfBusinessAdoptedDocID = chapter_adopted_docid,
                                        subjectOfBusinessAdoptedDocTitle = NA,
                                        speakerID = NA,
                                        speakerFirstName = speaker_first_name,
                                        speakerLastName = speaker_last_name,
                                        speakerFullName = speaker_full_name,
                                        speakerGender = speaker_gender,
                                        speakerType = speaker_type,
                                        speakerCountry = speaker_country,
                                        speakerIsMinister = speaker_is_minister,
                                        speakerParty = speaker_party,
                                        speakerPolGroup = speaker_polgroup,
                                        speakerDistrict = speaker_district,
                                        speakerMedia = speaker_media,
                                        interventionID = paste(gsub("dp", "", event_id),intervention_seqnum,sep=''),
                                        interventionDocID = NA,
                                        interventionDocTitle = NA,
                                        interventionType = intervention_type,
                                        interventionLang = intervention_lang,
                                        interventionWordCount = intervention_word_count,
                                        interventionSentenceCount = intervention_sentence_count,
                                        interventionParagraphCount = intervention_paragraph_count,
                                        interventionText = intervention_text,
                                        interventionTextFR = NA,
                                        interventionTextEN = intervention_translated_text,
                                        stringsAsFactors = FALSE)
          
          v2_row_to_commit <- v2_row_to_commit %>% mutate(across(everything(), as.character))
          
          v2_metadata_to_commit <- list("url"=event_url, "format"="xml", "location"="EU",
                                        "parliament_number"=parliament_number, "parliament_session"=NA_character_)
          
          dfInterventions <- clessnverse::commitAgoraplusInterventions(dfDestination = dfInterventions, 
                                                                      type = "parliament_debate", schema = "v2",
                                                                      metadata = v2_metadata_to_commit,
                                                                      data = v2_row_to_commit,
                                                                      opt$dataframe_mode, opt$hub_mode)

          intervention_count <- intervention_count + 1
          
          current_speaker_full_name <- speaker_full_name
  
      } #for (k in which(chapter_nodes_list == "INTERVENTION"))
      
    } #if ( "INTERVENTION" %in% chapter_nodes_list )
  
  } #for (j in 1:core_xml_nbchapters)
  
} #for (i in 1:length(urls_list))

clessnverse::logit(scriptname, final_message, logger)
clessnverse::logit(scriptname, paste(debate_count, "debates were added to the hub totalling", intervention_count, "interventions"), logger)
clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
quit(save="no", status = status)
