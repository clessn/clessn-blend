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


###############################################################################
#   Globals
#
#   scriptname
#   logger
#
installPackages()
library(dplyr)

if (!exists("scriptname")) scriptname <- "agorapluseurope-debats.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit("scraper", c("file", "hub"), Sys.getenv("LOG_PATH"))

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

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

# Download v2 Cache
if (opt$dataframe_mode %in% c("update","refresh")) {
  dfCache2 <- clessnverse::loadAgoraplusCacheDf(type = "parliament_debate", schema = "v2",
                                                location = "EU",
                                                download_data = opt$download_data,
                                                token = Sys.getenv('HUB_TOKEN'))
  
  if (is.null(dfCache2)) dfCache2 <- clessnverse::createAgoraplusCacheDf(type = "parliament_debate", schema = "v2")
  
  if (opt$download_data) { 
    dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location", 
                            "data.eventID", "data.rawContent")]
  } else {
    dfCache2 <- dfCache2[,c("key","type","schema","uuid", "metadata.url", "metadata.format", "metadata.location")]
  }
  
} else {
  clessnverse::logit(scriptname, "Not retreiving interventions from hub because hub_mode is rebuild or skip", logger)
  dfCache2 <- clessnverse::createAgoraplusCacheDf(type="parliament_debate", schema = "v2")
}

# Download v2 MPs information
metadata_filter <- list(country="CA", province_or_state="EU")
filter <- clessnhub::create_filter(type="mp", schema="v2", metadata=metadata_filter)  
dfPersons <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

# Download v2 public service personnalities information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(country="CA", province_or_state="EU")
filter <- clessnhub::create_filter(type="public_service", schema="v2", metadata=metadata_filter)  
dfPublicService <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

dfPersons <- dfPersons %>% full_join(dfPublicService)
dfPersons <<- dfPersons %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ")



# Load all objects used for ETL including V1 HUB MPs
# clessnverse::loadETLRefData(username = Sys.getenv('HUB_USERNAME'), 
#                             password = Sys.getenv('HUB_PASSWORD'), 
#                             url = Sys.getenv('HUB_URL'))
clessnverse::loadETLRefData()


###############################################################################
# Data source
#
# connect to the dataSource : the provincial parliament web site 
# get the index page containing the URLs to all the national assembly debates
# to extract those URLS and get them individually in order to parse
# each debate
#

scraping_method <- "DateRange"
#scraping_method <- "FrontPage"

start_date <- "2019-12-01"
#num_days <- as.integer(as.Date(Sys.time()) - as.Date(start_date))
num_days <- 60

if (scraping_method == "frontpage") {
  base_url <- "https://www.europarl.europa.eu"
  content_url <- "/plenary/fr/debates-video.html#sidesForm"
  
  source_page <- xml2::read_html(paste(base_url,content_url,sep=""))
  source_page_html <- htmlParse(source_page, useInternalNodes = TRUE)
  
  urls <- rvest::html_nodes(source_page, 'a')
  urls <- urls[grep("\\.xml", urls)]
  urls <- urls[grep("https", urls)]
  
  urls_list <- rvest::html_attr(urls, 'href')
  urls_list <- as.list(urls_list)
}

if (scraping_method == "DateRange") {
  date_vect <- seq( as.Date(start_date), by=1, len=num_days)
  
  urls_list <- list()
  nb_deb <- 0
  
  for (d in date_vect) {
    url <- paste("https://www.europarl.europa.eu/doceo/document/CRE-9-", as.character(as.Date(d, "1970-01-01")), "_FR.xml",sep= '')
    r <- httr::GET(url)
    if (r$status_code == 200) {
      urls_list <- c(urls_list, url)
      nb_deb <- nb_deb + 1
    }
  }
}


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
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it to extract the
# debates content
#
for (i in 1:length(urls_list)) {
  event_url <- urls_list[[i]]
  event_id <- stringr::str_match(event_url, "\\CRE-(.*)\\.")[2]
  event_id <- stringr::str_replace_all(event_id, "[[:punct:]]", "")
  
  parliament_number <- stringr::str_match(event_url, "CRE-(.*)-((?:19|20)[0-9]{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])(.*)$")[2]
  
  clessnverse::logit(scriptname, paste("Debate", i, "of", length(urls_list),sep = " "), logger)
  cat("\nDebat", i, "de", length(urls_list),"\n")
  

  ###
  # If the data is not cache we get the raw html from assnat.qc.ca
  # if it is cached (we scarped it before), we prefer not to bombard
  # the website with HTTP_GET requests and ise the cached version
  ###
  if ( !(event_id %in% dfCache2$key) ) {
    # Read and parse HTML from the URL directly
    #doc_html <- getURL(event_url)
    r <- httr::GET(event_url)
    if (r$status_code == 200) {
      doc_html <- httr::content(r)
      doc_xml <- XML::xmlTreeParse(doc_html, useInternalNodes = TRUE)
      top_xml <- XML::xmlRoot(doc_xml)
      head_xml <- top_xml[[1]]
      core_xml <- top_xml[[2]]
      cached_html <- FALSE
    } else {
      next
    }
  } else{ 
    # Retrieve the XML structure from dfCache and Parse
    filter <- clessnhub::create_filter(key = event_id, type = "parliament_debate", schema = "v2", metadata = list("location"="EU", "format"="xml"))
    doc_html <- clessnhub::get_items('agoraplus_cache', filter = filter)
    doc_html <- doc_html$data.rawContent
    doc_xml <- XML::xmlTreeParse(doc_xml, useInternalNodes = TRUE)
    top_xml <- XML::xmlRoot(doc_xml)
    head_xml <- top_xml[[1]]
    core_xml <- top_xml[[2]]
    cached_html <- TRUE
    clessnverse::logit(scriptname, paste(event_id, "cached"), logger)
  }
    
  # Get the length of all branches of the XML document
  core_xml_nbchapters <- length(names(core_xml))

    
  ###############################
  # Columns of the simple dataset
  #event_source_type <- xpathApply(source_page_html, '//title', XML::xmlValue)[[1]]
  event_source_type <- "Débats et vidéos | Plénière | Parlement européen"
  event_url <- event_url
  event_date <- substr(XML::xmlGetAttr(core_xml[[2]][["TL-CHAP"]], "VOD-START"),1,10)
  event_start_time <- substr(XML::xmlGetAttr(core_xml[[2]][["TL-CHAP"]], "VOD-START"),12,19)
  event_end_time <- substr(XML::xmlGetAttr(core_xml[[core_xml_nbchapters]][["TL-CHAP"]], "VOD-END"),12,19)
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
  
  pb_chap <- txtProgressBar(min = 0,      # Minimum value of the progress bar
                          max = core_xml_nbchapters, # Maximum value of the progress bar
                          style = 3,    # Progress bar style (also available style = 1 and style = 2)
                          width = 80,   # Progress bar width. Defaults to getOption("width")
                          char = "=")   # Character used to create the bar
  
  event_content <- ""
  event_translated_content <- ""
  
  current_speaker_full_name <- ""
  
  for (j in 1:core_xml_nbchapters) {
    cat(" chapter progress\r")
    setTxtProgressBar(pb_chap, j)
    
    # New chapter
    chapter_node <- core_xml[[j]]
    chapter_number <- XML::xmlGetAttr(chapter_node, "NUMBER")
    chapter_title <- XML::xmlValue(XML::xpathApply(chapter_node, "//CHAPTER/TL-CHAP[@VL='FR']")[j])
    
    if (!is.null(chapter_node[["TL-CHAP"]][[2]])) {
      # Here there is a URL within the title of the chapter => most likely a linked document 
      chapter_tabled_docid <- stringr::str_split(XML::xmlGetAttr(chapter_node[["TL-CHAP"]][[2]], "redmap-uri"), "/")[[1]][3]
      
      tabled_document_url <- paste("https://www.europarl.europa.eu/doceo/document/", chapter_tabled_docid, "_FR.html", sep = "")
      tabled_document_html <- getURL(tabled_document_url)
      tabled_document_html_table <- readHTMLTable(tabled_document_html)
      
      list1_id <- grep("Textes adoptés", tabled_document_html_table)[1]
      list2_id <- grep("Textes adoptés", tabled_document_html_table[[list1_id]])[2]
      chapter_adopted_docid <- tabled_document_html_table[[list1_id]][[list2_id]][which(grepl("Textes adoptés",tabled_document_html_table[[list1_id]][[list2_id]]))]
      chapter_adopted_docid <- stringr::str_split(chapter_adopted_docid, " ")[[1]][length(stringr::str_split(chapter_adopted_docid, " ")[[1]])]
      
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
        
        intervention_text <- ""
        
        #cat("Intervention", intervention_seqnum, "\r", sep = " ")
        
        # Strip out the speaker info
        intervention_node <- core_xml[[j]][[k]]
        speaker_node <- intervention_node[["ORATEUR"]]
        
        speaker_full_name <- XML::xmlGetAttr(speaker_node, "LIB")
        speaker_last_name <- trimws(stringr::str_split(speaker_full_name, "\\|")[[1]][[2]], "both")
        speaker_first_name <- trimws(stringr::str_split(speaker_full_name, "\\|")[[1]][[1]], "both")
        speaker_full_name <- stringr::str_remove(speaker_full_name, "\\|\\s")
        
        dfSpeaker <- clessnverse::getEuropeMepData(speaker_full_name)
        
        if (!is.na(dfSpeaker$mepid)) {
          speaker_mepid <- dfSpeaker$mepid
          speaker_party <- dfSpeaker$party
          speaker_polgroup <- case_when(XML::xmlGetAttr(speaker_node, "PP") != "NULL" ~ 
                                          paste(XML::xmlGetAttr(speaker_node, "PP"), ":", dfSpeaker$polgroup, sep = " "),
                                        TRUE ~ 
                                          dfSpeaker$polgroup)
          speaker_country <- dfSpeaker$country
        } else {
          speaker_mepid <- NA
          speaker_party <- case_when(XML::xmlGetAttr(speaker_node, "PP") != "NULL" ~  XML::xmlGetAttr(speaker_node, "PP"),
                                     TRUE ~ NA_character_)
          speaker_polgroup <- NA
          speaker_country <- countrycode::codelist$country.name.en[which(countrycode::codelist$iso2c == XML::xmlGetAttr(speaker_node, "LG"))]
          if (length(speaker_country) == 0) speaker_country <- NA
        }
        
        speaker_gender <- paste("", gender::gender(clessnverse::splitWords(speaker_first_name)[1])$gender, sep = "")
        if ( speaker_gender == "" ) speaker_gender <- NA
        speaker_is_minister <- NA
        
        speaker_type <- trimws(gsub("\\.|\\,|\\s\\–", "", XML::xmlValue(speaker_node[["EMPHAS"]])))
        
        if (!is.na(speaker_type) && stringr::str_detect(tolower(speaker_type), tolower(speaker_full_name)) ) speaker_type <- NA
        
        speaker_district <- NA
        speaker_media <- NA    
        
        intervention_type <- NA
        intervention_lang <- XML::xmlValue(speaker_node[["LG"]])
        intervention_word_count <- 0
        intervention_sentence_count <- 0
        intervention_paragraph_count <- 0
        intervention_text <- ""
        intervention_translated_text <- ""
        
        # Strip out the intervention by looping through paragraphs
        for (l in which(names(intervention_node) == "PARA")) {
          #cat("Paragraph", l, "\n", sep = " ")
          if (is.na(speaker_type)) speaker_type <- XML::xmlValue(intervention_node[["PARA"]][["EMPHAS"]])
          intervention_text <- paste(intervention_text, XML::xmlValue(intervention_node[[l]]), sep = " ")
        } #for (l in which(names(intervention_node) == "PARA"))
        
        if (stringr::str_detect(intervention_text, "\\. – ")) {
          intervention_text <- stringr::str_split(intervention_text, "\\. – ")[[1]][2]
        }
        
        if (stringr::str_detect(intervention_text, "^– ")) {
          intervention_text <- stringr::str_split(intervention_text, "^– ")[[1]][2]
        }
        
        intervention_text <- trimws(intervention_text, "left")
        intervention_text <- gsub("^– ", "", intervention_text) 
        intervention_word_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), word, txt, token="words",format="text"))
        intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
        intervention_paragraph_count <- length(which(names(intervention_node) == "PARA"))
        
        # Translation
        intervention_translated_text <- clessnverse::translateText(text = intervention_text, engine = "azure", 
                                                                target_lang = "fr", fake = TRUE)
        
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
        
        #dfInterventionRow <- data.frame(
          #uuid = "",
          #created = "",
          #modified = "",
          #metedata = "",
          #eventID = event_id,
          #chapterNumber = chapter_number,
          #chapterTitle = chapter_title,
          #chapterTabledDocId = chapter_tabled_docid,
          #chapterAdoptedDocId = chapter_adopted_docid,
          #interventionSeqNum = intervention_seqnum,
          #speakerFirstName = speaker_first_name,
          #speakerLastName = speaker_last_name,
          #speakerFullName = speaker_full_name,
          #speakerGender = speaker_gender,
          #speakerIsMinister = speaker_is_minister,
          #speakerType = speaker_type,
          #speakerCountry = speaker_country,
          #speakerParty = speaker_party,
          #speakerPolGroup = speaker_polgroup,
          #speakerDistrict = speaker_district,
          #speakerMedia = speaker_media,
          #speakerSpeechType = intervention_type,
          #speakerSpeechLang = intervention_lang,
          #speakerSpeechWordCount = intervention_word_count,
          #speakerSpeechSentenceCount = intervention_sentence_count,
          #speakerSpeechParagraphCount = intervention_paragraph_count,
          #speakerSpeech = intervention_text,
          #speakerTranslatedSpeech = intervention_translated_text)
        
        current_speaker_full_name <- speaker_full_name
  
      } #for (k in which(chapter_nodes_list == "INTERVENTION"))
      
    } #if ( "INTERVENTION" %in% chapter_nodes_list )
  
  } #for (j in 1:core_xml_nbchapters)
  
  #dfEventRow <- data.frame(uuid = "",
  #                         created = "",
  #                         modified = "",
  #                         metedata = "",
  #                         eventID = event_id,
  #                         eventSourceType = event_source_type,
  #                         eventURL = event_url,
  #                         eventDate = as.character(event_date),
  #                         eventStartTime = as.character(event_start_time),
  #                         eventEndTime = as.character(event_end_time),
  #                         eventTitle = event_title,
  #                         eventSubtitle = event_subtitle,
  #                         eventWordCount = event_word_count,
  #                         eventSentenceCount = event_sentence_count,
  #                         eventParagraphCount = event_paragraph_count,
  #                         eventContent = event_content,
  #                         eventTranslatedContent = event_translated_content)


  # Update the cache
  #row_to_commit <- data.frame(uuid = "", created = "", modified = "", metadata = "", eventID = event_id, eventHtml = doc_html, stringsAsFactors = FALSE)
  ###dfCache <- clessnverse::commitCacheRows(row_to_commit, dfCache, 'agoraplus_warehouse_cache_items', opt$cache_mode, opt$hub_mode)
  
  # Update cache dans hub v2
  #v2_row_to_commit <- data.frame(eventID = event_id, rawContent = doc_html, stringsAsFactors = FALSE)
  #v2_metadata_to_commit <- list("url"=event_url, "format"="html", "location"="CA-QC")
  
  #dfCache2 <- clessnverse::commitAgoraplusCache(dfDestination = dfCache2, type = "press_conference", schema = "v2",
  #                                              metadata = v2_metadata_to_commit,
  #                                              data = v2_row_to_commit,
  #                                             opt$dataframe_mode, opt$hub_mode)
  
} #for (i in 1:length(urls_list))

clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
