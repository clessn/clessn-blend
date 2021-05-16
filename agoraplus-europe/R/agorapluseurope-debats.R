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

if (!exists("scriptname")) scriptname <- "agorapluseurope-debats.R"
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, "file", Sys.getenv("LOG_PATH"))

opt <- list(cache_update = "update",simple_update = "update",deep_update = "update",
             hub_update = "skip",csv_update = "update",backend_type = "CSV")


if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (opt$backend_type == "HUB") {
  clessnverse::loadAgoraplusHUBDatasets("quebec", opt, 
                                        Sys.getenv('HUB_USERNAME'), 
                                        Sys.getenv('HUB_PASSWORD'), 
                                        Sys.getenv('HUB_URL'))
}

if (opt$backend_type == "CSV") {
  clessnverse::loadAgoraplusCSVDatasets("europe", opt, "../clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-europe")
}

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

scraping_method <- "DateRange"
#scraping_method <- "FrontPage"

start_date <- "2019-07-01"
#num_days <- as.integer(as.Date(Sys.time()) - as.Date(start_date))
num_days <- 730

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
    url <- paste("https://www.europarl.europa.eu/doceo/document/CRE-9-", as.character(as.Date(d, "1970-01-01")), "_FR.html",sep= '')
    urls_list <- c(urls_list, url)
    r <- httr::GET(url)
    if (r$status_code == 200) {
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
  if (opt$backend_type == "HUB") clessnhub::v1_refresh_token(configuration$token, configuration$url)
  current_url <- urls_list[[i]]
  current_id <- str_replace_all(urls_list[i], "[[:punct:]]", "")
  
  clessnverse::logit(paste("Debate", i, "of", length(urls_list),sep = " "), logger)
  cat("\nDebat", i, "de", length(urls_list),"\n")
  

  ###
  # If the data is not cache we get the raw html from assnat.qc.ca
  # if it is cached (we scarped it before), we prefer not to bombard
  # the website with HTTP_GET requests and ise the cached version
  ###
  if ( !(current_id %in% dfCache$eventID) ) {
    # Read and parse HTML from the URL directly
    #doc_html <- getURL(current_url)
    r <- httr::GET(current_url)
    if (r$status_code == 200) {
      doc_html <- httr::content(r)
      doc_xml <- xmlTreeParse(doc_html, useInternalNodes = TRUE)
      top_xml <- xmlRoot(doc_xml)
      head_xml <- top_xml[[1]]
      core_xml <- top_xml[[2]]
      cached_html <- FALSE
    } else {
      next
    }
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
  #event_source_type <- xpathApply(source_page_html, '//title', xmlValue)[[1]]
  event_source_type <- "Débats et vidéos | Plénière | Parlement européen"
  event_url <- current_url
  event_date <- substr(xmlGetAttr(core_xml[[2]][["TL-CHAP"]], "VOD-START"),1,10)
  event_start_time <- substr(xmlGetAttr(core_xml[[2]][["TL-CHAP"]], "VOD-START"),12,19)
  event_end_time <- substr(xmlGetAttr(core_xml[[core_xml_nbchapters]][["TL-CHAP"]], "VOD-END"),12,19)
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
        
        # Skip if this intervention already is in the dataset
        #if (nrow(dfDeep[dfDeep$eventID == current_id & dfDeep$interventionSeqNum == intervention_seqnum,]) > 0 &&
        #    opt$deep_update != "refresh") {
        #  intervention_seqnum <- intervention_seqnum+1
        #  next
        #}
        
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
        
        if (!is.na(speaker_type) && str_detect(tolower(speaker_type), tolower(speaker_full_name)) ) speaker_type <- NA
        
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
                                                                target_lang = "fr", fake = FALSE)
        
        if (is.null(speaker_translated_speech)) speaker_translated_speech <- NA
          
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
                                                          modeLocalData = opt$deep_update, 
                                                          modeHub = opt$hub_update)
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
                                             modeLocalData = opt$simple_update, 
                                             modeHub = opt$hub_update)

  dfCache <- clessnverse::commitCacheRows(dfSource = data.frame(eventID = current_id, eventHtml = toString(doc_html), stringsAsFactors = F),
                                            dfDestination = dfCache,
                                            hubTableName = 'agoraplus-eu_warehouse_cache_items', 
                                            modeLocalData = opt$cache_update, 
                                            modeHub = opt$hub_update)
} #for (i in 1:length(urls_list))


if (opt$csv_update != "skip" && opt$backend_type == "CSV") { 
  write.csv2(dfCache, file=paste(base_csv_folder,"dfCacheAgoraPlus-2021-03-28-notranslate.csv",sep='/'), row.names = FALSE)
  write.csv2(dfDeep, file = paste(base_csv_folder,"dfDeepAgoraPlus-2021-03-28-notranslate.csv",sep='/'), row.names = FALSE)
  write.csv2(dfSimple, file = paste(base_csv_folder,"dfSimpleAgoraPlus-2021-03-28-notranslate.csv",sep='/'), row.names = FALSE)
}

clessnverse::logit(paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
