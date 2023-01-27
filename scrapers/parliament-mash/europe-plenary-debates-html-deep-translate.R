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


detect_language <- function(text) {
  url <- "https://deep-translate1.p.rapidapi.com/language/translate/v2/detect"

  text <- gsub("\\\"","'", text)
  text <- gsub("\\n\\n","  ",text)

  if (is.null(text)) return(NA_character_) 
  if (is.na(text)) return(NA_character_) 
  if (nchar(trimws(text)) == 0) return(NA_character_)

  df <- tidytext::unnest_tokens(
      data.frame(txt=text), 
      input = txt, 
      output = "Sentence", 
      token = "regex",
      pattern = "(?<!\\b\\p{L}r)\\.|\\n\\n", to_lower=F)

  clessnverse::logit(scriptname, "detecting language", logger)

  response <- httr::VERB(
    "POST", 
    url, 
    body = paste("{\"q\":\"", df$Sentence[1],"\"}", sep=''),
    httr::add_headers(
        'X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775',
        'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'),
        httr::content_type("application/octet-stream"))
  r <- jsonlite::fromJSON(httr::content(response, "text"))

  clessnverse::logit(scriptname, "detecting language done", logger)

  return(r$data$detections$language)
}


translate_language <- function(text, source, dest) {
  url <- "https://deep-translate1.p.rapidapi.com/language/translate/v2"

  text <- gsub("\\\"","'", text)
  text <- gsub("\\n\\n","  ",text)

  if (is.null(text)) return(NA_character_) 
  if (is.na(text)) return(NA_character_) 
  if (nchar(trimws(text)) == 0) return(NA_character_)

  if (nchar(text) > 5000) {
    df <- tidytext::unnest_tokens(
      data.frame(txt=text), 
      input = txt, 
      output = "Sentence", 
      token = "regex",
      pattern = "(?<!\\b\\p{L}r)\\.|\\n\\n", to_lower=F)

    result <- ""
    payload_txt <- ""

    for (i in 1:nrow(df)) {
      if (is.null(df$Sentence[i])) next 
      if (is.na(df$Sentence[i])) next 
      if (nchar(trimws(df$Sentence[i])) == 0) next

      if (payload_txt == "") {
        payload_txt <- df$Sentence[i]
      } else {
        if (nchar(payload_txt) + nchar(df$Sentence[i]) < 5000 && i < nrow(df)) {
          payload_txt <- paste(payload_txt, df$Sentence[i], sep = ".  ") 
        } else {
          payload <- paste("{\"q\":\"", payload_txt,"\",\"source\": \"",source,"\",\"target\": \"",dest,"\"}", sep='')
          encode <- "json"

          clessnverse::logit(scriptname, paste("translating language - pass", i), logger)

          response <- httr::VERB(
            "POST", 
            url, 
            body = payload,
            httr::add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 
            'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'), 
            httr::content_type("application/json"), 
            encode = encode)
      
          clessnverse::logit(scriptname, paste("translating language done - pass", i), logger)

          r <- jsonlite::fromJSON(httr::content(response, "text"))

          result <- paste(result,r$data$translations$translatedText, sep=" ")

          payload_txt <- ""
        }
      }
    }

  } else {

    payload <- paste("{\"q\":\"", text,"\",\"source\": \"",source,"\",\"target\": \"",dest,"\"}", sep='')
    encode <- "json"

    clessnverse::logit(scriptname, "translating language", logger)

    response <- httr::VERB(
      "POST", 
      url, 
      body = payload,
      httr::add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 
      'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'), 
      httr::content_type("application/json"), 
      encode = encode)

    clessnverse::logit(scriptname, "translating language done", logger)
  
    r <- jsonlite::fromJSON(httr::content(response, "text"))

    result <- r$data$translations$translatedText
  }

  return(trimws(result))
}



###############################################################################
# Function : rm_accent
# Removes accents from a string
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
} # </function rm_accent>



###############################################################################
#   Globals
#
#   scriptname
#   logger
#
#installPackages()
library(dplyr)

if (!exists("scriptname")) scriptname <- "agorapluseurope-debats-html.R"

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

# Script command line options:
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataframe by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataframe
# - rebuild : wipes out completely the dataframe and rebuilds it from scratch
# - skip : does not make any change to the dataframe
opt <- list(dataframe_mode = "rebuild", hub_mode = "update", log_output = "file,console", download_data = FALSE, translate = TRUE)

if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit("scraper", opt$log_output, Sys.getenv("LOG_PATH"))


# Download HUB v2 data
if (opt$dataframe_mode %in% c("update","refresh")) {
  clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
  dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "parliament_debate", schema = "v3beta", 
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


# Download v2 MPs information
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(institution="European Parliament")
filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=metadata_filter)  
dfPersons <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)



# Load all objects used for ETL including V1 HUB MPs
clessnverse::loadETLRefData()
dfCountryLanguageCodes <- clessnverse::loadCountryLanguageCodes(token  = Sys.getenv('DROPBOX_TOKEN'))


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

start_date <- "2014-09-18"
#start_date <- "2014-01-01"
#num_days <- as.integer(as.Date(Sys.time()) - as.Date(start_date))
num_days <- 1
start_parliament <- 8
num_parliaments <- 1

if (scraping_method == "frontpage") {
  base_url <- "https://www.europarl.europa.eu"
  content_url <- "/plenary/en/debates-video.html#sidesForm"
  
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
    for (p in c(start_parliament:(start_parliament+num_parliaments))) {
      url <- paste("https://www.europarl.europa.eu/doceo/document/CRE-", p, "-", as.character(as.Date(d, "1970-01-01")), "_EN.html",sep= '')
      r <- httr::GET(url)
      if (r$status_code == 200) {
        urls_list <- c(urls_list, url)
        nb_deb <- nb_deb + 1
      }
    }
  }
}

# Hack here to get the url list from a rds file
#urls_list <- readRDS("urls_list.rds")

clessnverse::logit(scriptname = scriptname, message = paste("list of urls containing", length(urls_list), "debates"), logger = logger)

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
# Run through the URLs list, get the html content 
# from the parliament  website and start parsing it to extract the
# debates content
#
for (i in 1:length(urls_list)) {
  event_url <- urls_list[[i]]
  event_id <- stringr::str_match(event_url, "\\CRE-(.*)\\.")[2]
  event_id <- stringr::str_replace_all(event_id, "[[:punct:]]", "")

  parliament_number <- stringr::str_match(event_url, "CRE-(.*)-((?:19|20)[0-9]{2})-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01])(.*)$")[2]
  
  clessnverse::logit(scriptname, paste("Debate", i, "of", length(urls_list),sep = " "), logger)
  cat("\nDebat", i, "de", length(urls_list),"\n")
  
    clessnverse::logit(scriptname, "getting event_url", logger)

    r <- httr::GET(event_url)
    if (r$status_code == 200) {
      doc_html <- httr::content(r)
      #parsed_html <- XML::htmlTreeParse(doc_html, asText = TRUE)
      parsed_xml <- XML::htmlParse(doc_html)
      #xml_root <- XML::xmlRoot(parsed_html)
      xml_root <- XML::xmlRoot(parsed_xml)
      xml_head <- xml_root[[1]]
      xml_core <- xml_root[[2]]
      clessnverse::logit(scriptname, event_id, logger)
    } else {
      next
    }
    
  clessnverse::logit(scriptname = scriptname, message = event_url, logger = logger)

  # Get the length of all branches of the XML document
  core_xml_chapters <- XML::getNodeSet(xml_core[[5]], ".//table[@class='doc_box_header']")
  core_xml_nbchapters <- length(core_xml_chapters)
  
  if ( core_xml_nbchapters == 0 ) {
    # Get the length of all branches of the XML document
    core_xml_chapters <- XML::getNodeSet(xml_core[[6]], ".//table[@class='doc_box_header']")
    core_xml_nbchapters <- length(core_xml_chapters)
  }

    
  ###############################
  # Columns of the simple dataset
  #event_source_type <- xpathApply(source_page_html, '//title', xmlValue)[[1]]
  event_source_type <- "Débats et vidéos | Plénière | Parlement européen"
  #event_date <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//a")[[3]])[["href"]]
  #event_date <- stringr::str_match(event_date, "\\d{8}")
  #event_date <- paste(stringr::str_sub(event_date,1,4), stringr::str_sub(event_date,5,6), stringr::str_sub(event_date,7,8), sep="-")
  event_date <- stringr::str_match(event_url, "\\d{4}-\\d{2}-\\d{2}")
  event_date <- event_date[1]
  
  for (start_time_index in 2:core_xml_nbchapters) {
    if ( TRUE %in% grepl("Video\u00a0of\u00a0the\u00a0speeches", XML::xmlValue(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a"))) ) break
  }
  
  event_start_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a")[[3]])[["href"]]
  event_start_time <- stringr::str_match(event_start_time, "\\d{2}:\\d{2}:\\d{2}")
  event_start_time <- event_start_time[1]
  
  if ( is.na(event_start_time) ) {
    event_start_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a")[[5]])[["href"]]
    event_start_time <- stringr::str_match(event_start_time, "\\d{2}:\\d{2}:\\d{2}")
    event_start_time <- event_start_time[1]
  }
    
  
  last_chapter <- grep("Closure of the sitting", lapply(core_xml_chapters, XML::xmlValue))[2]
  
  if (is.na(last_chapter)) last_chapter <- grep("Interruption of the sitting", lapply(core_xml_chapters, XML::xmlValue))[2]
  if (is.na(last_chapter)) last_chapter <- grep("End of session", lapply(core_xml_chapters, XML::xmlValue))[2]
  if (is.na(last_chapter)) last_chapter <- grep("Adjournment of the session", lapply(core_xml_chapters, XML::xmlValue))[2]
  
  event_end_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[last_chapter]][["tr"]][["td"]], ".//a")[[3]])
  
  if ("href" %in% names(event_end_time)) {
    event_end_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[last_chapter]][["tr"]][["td"]], ".//a")[[3]])[["href"]]
    event_end_time <- stringr::str_match(event_end_time, "\\d{2}:\\d{2}:\\d{2}")
    event_end_time <- event_end_time[1]
  } else {
    event_end_time <- NA
  }
  
  if ( is.na(event_end_time) ) {
    event_end_time <- XML::getNodeSet(core_xml_chapters[[last_chapter]][["tr"]][["td"]], ".//p")
    event_end_time <- XML::xmlValue(event_end_time)
    event_end_time_index <- which(!is.na(stringr::str_match(event_end_time, "\\d{2}\\.\\d{2}")))
    
    if (length(event_end_time_index)>0) {
      event_end_time <- stringr::str_match(event_end_time, "\\d{2}\\.\\d{2}")[[event_end_time_index]]
      event_end_time <- stringr::str_replace(event_end_time, "\\.", ":") 
      event_end_time <- paste(event_end_time, ":00", sep='')
    }
  }
  
  
  # president_name  <- XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//p")[[1]])
  # president_title <- tail(strsplit(XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//p")[[1]]), split = " ")[[1]], 1)
  # president_name  <- stringr::str_match(tolower(president_name), "^(.*)(\\.|\\:|sidence de mme|puhetta johti)(.*)(\\s)(.*)$")[4]
  
  president_name  <- XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//span")[[1]])
  president_title <- tail(strsplit(XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//span")[[2]]), split = " ")[[1]], 1)
  president_name  <- stringr::str_match(tolower(president_name), "(\\.|\\:|sidence de mme|puhetta johti)(.*)$")[3]
  
  
  
  # if ( stringr::str_detect(tolower(president_name), "^présidence de") ) {
  #   president_name <- trimws(stringr::str_match(tolower(president_name), "présidence de (.*)")[[2]])
  #   president_name <- trimws(stringr::str_split(president_name, "\\.")[[1]][2])
  # } else {
  #   if ( stringr::str_detect(tolower(president_name), "^presidência: ") ) {
  #     president_name <- trimws(stringr::str_match(tolower(president_name), "presidência:(.*)\\svice-presidente")[[2]])
  #   } else {
  #     if ( stringr::str_detect(tolower(president_name), "^przewodnictwo: ") ) {
  #       president_name <- trimws(stringr::str_match(tolower(president_name), "przewodnictwo:(.*)\\swiceprzewodnicząca")[[2]])
  #     } else {
  #       president_name <- trimws(stringr::str_match(tolower(president_name), "(.*)president")[[2]])
  #       president_name <- trimws(stringr::str_split(president_name, "\\.")[[1]][2])
  #     }
  #   }
  # }
  
  president_name <- stringr::str_to_title(president_name)
  president_name <- trimws(president_name)
  
  
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
  speaker_polgroup <- NA
  speaker_party <- NA
  speaker_district <- NA
  speaker_country <- NA
  speaker_media <- NA
  chapter_tabled_docid <- NA
  intervention_type <- NA
  intervention_lang <- NA
  intervention_word_count <- NA
  intervention_sentence_count <- NA
  intervention_paragraph_count <- NA
  intervention_text <- ""
  intervention_translated_text <- NA
  
  dfSpeaker <- data.frame()

  ########################################################
  # Go through the xml document chapter by chapter
  # and strip out any relevant info
  intervention_seqnum <- 1
  
  # pb_chap <- txtProgressBar(min = 0,      # Minimum value of the progress bar
  #                         max = core_xml_nbchapters, # Maximum value of the progress bar
  #                         style = 3,    # Progress bar style (also available style = 1 and style = 2)
  #                         width = 80,   # Progress bar width. Defaults to getOption("width")
  #                         char = "=")   # Character used to create the bar
  
  event_content <- ""
  event_translated_content <- ""
  
  previous_speaker_full_name <- ""
  
  for (j in 2:core_xml_nbchapters) {
    # We start at 2 because the first one is the TOC
    # cat(" chapter progress\r")
    # setTxtProgressBar(pb_chap, j)
    
    # New chapter
    chapter_node <- core_xml_chapters[[j]][["tr"]][["td"]]
    
    title_node <- XML::getNodeSet(chapter_node, ".//td[@class='doc_title']")
    chapter_title <- XML::xmlValue(title_node)
    chapter_title <- stringr::str_remove(chapter_title, "\n")
    chapter_title <- trimws(chapter_title)
    chapter_number <- stringr::str_match(chapter_title, "^(0|[1-9][0-9]*)\\.(.*)")[[2]]
    chapter_title <- stringr::str_match(chapter_title, "^(0|[1-9][0-9]*)\\.(.*)")[[3]] 
    chapter_title <- trimws(chapter_title)
    
    ##################################################
    chapter_tabled_docid <- NA
    chapter_adopted_docid <- NA
    
    chapter_nodes_list <- names(chapter_node)
    
    if ( "a" %in% chapter_nodes_list ) {
      # There is potentially one or more interventions in this section.
      # From potentially multiple speakers
      
      
      # We have to loop through every intervention
      chapter_nodes_list <- which(chapter_nodes_list == "a")
      
      for (k in 1:length(chapter_nodes_list)) {
        #Find out if there is an intervention in this section
        chapter_container_node <- chapter_node[chapter_nodes_list[[k]]+2]
        
        content_node <- XML::getNodeSet(chapter_container_node$table, ".//p[@class='contents']")

        
        if (length(content_node) > 0) {
          for (l in 1:length(content_node)) {
            content_type <- XML::xmlAttrs(content_node[[l]][[1]])
            next_content_type <- if (l < length(content_node)) XML::xmlAttrs(content_node[[l+1]][[1]]) else NULL
            
            first_parag <- NULL
            
            # We have a new speaker taking the stand - first thing : get his info and then his first paragraph
            if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type ) {
              speaker_first_name <- NA
              speaker_last_name <- NA
              speaker_full_name <- NA
              speaker_full_name_native <- NA
              speaker_gender <- NA
              speaker_is_minister <- NA
              speaker_type <- NA
              speaker_mepid <- NA
              speaker_polgroup <- NA
              speaker_party <- NA
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
              
              hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,75), " – | − | - | — | - | - | – | ‒ ")[1]            
              
              if (is.na(hyphen_pos)) {
                hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,75), "\\)\\. ")[1] + 1
              }
              
              if (!is.na(hyphen_pos)) {
                speaker_text <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,hyphen_pos)))
                speaker_text <- gsub("\\.", "", speaker_text)
                speaker_text <- gsub('^(\u00a0*)','', speaker_text)
                #speaker_text <- gsub("\\((.*)", "", speaker_text)
                #speaker_text <- trimws(speaker_text)
              } else {
                speaker_text <- previous_speaker_full_name
              }
              
              
              if ( !is.na(speaker_text) && nchar(speaker_text) == 0 ) next
              
              # if ( length(which(dfInterventions$key == paste(event_id,"-",intervention_seqnum,sep=''))) > 0 ) 
              # {
              #   intervention_seqnum <- intervention_seqnum + 1
              #   next
              # }

              if (opt$hub_mode != "refresh") {
                tryCatch(
                  {
                    clessnverse::logit(scriptname, paste("checking if ",event_id, "-", intervention_seqnum, "beta"," already exists", sep-""), logger)
                    item_check <<- clessnhub::get_item(
                      'agoraplus_interventions', 
                      paste(event_id, "-", intervention_seqnum, "beta", sep="")
                    )

                    if (!is.null(item_check)) {
                      clessnverse::logit(
                        scriptname, 
                        paste(
                          "item ", 
                          event_id, "-", intervention_seqnum, "beta",
                          " already exists, skipping...", 
                          sep=""),
                        logger)

                      intervention_seqnum <- intervention_seqnum + 1
                      next
                    }
                  },
                  error=function(e) {},
                  finally={}
                )

              }

              #if (stringr::str_detect(speaker_text, ", ")) {
                
                if (length(stringr::str_split(speaker_text,",")[[1]]) == 2) {
                  speaker_full_name <- stringr::str_split(speaker_text,",")[[1]][1]
                  speaker_full_name <- trimws(speaker_full_name, "both")
                  
                  intervention_type <- stringr::str_split(speaker_text,",")[[1]][2:length(stringr::str_split(speaker_text,",")[[1]])]
                  intervention_type <- trimws(intervention_type)
                  intervention_type <- paste(intervention_type, collapse = ', ')
                  intervention_type <- trimws(intervention_type)                
                } else {
                  if (length(stringr::str_split(speaker_text,",")[[1]]) == 1) {
                    speaker_full_name <- speaker_text
                    intervention_type <- speaker_text 
                  } else {
                    speaker_full_name <- paste(stringr::str_split(speaker_text,",")[[1]][2], stringr::str_split(speaker_text,",")[[1]][1])
                    speaker_full_name <- trimws(speaker_full_name, "both")
                    
                    intervention_type <- stringr::str_split(speaker_text,",")[[1]][3:length(stringr::str_split(speaker_text,",")[[1]])]
                    intervention_type <- trimws(intervention_type)
                    intervention_type <- paste(intervention_type, collapse = ', ')
                    intervention_type <- trimws(intervention_type)
                  }
                }
                
                
                if ( !is.na(intervention_type) && intervention_type != "" && tolower(intervention_type) != "president" &&
                    !stringr::str_detect(tolower(intervention_type), "on behalf") &&
                    !stringr::str_detect(tolower(intervention_type), "member of") &&
                    (is.na(textcat::textcat(tolower(intervention_type))) ||
                    textcat::textcat(tolower(intervention_type)) != "english")
                    ) {

                  if (opt$translate && !is.null(intervention_type) && !is.na(intervention_type)) { 
                    #intervention_type <- clessnverse::translateText(intervention_type, engine="azure", target_lang="en",fake=!opt$translate)[2]
                    detected_lang <- detect_language(intervention_type)
                    if (detected_lang != "en") {
                      intervention_type_translated <- translate_language(intervention_type, detected_lang, "en")
                    } else {
                      intervention_type_translated <- intervention_type
                    }
                    if (!is.na(intervention_type_translated)) intervention_type <- intervention_type_translated
                  } else {
                    if (is.null(intervention_type) || is.na(intervention_type)) intervention_type <- NA_character_
                  }

                }
                
                
                if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteur") || 
                                                  stringr::str_detect(tolower(intervention_type), "reporter")) ) {
                  intervention_type <- NA
                  speaker_type <- "Reporter"
                }
                
                if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "auteur") ||
                                                  stringr::str_detect(tolower(intervention_type), "author")) ) {
                  intervention_type <- NA
                  speaker_type <- "Author"
                }
                  
                if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "membre") ||
                                                  stringr::str_detect(tolower(intervention_type), "member")) ) {
                  speaker_type <- intervention_type
                  intervention_type <- NA
                }
                
                if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "chancellor|president\\sof|minister|his\\sholiness|secretary|king") ) {
                  speaker_type <- intervention_type
                  intervention_type <- "Speech"
                }
                
                if ( !is.na(intervention_type) && (tolower(intervention_type) == "president" || tolower(intervention_type) == "chairman" )) {
                  intervention_type <- "moderation"
                  speaker_type <- "President"
                  speaker_full_name <- president_name
                } 
                
                if ( is.na(speaker_type) ) {
                  speaker_type <- intervention_type
                  intervention_type <- "Speech"
                }
                
              #} else {
              #  speaker_full_name <- speaker_text 
              #}
              
              speaker_text <- NA    
              
              speaker_full_name <- gsub(":", "", speaker_full_name)
              
              if ( stringr::str_detect(tolower(speaker_full_name), tolower(president_title)) || stringr::str_detect(tolower(speaker_full_name), tolower("ident")) ) {
                speaker_full_name <- president_name
                speaker_type <- "President"
              } else {
                speaker_full_name <- trimws(stringr::str_remove(speaker_full_name, "\\|\\s"))
              }
              
              if ( stringr::str_detect(speaker_full_name, "\\((.*)\\)") ) {
                  speaker_polgroup <- stringr::str_match(speaker_full_name, "\\((.*)\\)")[2]
                  speaker_full_name <- stringr::str_replace(speaker_full_name, "\\((.*)\\)", "")
                  speaker_full_name <- stringr::str_replace(gsub("\\s+", " ", stringr::str_trim(speaker_full_name)), "B", "b")
              }
              
              speaker_full_name <- trimws(speaker_full_name)
              speaker_full_name <- gsub('^(\u00a0*)','', speaker_full_name)
              
              speaker_full_name <- stringr::str_to_title(speaker_full_name)
              
              speaker_full_name_native  <- speaker_full_name
              
              # Get the speaker data from hub 2.0.  If absent try to get it from the parliament.
              # If parliament successful and not in hub, then write in hub for next time
              if ( !is.null(dfPersons) ) {
                dfSpeaker <- dfPersons[which(dfPersons$data.fullName == speaker_full_name),]
                if (nrow(dfSpeaker) == 0) {
                  dfSpeaker <- dfPersons[which(dfPersons$data.fullNameNative == speaker_full_name_native),]
                  if (nrow(dfSpeaker) == 0) {
                    dfSpeaker <- dfPersons[which(tolower(dfPersons$data.fullNameNative) == tolower(speaker_full_name_native)),]
                    if (nrow(dfSpeaker) == 0) {
                      dfSpeaker <- dfPersons[which(tolower(dfPersons$data.fullName) == tolower(speaker_full_name_native)),]
                    }
                  }
                }
              } else {
                dfSpeaker <- data_frame()
              }
              
              if (nrow(dfSpeaker) == 0 && !stringr::str_detect(rm_accent(tolower(speaker_full_name)), "president")) {
                # We could not find the speaker in the hub based on his/her full name.
                # Get it from the europe parliament web site
                # And then store it in the hub for next time
                dfSpeaker <- clessnverse::getEuropeMepData(speaker_full_name_native)
                
                if (!is.na(dfSpeaker$mepid)) {
                  # Found it on the europe parliament web site
                  speaker_full_name <- trimws(stringr::str_to_title(dfSpeaker$fullname))
                  speaker_first_name <- trimws(stringr::str_to_title(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]]))
                  speaker_last_name <- trimws(stringr::str_to_title(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2]))
                  speaker_mepid <- dfSpeaker$mepid
                  speaker_polgroup <- dfSpeaker$polgroup
                  speaker_party <- dfSpeaker$party
                  speaker_country <- dfSpeaker$country
                  skip_person_hub_write <- FALSE
                } else {
                  # Not found in hub NOR in parliament web site => translate
                  if (opt$translate == TRUE && !is.null(speaker_full_name) && !is.na(speaker_full_name)) {                    
                    detected_lang <- detect_language(speaker_full_name)
                    if (detected_lang != "en") {
                      speaker_full_name_translated <- translate_language(speaker_full_name, detected_lang, "en")
                    } else {
                      speaker_full_name_translated <- speaker_full_name
                    }
                    if (!is.na(speaker_full_name_translated)) speaker_full_name <- speaker_full_name_translated
                    #speaker_full_name <- clessnverse::translateText(speaker_full_name_native, engine="azure", target_lang="en", fake=!opt$translate)[2]
                  } 
                  
                  #cat("\ntranslating", speaker_full_name_native, "\n") #to", speaker_full_name, "\n")
                  speaker_first_name <- trimws(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]])
                  speaker_last_name <- trimws(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2])
                  
                  if (rm_accent(speaker_first_name) == "President") {
                    speaker_full_name <- president_name
                    speaker_first_name <- trimws(stringr::str_to_title(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]]))
                    speaker_last_name <- trimws(stringr::str_to_title(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2]))
                    skip_person_hub_write <- TRUE
                  } else {
                    skip_person_hub_write <- FALSE
                  }
                  
                }
                

                speaker_gender <- paste("", gender::gender(clessnverse::splitWords(speaker_first_name)[1])$gender, sep = "")
                if ( speaker_gender == "" ) speaker_gender <- NA
                
                speaker_is_minister <- NA
                speaker_district <- NA
                speaker_media <- NA
                
                
                if (!skip_person_hub_write) {
                  # Write it to the hub for next time
                  person_metadata_row <- list("source"="https://www.europarl.europa.eu/meps/fr/download/advanced/xml?name=",
                                          "country"=speaker_country,
                                          "institution"="European Parliament",
                                          "province_or_state"=speaker_country,
                                          "twitterAccountHasBeenScraped"="0"
                                          )
                  
                  person_data_row <- list("fullName"=speaker_full_name,
                                          "fullNameNative" = speaker_full_name_native,
                                          "isFemale"= if (!is.na(speaker_gender) && !is.null(speaker_gender) && speaker_gender=="female") as.character(1) else as.character(0),
                                          "lastName"=speaker_last_name,
                                          "firstName"=speaker_first_name,
                                          "twitterID"=NA_character_,
                                          "isMinister"="0",
                                          "twitterName"=NA_character_,
                                          "currentParty"=speaker_polgroup,
                                          "twitterHandle"=NA_character_,
                                          "currentMinister"=NA_character_,
                                          "currentPolGroup"=speaker_party,
                                          "twitterLocation"=NA_character_,
                                          "twitterPostsCount"=NA_character_,
                                          "twitterProfileURL"=NA_character_,
                                          "twitterListedCount"=NA_character_,
                                          "twitterFriendsCount"=NA_character_,
                                          "currentFunctionsList"=NA_character_,
                                          "twitterFollowersCount"=NA_character_,
                                          "currentProvinceOrState"=speaker_country,
                                          "twitterAccountVerified"=NA_character_,
                                          "twitterProfileImageURL"=NA_character_,
                                          "twitterAccountCreatedAt"=NA_character_,
                                          "twitterAccountCreatedOn"=NA_character_,
                                          "twitterAccountProtected"=NA_character_,
                                          "twitterProfileBannerURL"=NA_character_,
                                          "twitterUpdateDateStamps"=NA_character_,
                                          "twitterUpdateTimeStamps"=NA_character_
                                          )
                  
                  if ( is.na(speaker_mepid) ) speaker_mepid <- digest::digest(speaker_full_name)
                  
                  clessnverse::logit(scriptname=scriptname, message=paste("adding", speaker_full_name, "-", speaker_full_name_native, "to the hub"), logger = logger)
                  
                  tryCatch(
                    {
                      clessnhub::create_item("persons", paste("EU-",speaker_mepid,sep=''), "mp", "v3", person_metadata_row, person_data_row)
                    },
                    error= function(e) {
                      clessnhub::create_item("persons", digest::digest(speaker_full_name), "mp", "v3", person_metadata_row, person_data_row)
                    },
                    finally={}
                  )
                  
                  person_metadata_dfrow <- as.data.frame(person_metadata_row)
                  names(person_metadata_dfrow) <- paste("metadata.", names(person_metadata_dfrow),sep='')
                  person_data_dfrow <- as.data.frame(person_data_row)
                  names(person_data_dfrow) <- paste("data.", names(person_data_dfrow),sep='')
                  dfRow <- tibble::tibble(key=paste("EU-",speaker_mepid,sep=''), type="mp", schema="v3", uuid="") %>% cbind(person_metadata_dfrow) %>% cbind(person_data_dfrow)
    
                  if ( is.null(dfPersons) ) {
                    dfPersons <- dfRow
                    #dfPersons <- dfPersons %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ")
                  } else {
                    dfPersons <- dfPersons %>% rbind(dfRow)# %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " "))
                  }
                }    
              } else {
                # Found the speaker in the hub, use the one that has the most non NAs!
                dfSpeaker <- dfSpeaker[which(rowSums(is.na(dfSpeaker)) == min(rowSums(is.na(dfSpeaker))))[1],]

                speaker_mepid <- dfSpeaker$key
                speaker_full_name <- dfSpeaker$data.fullName
                speaker_first_name <- dfSpeaker$data.firstName
                speaker_last_name <- dfSpeaker$data.lastName
                speaker_polgroup <- dfSpeaker$data.currentParty
                speaker_party <- dfSpeaker$data.currentPolGroup
                speaker_country <- dfSpeaker$metadata.country
                speaker_gender <- if (dfSpeaker$data.isFemale == 1) "female" else "male"
                speaker_is_minister <- NA
                speaker_district <- NA
                speaker_media <- NA
              }
              
              #cat(speaker_full_name, speaker_full_name_native, "\n")
              
              if (!is.na(hyphen_pos)) {
                first_parag <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),hyphen_pos+2,nchar(XML::xmlValue(content_node[[l]])))))
                first_parag <- gsub("\\.", "", first_parag)
                first_parag <- gsub('^(\u00a0*)','', first_parag)
              } else {
                first_parag <- stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)–(\\s*)(.*)$")))[3]
                if (is.na(first_parag)) first_parag <- stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)−(\\s*)(.*)$")))[3]
              }
              
              
              if (length(first_parag) == 0) first_parag <- ""
              
              intervention_text <- ""
              
              #cat("first parag:",stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)–(\\s*)(.*)$")))[3],"\n")
            } else {
              if (opt$hub_mode != "refresh") {
                tryCatch(
                  {
                    clessnverse::logit(scriptname, paste("checking if ",event_id, "-", intervention_seqnum, "beta"," already exists", sep-""), logger)
                    item_check <<- clessnhub::get_item(
                      'agoraplus_interventions', 
                      paste(event_id, "-", intervention_seqnum, "beta", sep="")
                    )

                    if (!is.null(item_check)) {
                      clessnverse::logit(
                        scriptname, 
                        paste(
                          "item ", 
                          event_id, "-", intervention_seqnum, "beta",
                          " already exists, skipping...", 
                          sep=""),
                        logger)

                      intervention_seqnum <- intervention_seqnum + 1
                      next
                    }
                  },
                  error=function(e) {},
                  finally={}
                )
              }
            }# if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type )
            
            # Here we have an intervention - it is either  a new intervention (if first_parag is not null) or the continuation of the same intervention
            if ( is.null(content_type) || !is.null(first_parag) ) {

              if ( !is.null(first_parag) ) {
                #New
                intervention_text <- first_parag
              } else {
                #Continuation
                intervention_text <- paste(intervention_text, XML::xmlValue(content_node[[l]][[1]]), sep="\n\n")
              }
              
              if ( !is.na(intervention_text) && (intervention_text == "\n\n ") ) intervention_text <- ""
              
              first_parag <- NULL
            }
            
            if ( !is.null(intervention_type) && !is.na(intervention_type) ) {
              intervention_type <- gsub("\\(", "", intervention_type)
              intervention_type <- gsub("\\)", "", intervention_type)
              intervention_type <- gsub("\"", "", intervention_type)
              intervention_type <- gsub("\\\\u2012", "", intervention_type)
              intervention_type <- gsub("\\\\ U2012", "", intervention_type)
              intervention_type <- stringr::str_squish(intervention_type)
              intervention_type <- trimws(intervention_type)
              intervention_type <- stringr::str_to_title(intervention_type)
            }
            
            if ( !is.null(speaker_type) && !is.na(speaker_type) ) {
              speaker_type <- trimws(speaker_type)
            }
            
            # Here it's only procedural text
            #if (!is.null(content_type) && content_type == "italic") {
            #  cat("procedural text: ",XML::xmlValue(content_node[[l]][[1]]),"\n")
            #  next
            #}
            
            # Look at the next paragraph to see if it is a new intervention
            #if ( (is.null(next_content_type) || "/doceo/data/img/arrow_summary.gif" %in% next_content_type) &&  !is.na(intervention_text) ) {
            if ( "/doceo/data/img/arrow_summary.gif" %in% next_content_type || l == length(content_node) && !is.na(intervention_text) && intervention_text != "" ) {
              # next is new
              #if (stringr::str_detect(speaker_full_name, "sreekanth")) stop("bingo")
              
              intervention_text <- trimws(intervention_text, "left")
              intervention_word_count <- nrow(tidytext::unnest_tokens(tibble::tibble(txt=intervention_text), word, txt, token="words",format="text"))
              intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble::tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
              intervention_paragraph_count <- stringr::str_count(intervention_text, "\n\n") + 1
              

              # Translation
              if (grepl("\\\\", intervention_text)) intervention_text <- gsub("\\\\"," ", intervention_text)
              intervention_text <- gsub("^NA\n\n", "", intervention_text)
              intervention_text <- gsub("^\n\n", "", intervention_text)
              
              if (opt$translate && !is.null(intervention_text) && !is.na(intervention_text)) {
                detected_lang <- detect_language(substring(intervention_text,1,500))
                intervention_lang <- detected_lang
                if (detected_lang != "en") {
                  intervention_translated_text <- translate_language(intervention_text, detected_lang, "en")
                } else {
                  intervention_translated_text <- intervention_text
                }
                clessnverse::logit(
                  scriptname, 
                  paste(
                    "translated\n", 
                    substring(intervention_text, 1, 20), 
                    "\nto\n", 
                    substring(intervention_translated_text, 1, 20)
                  ), 
                  logger
                )
              } else {
                detected_lang <- "xx"
                intervention_translated_text <- "not transpated because translation if turned offs"
              }

              

              #intervention_translation <- clessnverse::translateText(text=intervention_text, engine="azure", target_lang="en", fake=!opt$translate)
              #intervention_lang <- stringr::str_to_upper(intervention_translation[1])
              #intervention_translated_text <- intervention_translation[2]
              
              if ( is.na(intervention_lang) ) {
                intervention_lang <- textcat::textcat(intervention_text)
                intervention_lang <- stringr::str_to_upper(stringr::str_sub(dfCountryLanguageCodes$Two.Letter[which(tolower(dfCountryLanguageCodes$Language) == tolower(intervention_lang))[1]],1,2))
                intervention_lang <- toupper(intervention_lang)
              }
              
              if (is.null(intervention_translated_text)) intervention_translated_text <- NA
              
              # commit to dfDeep and the Hub
              v2_row_to_commit <- data.frame(eventID = event_id,
                                            eventDate = event_date,
                                            eventStartTime = event_start_time,
                                            eventEndTime = event_end_time,
                                            eventTitle = event_title,
                                            eventSubTitle = event_subtitle,
                                            interventionSeqNum = paste(intervention_seqnum, "beta", sep=""),
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
                                            speakerFullNameNative = speaker_full_name_native,
                                            speakerGender = speaker_gender,
                                            speakerType = speaker_type,
                                            speakerCountry = speaker_country,
                                            speakerIsMinister = speaker_is_minister,
                                            speakerPolGroup = speaker_polgroup,
                                            speakerParty = speaker_party,
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

              v2_metadata_to_commit <- list("url"=event_url, "format"="html", "location"="EU",
                                            "parliament_number"=parliament_number, "parliament_session"=NA_character_)

              clessnverse::logit(scriptname, "committing row", logger)

              dfInterventions <- clessnverse::commitAgoraplusInterventions(dfDestination = dfInterventions,
                                                                          type = "parliament_debate", schema = "v3beta",
                                                                          metadata = v2_metadata_to_commit,
                                                                          data = v2_row_to_commit,
                                                                          opt$dataframe_mode, opt$hub_mode)



              clessnverse::logit(scriptname, "committing row done", logger)
              intervention_seqnum <- intervention_seqnum + 1

              previous_speaker_full_name <- speaker_full_name
              intervention_text <- ""
            } else {
              # Next is same speaker
            } #if ( "/doceo/data/img/arrow_summary.gif" %in% next_content_type || l == length(content_node) && !is.na(intervention_text) ) {
            
          } #for (l in 1:length(content_node))
        } #if (length(content_node) > 0)
      } # for (k in 1:length(chapter_nodes_list))
    } # if ("a" %in% chapter_nodes_list)
  
  } #for (j in 2:core_xml_nbchapters)
  
} #for (i in 1:length(urls_list))


clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)

