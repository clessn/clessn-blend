###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                             agora-plus-canada                               #
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

if (!exists("scriptname")) scriptname <- "parliament_mash_canada"

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

opt <- list(dataframe_mode = "rebuild", log_output = "console", hub_mode = "update", download_data = FALSE, translate=TRUE)


if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}

if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))


# Download HUB v2 data
if (opt$dataframe_mode %in% c("update","refresh")) {
  clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
  dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "parliament_debate", schema = "v2", 
                                                               location = "CA", format = "html",
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
metadata_filter <- list(institution="House of Commons of Canada")
filter <- clessnhub::create_filter(type="mp", schema="v2", metadata=metadata_filter)  
dfPersons <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)


# Load all objects used for ETL
clessnverse::loadETLRefData()


###############################################################################
# Data source
#
# connect to the dataSource : the federal parliament web site 
# get the index page containing the URLs to all the national assembly debates
# to extract those URLS and get them individually in order to parse
# each debate
#

scraping_method <- "SessionRange"
#scraping_method <- "Latest"
base_url <- "https://www.noscommunes.ca"
hansard_url1 <- "/Content/House"
hansard_url2 <- "/Debates"
hansard_url3 <- "/HAN"
hansard_url4fr <- "-F.XML"
hansard_url4en <- "-E.XML"


if (scraping_method == "Latest") {
  content_url <- "/PublicationSearch/fr/?PubType=37&xml=1"

  source_page <- NULL

  i_get_attempt <- 1
  
  while (is.null(source_page)  && i_get_attempt <= 20) {
    tryCatch(
      {
        source_page <- httr::GET(paste(base_url,content_url,sep=''))
      },
      error = function(e) {
        print(paste("error getting hansard page ", base_url, content_url, sep=''))
        print(e)
        quit(status=1)
      }, 
      finally = {
      }
    )
  }

  source_page_html <- httr::content(source_page)
  source_page_xml <- XML::xmlParse(source_page_html, useInternalNodes = TRUE)

  root_xml <- XML::xmlRoot(source_page_xml)
  head_xml <- root_xml[[1]]
  core_xml <- root_xml[[2]]

  latest_handsards <- as.list(XML::xmlApply(root_xml[["Publications"]], XML::xmlAttrs))
  latest_handsards <- lapply(latest_handsards, as.list)

  urls_list_fr <- list()
  urls_list_en <- list()

  for (h in 1:length(latest_handsards)) {
    hansard_num <- stringr::str_pad(trimws(strsplit(latest_handsards[h]$Publication$Title, "-")[[1]][2]), 3, side = "left", pad = "0")

    session_num <- latest_handsards[h]$Publication$Session
    parliam_num <- latest_handsards[h]$Publication$Parliament

    url_fr <- paste(base_url, hansard_url1,"/",parliam_num,session_num,hansard_url2,"/",hansard_num,hansard_url3,hansard_num,hansard_url4fr,sep='')
    urls_list_fr <- c(urls_list_fr, url_fr)
    url_en <- paste(base_url, hansard_url1,"/",parliam_num,session_num,hansard_url2,"/",hansard_num,hansard_url3,hansard_num,hansard_url4en,sep='')
    urls_list_en <- c(urls_list_en, url_en)
  } 

}

if (scraping_method == "SessionRange") {
  content_url <- "Content/House"
  start_parliam <- 43
  nb_parliam <- 1
  start_session <- 1
  nb_session <- 1
  start_seance <- 041
  nb_seance <- 1

  urls_list_fr <- c()
  urls_list_en <- c()

  for (p in start_parliam:(start_parliam+nb_parliam-1)) {
    for (s in start_session:(start_session+nb_session-1)) {
      for (seance in start_seance:(start_seance+nb_seance-1)) {
        url_fr <- paste(base_url,hansard_url1,"/", toString(p), toString(s),hansard_url2,"/",stringr::str_pad(toString(seance),3, side = "left", pad = "0"), hansard_url3,stringr::str_pad(toString(seance),3, side = "left", pad = "0"),hansard_url4fr,sep= '')
        urls_list_fr <- append(urls_list_fr, url_fr)
        url_en <- paste(base_url,hansard_url1,"/", toString(p), toString(s),hansard_url2,"/",stringr::str_pad(toString(seance),3, side = "left", pad = "0"), hansard_url3,stringr::str_pad(toString(seance),3, side = "left", pad = "0"),hansard_url4en,sep= '')
        urls_list_en <- append(urls_list_en, url_en)
      }
    }
  }
}



###############################################################################
########################               MAIN              ######################
###############################################################################

###############################################################################
# Let's get serious!!!
# Run through the URLs list, get the html content from the cache if it is 
# in it, or from the assnat website and start parsing it to extract the
# debates content
#
for (i_url in 1:length(urls_list_fr)) {
  current_url_fr <- urls_list_fr[[i_url]]
  #event_id <- stringr::str_replace_all(urls_list_fr[i_url], "[[:punct:]]", "")
  event_id <- paste(stringr::str_match(urls_list_fr[i_url], "\\/(\\d+)?\\/[a-zA-Z]+\\/(\\d+)?\\/(.*)?-[E|F]\\.XML$")[2:4], collapse="")
  

  clessnverse::logit(scriptname, paste("Debate", i_url, "of", length(urls_list_fr), event_id, sep = " "), logger)
  clessnverse::logit(scriptname, current_url_fr, logger)

  event_check <- NULL
  tryCatch(
    {event_check <- clessnhub::get_item('agoraplus_interventions', paste(event_id, "-1", sep=""))},
    error = function(e) {event_check <- NULL}, 
    finally = {}
  )

  if (!is.null(event_check) && stringr::str_detect(event_check$key, event_id) ||
      TRUE %in% stringr::str_detect(dfInterventions$key, event_id)) {
      clessnverse::logit(scriptname, paste("Debate", i_url, "of", length(urls_list_fr), "event_id=", event_id, "ALREADY EXISTS.  Skipping...", sep = " "), logger)
      next
  }

  # Read and parse HTML from the URL directly
  r_fr <- NULL
  r_en <- NULL

  i_get_attempt <- 1
  
  while(is.null(r_fr) && i_get_attempt <= 20) { r_fr <- safe_GET(current_url_fr) }

  if (!is.null(r_fr$result) && r_fr$result$status_code == 200) {
    current_url_en <- urls_list_en[[i_url]]

    i_get_attempt <- 1

    while(is.null(r_en) && i_get_attempt <= 20) { 
      tryCatch(
        {
          r_en <- safe_GET(current_url_en)
        },
        error = function(e) {
          print(paste("error getting hansard page ", base_url, content_url, sep=''))
          print(e)
          quit(status=1)
        }, 
        finally = {}
      ) 
    }

    if (r_en$result$status_code == 200) {
      doc_html_en <- httr::content(r_en$result, encoding = "UTF-8")
      doc_xml_en <- XML::xmlParse(doc_html_en, useInternalNodes = TRUE)
      top_xml_en <- XML::xmlRoot(doc_xml_en)
      title_xml_en <- top_xml_en[["DocumentTitle"]]
      header_xml_en <- top_xml_en[["ExtractedInformation"]]
      hansard_body_xml_en <- top_xml_en[["HansardBody"]]
    } else {
      if (!is.null(r_en$result)) {
        clessnverse::logit(scriptname, paste("Could not GET", current_url_en, "error code", r_en$result$status_code, sep = " "), logger)
      } else {
        clessnverse::logit(scriptname, paste("Could not GET", current_url_en, sep = " "), logger)
      }
      next  
    }
    doc_html_fr <- httr::content(r_fr$result, encoding = "UTF-8")
    doc_xml_fr <- XML::xmlParse(doc_html_fr, useInternalNodes = TRUE)
    top_xml_fr <- XML::xmlRoot(doc_xml_fr)
    title_xml_fr <- top_xml_fr[["DocumentTitle"]]
    header_xml_fr <- top_xml_fr[["ExtractedInformation"]]
    hansard_body_xml_fr <- top_xml_fr[["HansardBody"]]
    cached_html <- FALSE
  } else {
    if (!is.null(r_fr$result)) {
      clessnverse::logit(scriptname, paste("Could not GET", current_url_fr, "error code", r_fr$result$status_code, sep = " "), logger)
    } else {
      clessnverse::logit(scriptname, paste("Could not GET", current_url_fr, sep = " "), logger)
    }
    next
  }

  # Get the length of all branches of the XML document
  header_xml_fr_length <- length(names(header_xml_fr))
  hansard_body_xml_fr_length <- length(names(hansard_body_xml_fr))

  for (i_header_child_node in 1:header_xml_fr_length) {
    header_child_node_attr <- XML::xmlGetAttr(header_xml_fr[[i_header_child_node]], "Name")

    if (header_child_node_attr == "Institution") event_source_type <- paste(XML::xmlValue(header_xml_fr[[i_header_child_node]]), " | Parlement du Canada", sep='')
    if (header_child_node_attr == "InstitutionDebate") event_title <- trimws(XML::xmlValue(header_xml_fr[[i_header_child_node]]))
    if (header_child_node_attr == "MetaVolumeNumber") event_volume_number <- XML::xmlValue(header_xml_fr[[i_header_child_node]])
    if (header_child_node_attr == "MetaNumberNumber") event_hansard_number <- XML::xmlValue(header_xml_fr[[i_header_child_node]])
    if (header_child_node_attr == "ParliamentNumber") event_parliam_number <- XML::xmlValue(header_xml_fr[[i_header_child_node]])
    if (header_child_node_attr == "SessionNumber") event_session_number <- XML::xmlValue(header_xml_fr[[i_header_child_node]])
    if (header_child_node_attr == "MetaCreationTime") event_date_time <- XML::xmlValue(header_xml_fr[[i_header_child_node]])
  }

  #####################################
  # Columns common to all interventions
  event_source_type <- event_source_type
  event_url <- current_url_fr
  event_date <- substr(event_date_time,1,10)
  event_start_time <- substr(event_date_time,12,19)

  event_end_time <- stringr::str_match(XML::xmlValue(hansard_body_xml_fr[[hansard_body_xml_fr_length]]), "\\(La séance est levée à\\s(.*)?\\.\\)$")[2]
  event_end_time <- gsub("\u00a0", " ", event_end_time)
  event_end_time <- gsub(" h ", ":", event_end_time)
  if (length(event_end_time) < 8 ) event_end_time <- paste(event_end_time, ":00", sep = "")

  event_title <- paste( "parliament number",event_parliam_number,"| session",event_session_number,sep=' ')
  event_subtitle <- paste(event_volume_number,"| Hansard number",event_hansard_number,sep=' ')
  event_word_count <- NA
  event_sentence_count <- NA
  event_paragraph_count <- NA
  event_content <- NA
  event_translated_content <- NA


  ##########################################
  # The colums specific to each intervention
  oob_id <- NA
  oob_rubric <- NA
  oob_title <- NA
  oob_seqnum <- NA
  sob_id <- NA
  sob_title <- NA
  sob_header <- NA
  sob_seqnum <- NA
  sob_procedural_text <- NA
  sob_doc_id <- NA
  sob_doc_title <- NA
  intervention_seqnum <- NA
  speaker_first_name <- NA
  speaker_last_name <- NA
  speaker_full_name <- NA
  speaker_gender <- NA
  speaker_id <- NA
  speaker_is_minister <- NA
  speaker_type <- NA
  speaker_party <- NA
  speaker_district <- NA
  speaker_province <- NA
  speaker_media <- NA
  intervention_doc_id <- NA
  intervention_doc_title <- NA
  intervention_type <- NA
  intervention_lang <- NA
  intervention_timestamp <- NA
  intervention_word_count <- NA
  intervention_sentence_count <- NA
  intervention_paragraph_count <- NA
  intervention_text <- NA
  intervention_translated_text <- NA

  dfSpeaker <- data.frame()

  ##########################################################
  # Go through the xml document subject of business by 
  # subject of business and count them per order of business 
  nb_interventions <- length(gregexpr('(?=<Intervention)', XML::toString.XMLNode(hansard_body_xml_fr), perl=TRUE)[[1]])



  oob_seqnum <- 0
  sob_seqnum <- 0
  intervention_seqnum <- 0

  # pb_chap <- txtProgressBar(min = 0,      # Minimum value of the progress bar
  #                           max = nb_interventions, # Maximum value of the progress bar
  #                           style = 3,    # Progress bar style (also available style = 1 and style = 2)
  #                           width = 80,   # Progress bar width. Defaults to getOption("width")
  #                           char = "=")   # Character used to create the bar

  event_content <- ""
  event_translated_content <- ""

  current_speaker_full_name <- ""

  # Now we go through every child node of the hansard body and parse
  # i_oob is the Order of Business item index (l'index de l'item dans l'ordre du jour)

  for (i_oob in 1:length(names(hansard_body_xml_fr))) {

    # New oob
    oob_node <- hansard_body_xml_fr[[i_oob]]

    # At this point we have the Intro or an order of business item
    oob_seqnum <- oob_seqnum + 1

    if (XML::xmlName(oob_node) == "Intro") {
      oob_id <- "Intro"
      oob_rubric <- "Intro"
      oob_title <- "Intro"
      oob_catchline <- "Intro"
    } else {
      oob_id <- if(!is.null(XML::xmlGetAttr(oob_node, "id"))) XML::xmlGetAttr(oob_node, "id") else NA_character_
      oob_rubric <- if(!is.null(XML::xmlGetAttr(oob_node, "Rubric"))) trimws(XML::xmlGetAttr(oob_node, "Rubric")) else NA_character_
      oob_title <- if(!is.null(XML::xmlValue(oob_node[["OrderOfBusinessTitle"]]))) trimws(XML::xmlValue(oob_node[["OrderOfBusinessTitle"]])) else NA_character_
      oob_catchline <- if(!is.null(XML::xmlValue(oob_node[["CatchLine"]]))) XML::xmlValue(oob_node[["CatchLine"]]) else NA_character_

      if (is.na(oob_title) && !is.na(oob_catchline)) oob_title <- oob_catchline

      if (!is.na(oob_catchline) && !is.na(oob_title) && tolower(oob_catchline) != tolower(oob_title)) {
        oob_title <- paste(oob_title, ": ", oob_catchline, sep='')
      }
    }

    if (!is.null(oob_title) && !is.na(oob_title))  oob_title <- stringr::str_to_title(oob_title)


    oob_sob_list <- names(oob_node)

    # Then we get into the subjects of business
    if ( "SubjectOfBusiness" %in% oob_sob_list ) {
      # We have to loop through every subject of business but first
      # let's see if there is any floor language tag (anomaly seen in hansard 31 of session 43-1)
      if ("FloorLanguage" %in% oob_sob_list) {
        sob_node <- oob_node[[which(oob_sob_list == "FloorLanguage")]]
        if (!is.null(XML::xmlGetAttr(sob_node, "language"))) {
          intervention_lang <- tolower(XML::xmlGetAttr(sob_node, "language")) 
        } else {
          intervention_lang <- NA_character_
        }
      }

      intervention_id <- NA

      for (i_sob in which(oob_sob_list == "SubjectOfBusiness")) {
        sob_id <- NA
        sob_title <- NA
        sob_header <- NA
        sob_procedural_text <- NA
        sob_doc_id <- NA
        sob_doc_title <- NA

        sob_seqnum <- sob_seqnum + 1

        # Strip out the sob header info
        sob_node <- oob_node[[i_sob]]

        sob_id <- if (!is.null(XML::xmlGetAttr(sob_node, "id"))) XML::xmlGetAttr(sob_node, "id") else NA_character_


        if (length(grep("FloorLanguage", names(sob_node))) > 0) {
          if (!is.null(XML::xmlGetAttr(sob_node[["FloorLanguage"]], "language"))) {
            intervention_lang <- tolower(XML::xmlGetAttr(sob_node[["FloorLanguage"]], "language")) 
          } else {
            intervention_lang <- NA_character_
          }
        }

        sob_title <- if (!is.null(XML::xmlValue(sob_node[["SubjectOfBusinessTitle"]]))) trimws(XML::xmlValue(sob_node[["SubjectOfBusinessTitle"]])) else NA_character_
        if (is.na(sob_title)) {
          sob_title <- if (!is.null(XML::xmlValue(sob_node[["SubjectOfBusinessQualifier"]]))) trimws(XML::xmlValue(sob_node[["SubjectOfBusinessQualifier"]])) else NA_character_
        }


        sob_procedural_text <- if (!is.null(XML::xmlValue(sob_node[["ProceduralText"]]))) trimws(XML::xmlValue(sob_node[["ProceduralText"]])) else NA_character_

        # Then we start stripping the content out by going through each node sequentially
        sob_content_node <- sob_node[["SubjectOfBusinessContent"]]

        for (i_sob_content in 1:length(names(sob_content_node))) {

          # First we might have a header
          if (XML::xmlName(sob_content_node[[i_sob_content]]) == "ParaText") {
            sob_header <- XML::xmlValue(sob_content_node[[i_sob_content]])
            # See if there is a document refered in it
            if ("Document" %in% names(sob_content_node[[i_sob_content]])) {
              sob_doc_title <- trimws(XML::xmlValue(sob_content_node[[i_sob_content]][["Document"]]))
              sob_doc_id <- XML::xmlGetAttr(sob_content_node[[i_sob_content]][["Document"]], "DbId")
            }
          }

          # we might have a language change
          if (XML::xmlName(sob_content_node[[i_sob_content]]) == "FloorLanguage") {
            intervention_lang <- tolower(XML::xmlGetAttr(sob_content_node[[i_sob_content]], "language"))
          }

          # we might have a timestamp
          if (XML::xmlName(sob_content_node[[i_sob_content]]) == "Timestamp") {
            intervention_timestamp <- paste(XML::xmlGetAttr(sob_content_node[[i_sob_content]], "Hr"),
                                            XML::xmlGetAttr(sob_content_node[[i_sob_content]], "Mn"),
                                            "00",
                                            sep=":")
          }

          # we might have a procedural text
          if (XML::xmlName(sob_content_node[[i_sob_content]]) == "ProceduralText") {
            sob_procedural_text <- XML::xmlValue(sob_content_node[[i_sob_content]])
          }


          # We might have an intervention
          if (XML::xmlName(sob_content_node[[i_sob_content]]) == "Intervention") {
            speaker_first_name <- NA
            speaker_last_name <- NA
            speaker_full_name <- NA
            speaker_gender <- NA
            speaker_id <- NA
            speaker_is_minister <- NA
            speaker_type <- NA
            speaker_party <- NA
            speaker_district <- NA
            speaker_province <- NA
            speaker_media <- NA
            intervention_doc_id <- NA
            intervention_doc_title <- NA
            intervention_type <- NA
            #intervention_lang <- NA
            intervention_timestamp <- NA
            intervention_word_count <- NA
            intervention_sentence_count <- NA
            intervention_paragraph_count <- NA
            intervention_text <- NA
            intervention_translated_text <- NA

            intervention_seqnum <- intervention_seqnum + 1
            # setTxtProgressBar(pb_chap, intervention_seqnum)

            # Type of the intervention
            intervention_type <- XML::xmlGetAttr(sob_content_node[[i_sob_content]], "Type")

            intervention_id <- XML::xmlGetAttr(sob_content_node[[i_sob_content]], "id")

            intervention_node <- sob_content_node[[i_sob_content]]

            intervention_content_node <- intervention_node[["Content"]]


            # Identify the speaker and speaker type
            speaker_node <- intervention_node[["PersonSpeaking"]]
            speaker_type <- XML::xmlGetAttr(speaker_node[["Affiliation"]], "Type")
            if (!is.null(speaker_type)) {
              speaker_type <- dplyr::case_when(speaker_type == "18" ~ "Secrétaire parlementaire",
                                               speaker_type == "2"  ~ "Député(e)",
                                               speaker_type == "13" ~ "Député(e)",
                                               speaker_type == "1"  ~ "Premier ministre",
                                               speaker_type == "15" ~ "Président",
                                               speaker_type == "9"  ~ "Chef de l'opposition",
                                               speaker_type == "10"  ~ "Chef de l'opposition",
                                               speaker_type == "4"  ~ "Ministre",
                                               speaker_type == "20" ~ "Ministre",
                                               speaker_type == "96" ~ "Ministre",
                                               speaker_type == "7"  ~ "Leader parlementaire de l'opposition",
                                               speaker_type == "93" ~ "Vice-président(e) adjoint(e)",
                                               speaker_type == "92" ~ "Vice-président(e) adjoint(e)",
                                               speaker_type == "22" ~ "Vice-président(e)",
                                               speaker_type == "12" ~ "Whip en chef du gouvernement",
                                               speaker_type == "60056" ~ "Leader adjoint(e) du gouvernement à la Chambre des communes",
                                               TRUE ~ speaker_type) 
            } else {
              speaker_type <- NA
            }


            #if (!is.na(speaker_type)) {
            #  if (!is.na(speaker_type) && (speaker_type == "Ministre" || speaker_type == "Premier ministre")) speaker_is_minister <- 1
            #}

            speaker_id <- XML::xmlGetAttr(speaker_node[["Affiliation"]], "DbId")
            speaker_value <- XML::xmlValue(speaker_node[["Affiliation"]])

            #title_patterns <- stringr::str_match(tolower(speaker_value), tolower(patterns_titres))

            speaker_full_name <- speaker_value

            if (length(grep(tolower("vice-présidente? adjointe?"), tolower(speaker_value))) > 0) {
              if (length(grep("^(.*)\\(M(.|me|adame) (.*)\\)", speaker_value)) > 0) {
                name_matches <- stringr::str_match(speaker_value, "^(.*)\\(M(.|me|adame) (.*)\\)")
                speaker_full_name <- name_matches[length(name_matches)]
                speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
                speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
              } 
            }

            if (length(grep("le vice-président", tolower(speaker_value))) > 0) {
              name_matches <- ""
              name_matches[1] <- ""
              name_matches[2] <- "M."
              speaker_full_name = ""
              speaker_first_name = ""
              speaker_last_name = "Stanton"
            }

            if (length(grep("la vice-présidente", tolower(speaker_value))) > 0) {
              name_matches <- ""
              name_matches[1] <- ""
              name_matches[2] <- "Mme"
              speaker_type <- "Vice-président(e)"

              speaker_full_name <- "Vice Président(e)"
              speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
              speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
            }

            if (length(grep("la vice-présidente adjointe", tolower(speaker_value))) > 0) {
              speaker_type <- "Vice-président(e)"

              if (length(grep("(L|l)a (V|v)ice-présidente (A|a)djointe\\s\\(Mme\\s(.*)\\)", speaker_value)) > 0) {
                name_matches <- stringr::str_match(speaker_value, "(L|l)a (V|v)ice-présidente (A|a)djointe\\s\\(Mme\\s(.*)\\)")
                speaker_type <- " Vice-président(e) adjoint(e)"
                speaker_full_name <- name_matches[length(name_matches)]
                speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
                speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
              } else {
                speaker_full_name <- "Vice Président(e)"
                speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
                speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
              }
            }

            if (length(grep("Le président suppléant", speaker_value)) > 0) {
              if (length(grep("Le président suppléant\\s\\((M\\.|Monsieur|Mr\\.)\\s(.*)\\)", speaker_value)) > 0) {
                name_matches <- stringr::str_match(speaker_value, "Le président suppléant\\s\\((M\\.|Monsieur|Mr\\.)\\s(.*)\\)")
                speaker_type <- "Vice-président(e) suppléant(e)"
                speaker_full_name <- name_matches[length(name_matches)]
                speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
                speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
              } 
            }

            if (length(grep("La présidente suppléante", speaker_value)) > 0) {
              if (length(grep("La présidente suppléante\\s\\((Mme|Madame)\\s(.*)\\)", speaker_value)) > 0) {
                name_matches <- stringr::str_match(speaker_value, "La présidente suppléante\\s\\((Mme|Madame)\\s(.*)\\)")
                speaker_type <- "Vice-président(e) suppléant(e)"
                speaker_full_name <- name_matches[length(name_matches)]
                speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
                speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
              }
            }

            if (length(grep("le président", tolower(speaker_value))) > 0) {
              name_matches <- ""
              name_matches[1] <- ""
              name_matches[2] <- "M."
              speaker_full_name <- "Anthony Rota"
              speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
              speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
            }

            if (length(grep("le greffier de la chambre", tolower(speaker_value))) > 0) {
              name_matches <- ""
              name_matches[1] <- ""
              name_matches[2] <- "M."
              speaker_type <- "Greffier de la chambre"
              speaker_full_name <- "Undetermined Speaker"
              speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
              speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
            }

            if (length(grep("L.?hon.", speaker_value)) > 0) {
              if (length(grep("^L.?hon.\\s(.*)\\s\\((.*)\\((.*)\\)(.*)\\)", speaker_value)) > 0)
                name_matches <- stringr::str_match(speaker_value, "^L.?hon.\\s(.*)\\s\\((.*)\\((.*)\\)(.*)\\)")
              else
                if (length(grep("^L.?hon.\\s(.*)\\s\\((.*)\\)", speaker_value)) > 0)
                  name_matches <- stringr::str_match(speaker_value, "^L.?hon.\\s(.*)\\s\\((.*)\\)")
                else
                  if (length(grep("^L.?hon.\\s(.*)", speaker_value)) > 0)
                    name_matches <- stringr::str_match(speaker_value, "^L.?hon.\\s(.*)")
                  else
                    if (length(grep("^L.?hon(.*))\\s(.*)\\s()", speaker_value)) > 0)
                      name_matches <- stringr::str_match(speaker_value, "^L.?hon.\\s(.*)")
                    else
                      if (length(grep("^L.?hon(.*)\\s(.*)\\s()", speaker_value)) > 0)
                        name_matches <- stringr::str_match(gsub("rable ","",speaker_value), "^L.?hon.(.*)\\s()")

              #name_matches <- stringr::str_match(speaker_value, "^L.?hon.\\s(.*)\\s\\((.*)\\)")
              speaker_full_name <- name_matches[2]
              speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
              speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
            }

            if (length(grep("Le très hon.", speaker_value)) > 0) {
              name_matches <- stringr::str_match(speaker_value, "^Le très hon.\\s(.*)\\s\\((.*)\\)")
              speaker_full_name <- name_matches[2]
              speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
              speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
            }

            if (length(grep("^(M\\.|Mme|Mr\\.)\\s(.*)", speaker_value)) > 0) {
              if (length(grep("^(M\\.|Mme|Mr\\.)\\s(.*)\\s\\((.*)\\((.*)\\)(.*)\\((.*)\\)(.*)\\)", speaker_value)) > 0)
                # string de type M. Nom Prénom (blah blah (blah blah) blah blah (blah blah blah) blah blah)
                name_matches <- stringr::str_match(speaker_value, "^(M\\.|Mme|Mr\\.)\\s(.*)\\s\\((.*)\\((.*)\\)(.*)\\((.*)\\)(.*)\\)")
              else
                if (length(grep("^(M\\.|Mme|Mr\\.)\\s(.*)\\s\\((.*)\\((.*)\\)(.*)\\)", speaker_value)) > 0)
                  # string de type M. Nom Prénom (blah blah blah) blah blah )
                  name_matches <- stringr::str_match(speaker_value, "^(M\\.|Mme|Mr\\.)\\s(.*)\\s\\((.*)\\((.*)\\)(.*)\\)")
                else
                  if (length(grep("^(M\\.|Mme|Mr\\.)\\s(.*)\\s\\((.*)\\)", speaker_value)) > 0)
                    # string de type M. Nom Prénom (blah blah)
                    name_matches <- stringr::str_match(speaker_value, "^(M\\.|Mme|Mr\\.)\\s(.*)\\s\\((.*)\\)")
                  else
                    if (length(grep("^(M\\.|Mme|Mr\\.)\\s(.*)", speaker_value)) > 0)
                      # string de typr M Nom Prénom
                      name_matches <- stringr::str_match(speaker_value, "^(M\\.|Mme|Mr\\.)\\s(.*)")

              speaker_full_name <- name_matches[3]
              speaker_first_name <- strsplit(speaker_full_name," ")[[1]][1]
              speaker_last_name <- paste(strsplit(speaker_full_name," ")[[1]][2:length(strsplit(speaker_full_name," ")[[1]])],collapse = ' ')
            }

            if (!is.null(speaker_first_name) && !is.na(speaker_first_name)) {
                if (!is.na(name_matches[2]) && name_matches[2] == "M.") speaker_gender <- "M"
                if (!is.na(name_matches[2]) && name_matches[2] == "Mme") speaker_gender <- "F"
                if (is.na(name_matches[2]) || name_matches[2] != "M." && name_matches[2] != "Mme") {
                  speaker_gender <- gender::gender(speaker_first_name)$gender
                  speaker_gender <- dplyr::case_when(speaker_gender == "male" ~ "M",
                                                     speaker_gender == "female" ~ "F",
                                                     TRUE ~ NA_character_
                                                     )
                  if (length(speaker_gender) == 0) speaker_gender <- NA_character_
                }
            }

            speaker_first_name <- trimws(speaker_first_name, "both")
            speaker_last_name <- trimws(speaker_last_name, "both")
            speaker_full_name <- trimws(speaker_full_name, "both")

            if (!is.na(speaker_full_name) && stringr::str_detect(speaker_full_name, "’")) speaker_full_name <- gsub("’", "'",speaker_full_name) 
            if (!is.na(speaker_first_name) && stringr::str_detect(speaker_first_name, "’")) speaker_first_name <- gsub("’", "'",speaker_first_name) 
            if (!is.na(speaker_last_name) && stringr::str_detect(speaker_last_name, "’")) speaker_last_name <- gsub("’", "'",speaker_last_name) 

            dfSpeaker <- clessnverse::getCanadaMepData(speaker_full_name)

            speaker_district <- dfSpeaker$district
            speaker_province <- dfSpeaker$province
            speaker_is_minister <- if (!is.na(speaker_type) && (speaker_type == "Premier ministre" || speaker_type == "Ministre")) 1 else 0
            speaker_party <- dfSpeaker$party

            #if (is.na(speaker_party) && speaker_full_name != "Undetermined Speaker") { stop("bingo") }

            # Now run through the intervention content and build the intervention with paragraphs breaks
            intervention_text <- ""
            intervention_text_fr <- ""
            intervention_text_en <- ""
            para_text_id <- ""

            for (i_intervention_subnode in 1:length(names(intervention_content_node))) {

              if (i_intervention_subnode == 1 &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode]]) == "ProceduralText" &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode+1]]) == "ParaText") {

                sob_procedural_text <- XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]])
              }

              if (i_intervention_subnode > 1 && 
                  i_intervention_subnode < length(names(intervention_content_node)) &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode-1]]) == "ParaText" &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode]]) == "ProceduralText" &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode+1]]) == "ParaText") {

                if ( is.na(sob_procedural_text) || sob_procedural_text == "" )
                  sob_procedural_text <- XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]])
                else
                  sob_procedural_text <- paste(sob_procedural_text, "\nDuring the intervention: ", XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]]), sep='')
              }


              if (XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode]]) == "ParaText") {

                para_text_id <- XML::xmlGetAttr(intervention_node[["Content"]][[i_intervention_subnode]], "id")

                if (intervention_lang == "fr") {
                  intervention_text <- paste(intervention_text, XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]]), sep='\n\n')
                  intervention_text_fr <- paste(intervention_text, XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]]), sep='\n\n')
                  intervention_text_en <- paste(intervention_text_en, XML::xpathApply(hansard_body_xml_en,paste("//ParaText[@id='", para_text_id,"']",sep=''),XML::xmlValue), sep='\n\n')
                }

                if (intervention_lang == "en") {
                  intervention_text <- paste(intervention_text, XML::xpathApply(hansard_body_xml_en,paste("//ParaText[@id='", para_text_id,"']",sep=''),XML::xmlValue), sep='\n\n')
                  intervention_text_fr <- paste(intervention_text_fr, XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]]), sep='\n\n')
                  intervention_text_en <- paste(intervention_text_en, XML::xpathApply(hansard_body_xml_en,paste("//ParaText[@id='", para_text_id,"']",sep=''),XML::xmlValue), sep='\n\n')
                }

                para_node <- intervention_node[["Content"]][[i_intervention_subnode]]

                if ("Document" %in% names(para_node)) {
                  tmp_doc_id <- XML::xmlGetAttr(para_node[["Document"]], "DbId")

                  if (!is.na(sob_doc_id) && !stringr::str_detect(sob_doc_id, tmp_doc_id)) {
                    sob_doc_id <- paste(sob_doc_id, XML::xmlGetAttr(para_node[["Document"]], "DbId"), sep = ' | ')
                    sob_doc_title <- paste(sob_doc_title, trimws(XML::xmlValue(para_node[["Document"]])), sep = ' | ')
                  }
                  else {
                    sob_doc_id <- XML::xmlGetAttr(para_node[["Document"]], "DbId")
                    sob_doc_title <- trimws(XML::xmlValue(para_node[["Document"]]))
                  }


                  tmp_doc_id <- NA
                }

              }

              if (i_intervention_subnode > 1 &&
                  i_intervention_subnode == length(names(intervention_content_node)) &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode-1]]) == "ParaText" &&
                  XML::xmlName(intervention_node[["Content"]][[i_intervention_subnode]]) == "ProceduralText") {

                if ( is.na(sob_procedural_text) || sob_procedural_text == "" )
                  sob_procedural_text <- XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]])
                else
                  sob_procedural_text <- paste(sob_procedural_text,"\nAt the end of intervention: ", XML::xmlValue(intervention_node[["Content"]][[i_intervention_subnode]]), sep='')
              }
            }

            intervention_text <- trimws(intervention_text, "both")
            intervention_text_fr <- trimws(intervention_text_fr, "both")
            intervention_text_en <- trimws(intervention_text_en, "both")


            intervention_word_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), word, txt, token="words",format="text"))
            intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
            intervention_paragraph_count <- length(which(names(intervention_content_node) == "ParaText"))

            sob_procedural_text <- trimws(sob_procedural_text, "both")

            # commit to dfDeep and the Hub
            v2_row_to_commit <- data.frame(eventID = event_id,
                                          eventDate = event_date,
                                          eventStartTime = event_start_time,
                                          eventEndTime = event_end_time,
                                          eventTitle = event_title,
                                          eventSubTitle = event_subtitle,
                                          interventionSeqNum = intervention_seqnum,
                                          objectOfBusinessID = oob_id,
                                          objectOfBusinessRubric = oob_rubric,
                                          objectOfBusinessTitle = oob_title,
                                          objectOfBusinessSeqNum = oob_seqnum,
                                          subjectOfBusinessID = sob_id,
                                          subjectOfBusinessTitle = sob_id,
                                          subjectOfBusinessHeader = sob_header,
                                          subjectOfBusinessSeqNum = sob_seqnum,
                                          subjectOfBusinessProceduralText = sob_procedural_text,
                                          subjectOfBusinessTabledDocID = NA,
                                          subjectOfBusinessTabledDocTitle = NA,
                                          subjectOfBusinessAdoptedDocID = NA,
                                          subjectOfBusinessAdoptedDocTitle = NA,
                                          speakerID = speaker_id,
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
                                          interventionID = intervention_id,
                                          interventionDocID = intervention_doc_id,
                                          interventionDocTitle = intervention_doc_title,
                                          interventionType = intervention_type,
                                          interventionLang = intervention_lang,
                                          interventionWordCount = intervention_word_count,
                                          interventionSentenceCount = intervention_sentence_count,
                                          interventionParagraphCount = intervention_paragraph_count,
                                          interventionText = intervention_text,
                                          interventionTextFR = intervention_text_fr,
                                          interventionTextEN = intervention_text_en,
                                          stringsAsFactors = FALSE)
            
            v2_row_to_commit <- v2_row_to_commit %>% mutate(across(everything(), as.character))
            
            v2_metadata_to_commit <- list("url"=event_url, "format"="xml", "location"="CA", "institution"="House of Commons of Canada",
                                          "parliament_number"=event_parliam_number, "parliament_session"=event_session_number)
            
            dfInterventions <- clessnverse::commitAgoraplusInterventions(dfDestination = dfInterventions, 
                                                                        type = "parliament_debate", schema = "v2",
                                                                        metadata = v2_metadata_to_commit,
                                                                        data = v2_row_to_commit,
                                                                        opt$dataframe_mode, opt$hub_mode)

            current_speaker_full_name <- speaker_full_name

            intervention_id <- NA

          } #if (XML::xmlName(sob_content_node[[i_sob_content]]) == "Intervention")

        } #for (i_sob_content in 1::length(names(sob_content_node)))

      } #for (i_sob in which(oob_sob_list == "SubjectOfBusiness"))

    } #if ( "SubjectOfBusiness" %in% oob_sob_list )

  } #for (i_oob in names(hansard_body_xml_fr))

  clessnverse::logit(scriptname, 
                     paste("Commited", intervention_seqnum, "interventions to the hub for debate", event_id, "at URL", current_url_fr, sep = " "), 
                     logger)
  Sys.sleep(60)
} #for (i_url in 1:length(urls_list))


clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
