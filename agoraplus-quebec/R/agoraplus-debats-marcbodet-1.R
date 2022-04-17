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
  new_packages <- required_packages[!(required_packages %in% installed.packages()[, "Package"])]

  for (p in 1:length(new_packages)) {
    if (grepl("\\/", new_packages[p])) {
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
    if (!grepl("\\/", required_packages[p])) {
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
if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit("scraper", c("file", "console"), Sys.getenv("LOG_PATH"))

clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
# Script command line options:
# Possible values : update, refresh, rebuild or skip
# - update : updates the dataframe by adding only new observations to it
# - refresh : refreshes existing observations and adds new observations to the dataframe
# - rebuild : wipes out completely the dataframe and rebuilds it from scratch
# - skip : does not make any change to the dataframe
opt <- list(dataframe_mode = "rebuild", hub_mode = "skip", log_output = "hub,file,console", download_data = FALSE)

if (!exists("opt")) {
  opt <- clessnverse::processCommandLineOptions()
}


# Download HUB v2 data
if (opt$dataframe_mode %in% c("update", "refresh")) {
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


patterns_oob_rubric <- c("Présidence de")
patterns_oob_title <- c() # Ce sont les titres centrés
patterns_sob_title <- c() # Ce sont les titres centrés
patterns_sob_header <- c("En comité", "Messages du Conseil législatif:", "Résolutions à rapporter:", "Rapports de comités:",
                         "Demande de documents:", "Dépôt de documents:", "Présentation de pétitions:", "Lecture de pétitions:")

patterns_sob_proc_text <- c("adopt(é|ée|ées|és)\\.", "adopt(é|ée|ées|és) sur division\\.")

patterns_new_speakers <- c("Le Président\\s*:\\s*", "La Présidente\\s*:\\s*", "^M. l'Orateur:", "^M. l'Orateur\\s", "^M. l'Orateur,",
                           "^Des voix", "^Des Voix",
                           "^(Le Vice-Président|La Vice-Présidente)?\\s(.*?)?\\((.*?)\\)\\s+?:",
                           "^(Le Vice-Président|La Vice-Présidente)?\\s(.*?)?\\((.*?)\\)\\s+?",
                           "Le Vice-Président", "La Vice-Présidente",
                           "^(M\\.|Mme)?\\s(.*?)?\\((.*?)\\)",
                           "^(M\\.|Mme)?\\s(.*?)?\\((.*?)\\):",
                           "^(M\\.|Mme)?\\s(.*?)?:",
                           "^(M\\.|Mme)?\\s(.*?)?",
                           "^(M\\»|Mme)?\\s(.*?)?:",
                           "^(M\\»|Mme)?\\s(.*?)?",
                           "^L'honorable\\s+M\\.?(\\s+)?(.*?)\\s+(\\(.*?\\))",
                           "^L'honorable\\s+M\\.?(\\s+)?(.*?)\\s+(\\(.*?\\))",
                           "^L'honorable\\s+M\\.?(\\s+)?(.*?)\\s+(\\(.*?\\:)",
                           "^L'honorable\\s+Mme(\\s+)?(.*?)\\s+(\\(.*?\\))",
                           "^M\\.\\s+(\\w+)\\s+\\(((?:\\w+-?)+\\w+)\\)",
                           "^L'honorable", "^Le gentilhomme huissier à la verge noire",
                           "^Son Honneur le lieutenant-gouverneur(.*)\\:$", "^M. l'Orateur du Conseil législatif",
                           "^Messages du lieutenant-gouverneur:", "^Son Honneur le lieutenant-gouverneur")

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
file_list <- clessnverse::dbxListDir('/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-vintage', Sys.getenv("DROPBOX_TOKEN"))
file_list <- file_list %>% filter(objectType == "file")

list_urls <- NULL

for (i in 1:nrow(file_list)) {
  doc_html_name <- paste(file_list$objectPath[i], "/", file_list$objectName[i], sep = '')
  doc_html <- clessnverse::dbxDownloadFile(doc_html_name, "./", Sys.getenv("DROPBOX_TOKEN"))

  if (doc_html) {
    doc_html <- readLines(file_list$objectName[i])
    file.remove(paste("./", file_list$objectName[i], sep = ''))
  }

  parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
  doc_urls <- XML::xpathSApply(parsed_html, "//a/@href")
  list_urls <- c(list_urls, doc_urls[grep("assemblee-nationale/\\d\\d-\\d/journal-debats", doc_urls)])
}

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
#for (i in 1:length(list_urls)) {
for (i in 601:length(list_urls)) {
#for (i in 4768:4768) {

  if (i %% 100 == 0) {
    write.csv2(dfInterventions, paste("_SharedFolder_clessn-blend/data/agoraplus-vintage/data/agoraplus-vintage-", i, ".csv", sep = ""))
    dfInterventions <- dfInterventions[-c(1:nrow(dfInterventions)),]
  }

  #if (opt$hub_mode != "skip") clessnhub::refresh_token(configuration$token, configuration$url)
  if (opt$hub_mode != "skip") clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

  event_url <- paste(base_url, list_urls[i], sep = "")
  event_id <- paste("dp",
                    gsub("[[:punct:]]", "",
                         paste(stringr::str_sub(event_url, 71, 74),
                               stringr::str_sub(event_url, 90, 105), sep = "")
                         ),
                    sep = "")

  clessnverse::logit(scriptname, "", logger)
  clessnverse::logit(scriptname, paste("Debate", i, "of", length(list_urls),sep = " "), logger)

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

    doc_html <- stringr::str_replace_all(doc_html, '<P ALIGN="CENTER"><B>', '<P ALIGN="CENTER"><B>Titre:=')
    doc_html <- stringr::str_replace_all(doc_html, '<p align="center">\n  <b>', '<p align="center"><b>Titre:=')
    doc_html <- stringr::str_replace_all(doc_html, '</p>\n<p align=center><b>', '<p align="center"><b>Titre:=')

    doc_html <- stringr::str_replace_all(doc_html, '<P ALIGN="JUSTIFY"><B>', '<P ALIGN="JUSTIFY"><B>NewSpeaker:=')
    doc_html <- stringr::str_replace_all(doc_html, '<p align="JUSTIFY"><b>', '<p align="JUSTIFY"><b>NewSpeaker:=')
    doc_html <- stringr::str_replace_all(doc_html, '<p>M(.*?):', paste('<p>', 'NewSpeaker:=', 'M', '\\1', ':', sep='')) 
    doc_html <- stringr::str_replace_all(doc_html, '<p>\n  <b>M\\.', '<p><b>NewSpeaker:=M\\.')
    doc_html <- stringr::str_replace_all(doc_html, '<p>\n  <b>Mme', '<p><b>NewSpeaker:=Mme')
    doc_html <- stringr::str_replace_all(doc_html, '<p>\n?<b>M\\.', '<p><b>NewSpeaker:=M\\.')
    doc_html <- stringr::str_replace_all(doc_html, '<p>\n?<b>Mme', '<p><b>NewSpeaker:=Mme')
    doc_html <- stringr::str_replace_all(doc_html, '<p>\n\\s+<b>(.*?):?\\s?</b>', paste('</p>\n<p><b>NewSpeaker:=','\\1: ','</b>',sep=''))

    doc_html <- stringr::str_replace_all(doc_html, '<p style="text-align: justify">\n\\s+<a name="Toc(.*)>\n\\s+<b>', '<p style="text-align: justify"><a name="\\1>\n<b>Titre:=')
    doc_html <- stringr::str_replace_all(doc_html, '<a name="(.*)>\n\\s+<b>', '<a name="\\1>\n<b>Titre:=')

    doc_html <- stringr::str_replace_all(doc_html, '</p>\n?<p><b>L’honorable M.(.*?)', paste('</p><p><b>NewSpeaker:=L’honorable M.','\\1',sep=''))

    #doc_html <- stringr::str_replace_all(doc_html, '<a name="_Toc([0-9]+)">(\\b[\\w]+[\\b[\\s+]?[\\w]+?]+)</a>', paste('<a name="_Toc\\1">Titre:=\\2</a>', sep=''))
    doc_html <- stringr::str_replace_all(doc_html, '<a name="_Toc([0-9]+)">(.*?(\n?.*?)*)?</a>', '<a name="_Toc\\1">Titre:=\\2</a>')
    doc_html <- stringr::str_replace_all(doc_html, 'Titre:=</a>', '</a>')


    doc_html <- stringr::str_replace_all(doc_html, '<p style="mso-style-link: &quot;text-align: justify">\n?\\s+?<b>(.*\n?.*)</b>', paste('<p style="mso-style-link: &quot;text-align: justify"><b>NewSpeaker:=',  gsub('\n','','\\1'), '</b>',sep=''))
    doc_html <- stringr::str_replace_all(doc_html, '<p style="text-align: justify">\n?\\s+?<b>(.*\n?.*)</b>', paste('<p style="text-align: justify"><b>NewSpeaker:=', gsub('\n','','\\1'), '</b>',sep=''))




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
    } else {
      version_finale <- FALSE
      clessnverse::logit(scriptname, "version préliminaire", logger)
    }

    clessnverse::logit(scriptname, event_url, logger)

    if ( version_finale && 
         ( (opt$dataframe_mode == "update" && !(event_id %in% dfInterventions$data.eventID) ||
            opt$dataframe_mode == "refresh" ||
            opt$dataframe_mode == "rebuild") ||
          (opt$hub_mode == "refresh" ||
             opt$hub_mode == "update") && event_id %in% dfSimple$eventID)
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
      datestr <- paste(event_date[2],months_fr[match(tolower(event_date[3]),months_fr)],event_date[4])
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
      doc_text <- trimws(doc_text)

      doc_text <- gsub('NewSpeaker:=NewSpeaker:=','NewSpeaker:=', doc_text)
      doc_text <- gsub('Titre:=Titre:=','Titre:=', doc_text)
      doc_text <- gsub('NewSpeaker:=\\s+NewSpeaker:=','NewSpeaker:=', doc_text)
      doc_text <- gsub('Titre:=\\s+Titre:=','Titre:=', doc_text)
      doc_text <- gsub('NewSpeaker:=M. le Président, ', 'M. le Président, ', doc_text) 
      doc_text <- gsub('NewSpeaker:=Mme la Présidente, ', 'Mme la Présidente, ', doc_text) 

      patterns_time_digits_or_text_fr <- "((une|1)\\s+(heure|h\\s?)(\\d+)?(.*)?\\.?)|(((deux|2)|(trois|3)|(quatre|4)|(cinq|5)|(six|6)|(sept|7)|(huit|8)|(neuf|9)|(dix|10)|(onze|11)|(douze|midi|12|minuit)|(treize|13)|(quatorze|14)|(quinze|15)|(seize|16)|(dix-sept|17)|(dix-huit|18)|(dix-neuf|19)|(vingt|20)|(vingt-et-une|21)|(vingt-deux|22)|(vingt-trois|23))(\\s+)?(heures|h)?(\\s+\\d+)?(.*)?\\.?)"

      patterns_time_text_fr <- "(une|zéro\\s+(heure|h)(\\s+\\d+)?\\.?)|((deux|trois|quatre|cinq|six|sept|huit|neuf|dix|onze|douze|midi|treize|quatorze|quinze|seize|dix-sept|dix-huit|dix-neuf|vingt|vint-et-une|vingt-deux|vingt-trois|minuit)(\\s+heures)?(.*)?\\.?)"

      patterns_time_digits <- "(1|0\\s+(heure|h)(\\s+\\d+)?(.*)?\\.?)|((2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23)\\s+(heures|h)(\\s+\\d+)?(.*)?\\.?)"

      time_anomaly_index <- grep("heures(\\d+)", doc_text)
      if ( length(time_anomaly_index) > 0 ) {
        doc_text[time_anomaly_index] <- stringr::str_replace(doc_text[time_anomaly_index], "heures(\\d+)", "heures \\1")
      }

      time_anomaly_index <- grep("minuit(\\d+)", doc_text)
      if ( length(time_anomaly_index) > 0 ) {
        doc_text[time_anomaly_index] <- stringr::str_replace(doc_text[time_anomaly_index], "minuit(\\d+)", "minuit \\1")
      }

      time_anomaly_index <- grep("midi(\\d+)", doc_text)
      if ( length(time_anomaly_index) > 0 ) {
        doc_text[time_anomaly_index] <- stringr::str_replace(doc_text[time_anomaly_index], "midi(\\d+)", "midi \\1")
      }

      start.index <- grep(patterns_time_digits_or_text_fr, tolower(doc_text))

      #if ( length(start.index) > 0 ) {

        for (x in 0:(start.index[1]-2)) doc_text[x] <- NA

        doc_text <- na.omit(doc_text)  

        # Extract start time of the conference
        dt_index <- 0

        if (TRUE %in% stringr::str_detect(doc_text, "^La séance s'ouvre à |^La séance est ouverte à |^Le\\s[\\p{Letter}]+\\s+[0-9]*\\s+[\\p{Letter}]+\\s+[0-9]+\\s+|^\\(.*?heure.*?\\)$")) {
          dt_index <- which(stringr::str_detect(doc_text, "^La séance s'ouvre à |^La séance est ouverte à |^Le\\s[\\p{Letter}]+\\s+[0-9]*\\s+[\\p{Letter}]+\\s+[0-9]+\\s+|^\\(.*?heure.*?\\)$"))[[1]]
        }

        if (dt_index != 0) {
          if ( stringr::str_detect(tolower(doc_text[dt_index]), patterns_time_text_fr) ) {
            if ( stringr::str_detect(doc_text[dt_index], "^Le\\s[\\p{Letter}]+\\s+[0-9]*\\s+[\\p{Letter}]+\\s+[0-9]+\\s+") ) {
              doc_text[dt_index] <- stringr::str_replace(doc_text[dt_index], "^Le\\s[\\p{Letter}]+\\s+[0-9]*\\s+[\\p{Letter}]+\\s+[0-9]+\\s+", "")
            } else {
              doc_text[dt_index] <- gsub("^\\(", "", doc_text[dt_index])
              doc_text[dt_index] <- gsub("\\)$", "", doc_text[dt_index])
              doc_text[dt_index] <- gsub("^'", "", doc_text[dt_index])
              doc_text[dt_index] <- gsub("'$", "", doc_text[dt_index])
              if (stringr::str_detect(doc_text[dt_index], "^La séance s'ouvre à |^La séance est ouverte à |^Reprise de la séance à ")) {
                doc_text[dt_index] <- gsub("^La séance s'ouvre à |^La séance est ouverte à |^Reprise de la séance à ", "", doc_text[dt_index])
              } else {
                if (stringr::str_detect(doc_text[dt_index], "^\\(.*?heure.*?\\)$")) {
                  doc_text[dt_index] <- gsub("^\\(", "", doc_text[dt_index])
                  doc_text[dt_index] <- gsub("\\)$", "", doc_text[dt_index])
                  doc_text[dt_index] <- gsub("du matin", "", doc_text[dt_index])
                  doc_text[dt_index] <- gsub("de l'après-midi", "", doc_text[dt_index])
                  doc_text[dt_index] <- gsub("de l'avant-midi", "", doc_text[dt_index])
                  doc_text[dt_index] <- trimws(doc_text[dt_index])
                }
              }
            }
            doc_text[dt_index] <- gsub("\\(|\\)", "", doc_text[dt_index])
            doc_text[dt_index] <- gsub("\\s\\s+", " ", doc_text[dt_index])
            doc_text[dt_index] <- gsub("\\.$", "", doc_text[dt_index])

            hour <- strsplit(doc_text[dt_index], " ")[[1]][1]
            hour <- clessnverse::convertTextToNumberFR(hour)[[2]][1]

            if (!is.na(hour) && nchar(hour) == 1) hour <- paste("0", hour, sep = "")
            doc_text[dt_index] <- gsub("-minutes", "", doc_text[dt_index])

            if (clessnverse::countWords(doc_text[dt_index]) > 2) {
              minute <- strsplit(doc_text[dt_index], " ")[[1]][3]
              if (lengths(regmatches(doc_text[dt_index], gregexpr(" ", doc_text[dt_index]))) + 1 > 3 &&
                  strsplit(doc_text[dt_index], " ")[[1]][4] == "et") {
                minute <- paste(minute, "et", strsplit(doc_text[dt_index], " ")[[1]][5], sep = " ")
              }
              if (!is.na(minute)) minute <- clessnverse::convertTextToNumberFR(minute)[[2]][1]
              if (!is.na(minute) && nchar(minute) == 1) minute <- paste("0", minute, sep = "")
            } else {
              minute <- "00"
            }
            event_start_time <- paste(event_date, " ", hour, ":", minute, ":00", sep = "")
          }


          if (stringr::str_detect(tolower(doc_text[dt_index]), patterns_time_digits)) {
            doc_text[dt_index] <- gsub("^La séance s'ouvre à |^La séance est ouverte à |^Reprise de la séance à ", "", doc_text[dt_index])
            doc_text[dt_index] <- gsub("\\(|\\)", "", doc_text[dt_index])
            doc_text[dt_index] <- gsub("\\s\\s+", " ", doc_text[dt_index])
            doc_text[dt_index] <- gsub("\\.$", "", doc_text[dt_index])

            hour <- strsplit(doc_text[dt_index], " ")[[1]][1]

            if (nchar(hour) == 1) hour <- paste("0", hour, sep="")

            if (clessnverse::countWords(doc_text[dt_index]) > 2) {
              minute <- strsplit(doc_text[dt_index], " ")[[1]][3]
              if (!is.na(strsplit(doc_text[dt_index], " ")[[1]][4]) && strsplit(doc_text[dt_index], " ")[[1]][4] == "et") {
                minute <- paste(minute, "et", strsplit(doc_text[dt_index], " ")[[1]][5], sep = " ")
              }
              #minute <- clessnverse::convertTextToNumberFR(minute)[[2]][1]
              if (nchar(minute) == 1) minute <- paste("0", minute, sep = "")
            } else {
              minute <- "00"
            }
            event_start_time <- paste(event_date, " ", hour, ":", minute, ":00", sep = "")
          }
        } #if (df_index != 0) {

        # Get rid of the line containing the start time
        doc_text[dt_index] <- NA
        doc_text <- na.omit(doc_text)
      #}

      # Figure out the end time of the conference
      end_time_index <- which(stringr::str_detect(doc_text, "^\\La séance est levée (à|vers)|Fin de la séance à"))
      end_time_index <- end_time_index[length(end_time_index)]
      event_end_time <- doc_text[end_time_index]
      event_end_time <- gsub("du|matin|soir|de|l|après-midi|\\.|\\'", "", event_end_time)
      event_end_time <- trimws(event_end_time)
      if (length(event_end_time) > 0) {
        event_end_time <- event_end_time[length(event_end_time)]

        if (stringr::str_detect(event_end_time, "\\((.*)\\)")) event_end_time <- stringr::str_match(event_end_time, "\\((.*)\\)")[2]

        if (stringr::str_detect(event_end_time, patterns_time_digits) && !stringr::str_detect(event_end_time, "midi|minuit")) {

          event_end_time <- gsub("\\(", "", event_end_time)
          event_end_time <- gsub("\\)", "", event_end_time)
          event_end_time <- gsub("\\.", "", event_end_time)
          event_end_time <- gsub("et quart", "15", event_end_time)
          event_end_time <- gsub("environ", "", event_end_time)
          event_end_time <- gsub("heure", " heure ", event_end_time)
          event_end_time <- gsub("heures", " heures ", event_end_time)
          event_end_time <- gsub("h", " h ", event_end_time)
          event_end_time <- clessnverse::splitWords(event_end_time)
          event_end_time <- gsub("^de$", "", event_end_time)

          # if ( event_end_time[length(event_end_time)] == "heures" || event_end_time[length(event_end_time)] == "h" ) {
          #   event_end_time[length(event_end_time)] <- ":"
          #   event_end_time[length(event_end_time)+1] <- "00"
          # }

          

          hour_separator_index <- which(stringr::str_detect(event_end_time, "heure(s?)|h"))
          hour_separator_index <- hour_separator_index[length(hour_separator_index)]

          if (length(hour_separator_index) == 0) hour_separator_index <- 0

          if (length(event_end_time[hour_separator_index+1]) == 1) event_end_time[hour_separator_index+1] <- paste("0", event_end_time[hour_separator_index+1], sep = "")
          if (event_end_time[hour_separator_index-1] == "24") {
            event_end_time[hour_separator_index - 1] <- "00"
            event_end_date <- event_date + 1
          } else {
            event_end_date <- event_date
          }

          if (hour_separator_index > 0) {
            event_end_time <- paste(event_end_time[hour_separator_index - 1], ":", event_end_time[hour_separator_index + 1])
          }  else {
            event_end_time <- paste(event_end_time[length(event_end_time)-1], ":", event_end_time[length(event_end_time)])
          }
          event_end_time <- gsub(" ", "", event_end_time)
          event_end_time <- gsub("NA", "", event_end_time)

          event_end_time <- gsub("\\:$", "\\:00", event_end_time)

          event_end_time <- strptime(paste(event_end_date, event_end_time), "%Y-%m-%d %H:%M")
        }

        if (stringr::str_detect(tolower(event_end_time), patterns_time_text_fr)) {
          event_end_time <- gsub("\\(", '', event_end_time)
          event_end_time <- gsub("\\)", '', event_end_time)
          event_end_time <- gsub("\\.", '', event_end_time)
          event_end_time <- gsub("et quart", '15', event_end_time)
          event_end_time <- gsub("environ", '', event_end_time)
          event_end_time <- clessnverse::splitWords(event_end_time)
          event_end_time <- gsub("^de$", "", event_end_time)

          end_time_index_pos <- which(stringr::str_detect(patterns_time_text_fr, event_end_time))[1]

          hour <- clessnverse::convertTextToNumberFR(event_end_time[end_time_index_pos])[[2]][1]
          if (nchar(hour) == 1) hour <- paste("0",hour,sep="")

          if (grepl("heure(s?)|h", event_end_time[end_time_index_pos+1])) {
            end_time_minute_offset <- 2
          } else {
            end_time_minute_offset <- 1
          }

          if (!is.na(event_end_time[end_time_index_pos + end_time_minute_offset])) {
            if (grepl("\\d+", event_end_time[end_time_index_pos + end_time_minute_offset])) {
              minute <- event_end_time[end_time_index_pos + end_time_minute_offset]
            } else {
              if (!grepl("moins", tolower(event_end_time[end_time_index_pos + end_time_minute_offset]))) {
                minute <- clessnverse::convertTextToNumberFR(event_end_time[end_time_index_pos + end_time_minute_offset])[[2]][1]
              } else {
                minute <- "00"
              }
            }
            if (nchar(minute) == 1) minute <- paste("0", minute, sep="")
          } else {
            minute <- "00"
          }

          event_end_time <- paste(event_date, " ", hour, ":", minute, ":00", sep = "")
        }

      } else {
        event_end_time <- NA
      }

      # Remove consecutive spaces (cleaning)
      doc_text <- gsub("(?<=[\\s])\\s*|^\\s+|\\s+$", "", doc_text, perl = TRUE)
      doc_text[which(doc_text == "")] <- NA
      doc_text <- na.omit(doc_text)

      clessnverse::logit(scriptname, paste("event_start_time", event_start_time, "event_end_time", event_end_time), logger)

      # Find come patterns in doc_text that are impossible to find and replace in html
      for (j in 1:length(doc_text)) {
        if (stringr::str_detect(doc_text[j], "^Son Honneur le lieutenant-gouverneur(.*):$")) {
          doc_text[j] <- paste("NewSpeaker:=", doc_text[j])
        }
      }


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
      persistent_president_name <- NA

      parliament_number <- stringr::str_sub(event_url, 71, 72)
      parliament_session <- stringr::str_sub(event_url, 74, 74)

      gender_femme <- 0
      speaker <- data.frame()
      periode_de_questions <- FALSE
      title_in_progress <- FALSE

      ########################################################
      # Go through the vector of paragraphs of the event
      # and strip out any relevant info
      intervention_seqnum <- 1

      event_paragraph_count <- length(doc_text) - 1
      event_sentence_count <- clessnverse::countVecSentences(doc_text) - 1
      speech_paragraph_count <- 0

      party_started <- FALSE

      for (j in 1:length(doc_text)) {
        president_name <- NA

        if (j > 1) {
          previous_paragraph <- current_paragraph
        } else {
          previous_paragraph <- NA
        }

        paragraph_start <- gsub("’", "'", substr(doc_text[j], 1, 75))
        next_paragraph_start <- gsub("’", "'", substr(doc_text[j + 1], 1, 75))
        next2_paragraph_start <- gsub("’", "'", substr(doc_text[j + 2], 1, 75))
        current_paragraph <-  gsub("’", "'", doc_text[j])
        next_paragraph <-  gsub("’", "'", doc_text[j + 1])
        next2_paragraph <- gsub("’", "'", doc_text[j + 2])




        if (TRUE %in% stringr::str_detect(tolower(paragraph_start), tolower(patterns_oob_rubric))) {
          current_paragraph <- gsub("’", "'", current_paragraph)
          oob_rubric <- gsub(":$", "", current_paragraph)
          current_paragraph <- gsub("NewSpeaker:=", "", current_paragraph)
          oob_rubric <- gsub("NewSpeaker:=", "", current_paragraph)
          if (stringr::str_detect(tolower(oob_rubric), "présidence de l'honorable ")) {
            if (stringr::str_detect("sous la présidence de", tolower(oob_rubric))) {
              president_name <- stringr::str_replace(tolower(oob_rubric), "sous la présidence de l'honorable ", "")
            } else {
              president_name <- stringr::str_replace(tolower(oob_rubric), "présidence de l'honorable ", "")
            }


            president_name <- stringr::str_to_title(president_name)
            president_name <- trimws(president_name) 
            president_first_name <- strsplit(president_name, " ")[[1]][1]
            president_last_name <- strsplit(president_name, " ")[[1]][2]
            president_full_name <- president_name
            sob_procedural_text <- current_paragraph
            sob_title <- current_paragraph
            oob_title <- current_paragraph
          }
          next
        }
 
        if ( grepl("^Titre:=", paragraph_start) ) {
          # it's a agenda item
          party_started <- TRUE
 
          # # speaker_first_name <- NA
          # # speaker_last_name <- NA
          # # speaker_full_name <- NA
          # # speaker_gender <- NA
          # # speaker_type <- NA
          # # speaker_party <- NA
          # # speaker_district <- NA
          # # speaker_is_minister <- NA
          # # 
          # # intervention_type <- NA
          # # intervention_text <- NA
          # # language <- NA
          # # sob_procedural_text <- NA
          # # speaker <- data.frame()
          #
          # speech_paragraph_count <- 1
          # speech_sentence_count <- 0
          # speech_word_count <- 0

          if (grepl("^Titre:=", previous_paragraph)) {
            oob_title <- gsub("^Titre:=", "", previous_paragraph)
            sob_title <- gsub("^Titre:=", "", previous_paragraph)
          } else {
            oob_title <- gsub("^Titre:=", "", current_paragraph)
            sob_title <- gsub("^Titre:=", "", current_paragraph)
          }

          if (!grepl("^NewSpeaker:=", next_paragraph_start)) {
            sob_procedural_text <- ""
          }

          title_in_progress <- TRUE

          next
        }


 
        if (grepl("^NewSpeaker:=", paragraph_start)) {
          party_started <- TRUE
          title_in_progress <- FALSE
 
          paragraph_start <- trimws(gsub("^NewSpeaker:=", "", paragraph_start), "both")

          current_paragraph <- trimws(gsub("^NewSpeaker:=", "", current_paragraph), "both")


          if (TRUE %in% stringr::str_detect(paragraph_start, patterns_sob_header)) {
            # It's a sob header ("En comité", "Messages du Conseil législatif:", "Résolutions à rapporter:") etc... 
            sob_header <- gsub(":$", "", current_paragraph)
            next
          } else {
            intervention_text <- NA
          }

          if (j < length(doc_text) && TRUE %in% stringr::str_detect(next_paragraph_start, patterns_sob_proc_text)) {
            # It's a procedural text ("adopté", "La résolution est adoptée.", "adoptées.") etc...
            sob_procedural_text <- next_paragraph
          } else {
            intervention_text <- NA
          }

          if (TRUE %in% stringr::str_detect(paragraph_start, patterns_new_speakers)) {
            # It's a new person speaking
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
            speaker <- data.frame()

            speech_paragraph_count <- 1
            speech_sentence_count <- 0
            speech_word_count <- 0


            # Skip if this intervention already is in the dataset and if we're not refreshing it
            matching_row <- which(dfInterventions$key == paste(event_id, intervention_seqnum, sep = "-"))
            if (length(matching_row) > 0 && opt$dataframe_mode != "refresh") {
              intervention_seqnum <- intervention_seqnum + 1
              matching_row <- NULL
              next
            }

            # Otherwise parse this paragraph
            intervention_text <- gsub(":$", "", current_paragraph)
            pattern_found <- patterns_new_speakers[which(stringr::str_detect(paragraph_start, patterns_new_speakers) == TRUE)[1]][1]
            intervention_text <- trimws(stringr::str_split(intervention_text, pattern_found)[[1]][2])
            intervention_text <- gsub("^:", "", intervention_text)
            intervention_text <- gsub("^,", "", intervention_text)
            intervention_text <- trimws(intervention_text)

            if (pattern_found == "^(M\\.|Mme)?\\s(.*)?:" || pattern_found == "^(M\\»|Mme)?\\s(.*?)?:" || pattern_found == "^(M\\.|Mme)?\\s(.*?)?:") {
              if (pattern_found == "^(M\\.|Mme)?\\s(.*)?:") {
                speaker_full_name <- trimws(stringr::str_match_all(current_paragraph, pattern_found)[[1]][3])
              } else {
                speaker_full_name <- trimws(stringr::str_match_all(current_paragraph, pattern_found)[[1]][1])
              }
            } else {
              if (pattern_found == "^(Le Vice-Président|La Vice-Présidente)?\\s(.*?)?\\((.*?)\\)\\s+?:" || pattern_found == "^(Le Vice-Président|La Vice-Présidente)?\\s(.*?)?\\((.*?)\\)\\s+?") {
                speaker_full_name <- trimws(stringr::str_match_all(paragraph_start, pattern_found)[[1]][4])
                if (!is.na(speaker_full_name)) {
                  speaker_full_name <- gsub("\\(", "", speaker_full_name)
                  speaker_full_name <- gsub("\\)", "", speaker_full_name)
                  speaker_full_name <- stringr::str_to_title(speaker_full_name)
                  president_name <- stringr::str_to_title(speaker_full_name)
                  president_name <- trimws(president_name) 
                  president_first_name <- strsplit(president_name, " ")[[1]][1]
                  president_last_name <- strsplit(president_name, " ")[[1]][2]
                  president_full_name <- president_name
                  speaker_type <- "modérateur"
                } else {
                  speaker_full_name <- stringr::str_split(current_paragraph, ":")[[1]][1]
                }
              } else {
                if (pattern_found == "Le Vice-Président" ||  pattern_found == "La Vice-Présidente") {
                  speaker_full_name <- trimws(stringr::str_match_all(paragraph_start, "\\((.*)\\)?(\\s+)?:")[[1]][2])
                  if (!is.na(speaker_full_name)) {
                    speaker_full_name <- gsub("\\(", "", speaker_full_name)
                    speaker_full_name <- gsub("\\)", "", speaker_full_name)
                    speaker_full_name <- stringr::str_to_title(speaker_full_name)
                    president_name <- stringr::str_to_title(speaker_full_name)
                    president_name <- trimws(president_name)
                    president_first_name <- strsplit(president_name, " ")[[1]][1]
                    president_last_name <- strsplit(president_name, " ")[[1]][2]
                    president_full_name <- president_name
                    speaker_type <- "modérateur"
                  } else {
                    speaker_full_name <- stringr::str_split(current_paragraph, ":")[[1]][1]
                  }
                } else {
                  if (pattern_found == "Le Président\\s*:\\s*" ||  pattern_found == "La Présidente\\s*:\\s*") {
                    speaker_full_name <- trimws(stringr::str_match_all(paragraph_start, "\\((.*)\\)?(\\s+)?:")[[1]][2])
                    if ( !is.na(speaker_full_name)  ) {
                      speaker_full_name <- gsub("\\(", "", speaker_full_name)
                      speaker_full_name <- gsub("\\)", "", speaker_full_name)
                      speaker_full_name <- stringr::str_to_title(speaker_full_name)
                      president_name <- stringr::str_to_title(speaker_full_name)
                      president_name <- trimws(president_name) 
                      president_first_name <- strsplit(president_name, " ")[[1]][1]
                      president_last_name <- strsplit(president_name, " ")[[1]][2]
                      president_full_name <- president_name
                      speaker_type <- "modérateur"
                    } else {
                      speaker_full_name <- stringr::str_split(current_paragraph, ":")[[1]][1]
                    }
                  } else {
                    speaker_full_name <- trimws(stringr::str_match_all(current_paragraph, pattern_found)[[1]][1])
                  }
                }
              }
            }

            if (stringr::str_detect(speaker_full_name, "^Le\\s") || stringr::str_detect(speaker_full_name, "^M\\.") || 
                stringr::str_detect(speaker_full_name, "^M\\»")  || stringr::str_detect(speaker_full_name, "^L'honorable\\s+M\\.")) {
              speaker_gender <- "M"
            } else {
              if (stringr::str_detect(speaker_full_name, "^La\\s")  || stringr::str_detect(speaker_full_name, "^Mme") ||
                  stringr::str_detect(speaker_full_name, "^Mme\\»") || stringr::str_detect(speaker_full_name, "^L'honorable\\s+Mme")) {
                speaker_gender <- "F"
              } else {
                speaker_gender <- NA
              }
            }

            speaker_full_name <- gsub("\\(\\(", "\\(", speaker_full_name)
            speaker_full_name <- gsub("\\)\\)", "\\)", speaker_full_name)
            speaker_full_name <- trimws(speaker_full_name)

            if (stringr::str_detect(speaker_full_name, "\\((.*)\\)?:?$")) {
              # speaker identification : Title Name (district) 
              if (stringr::str_detect(speaker_full_name, "\\((.*)\\):?$")) {
                speaker_district <- stringr::str_match(speaker_full_name, "\\((.*?)\\)")[2]
                if (is.na(speaker_district)) speaker_district <- stringr::str_match(speaker_full_name, "\\((.*?)\\(")[2]
                if (stringr::str_detect(speaker_district, "^\\w+\\s\\((\\w*)?")) speaker_district <- stringr::str_match(speaker_district, "^\\w+\\s\\((\\w*)?")[2]
                speaker_full_name <- trimws(gsub(speaker_district, "", speaker_full_name))
                speaker_full_name <- trimws(gsub(" \\(\\)", "", speaker_full_name))
                speaker_full_name <- trimws(gsub("^L'honorable\\s+", "", speaker_full_name))
                speaker_full_name <- trimws(gsub("^M\\.|^Mme\\s+", "", speaker_full_name))
                speaker_full_name <- trimws(gsub(":", "", speaker_full_name))
                if (stringr::str_detect(tolower(speaker_district), "président")) {
                  president_name <- stringr::str_to_title(speaker_full_name)
                  president_name <- trimws(president_name) 
                  president_first_name <- strsplit(president_name, " ")[[1]][1]
                  president_last_name <- strsplit(president_name, " ")[[1]][2]
                  president_full_name <- president_name
                }
              } else {
                if (stringr::str_detect(speaker_full_name, "\\((.*)\\:$")) {
                  speaker_district <- stringr::str_match(speaker_full_name, "\\((.*?)\\:")[2]
                  if (is.na(speaker_district)) speaker_district <- stringr::str_match(speaker_full_name, "\\((.*?)\\(")[2]
                  if (stringr::str_detect(speaker_district, "\\($")) speaker_district <- gsub("\\($", "", speaker_district)
                  speaker_full_name <- trimws(gsub(speaker_district, "", speaker_full_name))
                  speaker_full_name <- trimws(gsub(" \\(\\:", "", speaker_full_name))
                  speaker_full_name <- trimws(gsub("^L'honorable\\s+", "", speaker_full_name))
                  speaker_full_name <- trimws(gsub("^M\\.|^Mme\\s+", "", speaker_full_name))
                  speaker_full_name <- trimws(gsub(":", "", speaker_full_name))
                  if (stringr::str_detect(tolower(speaker_district), "président")) {
                    president_name <- stringr::str_to_title(speaker_full_name)
                    president_name <- trimws(president_name)
                    president_first_name <- strsplit(president_name, " ")[[1]][1]
                    president_last_name <- strsplit(president_name, " ")[[1]][2]
                    president_full_name <- president_name
                  }
                } else {
                  # nothing
                }
              }


              if (stringr::str_detect(tolower(speaker_district),"président") || stringr::str_detect(tolower(speaker_district), "president")) {
                speaker_full_name <- trimws(gsub(":", "", speaker_full_name))
                speaker_full_name <- stringr::str_to_title(speaker_full_name)
                persistent_president_name <- speaker_full_name
                speaker_type <- "modérateur"
                speaker_district <- NA
              }



            } else {
              # speaker identification : Title Name
              if (stringr::str_detect(speaker_full_name, "^M\\.|^Mme\\s+(.*)$")) {
                speaker_full_name <- trimws(gsub("^M\\.|^Mme\\s+", "", speaker_full_name))

                if (stringr::str_detect(tolower(speaker_full_name), "orateur")) {
                  speaker_full_name <- president_full_name
                  speaker_first_name <- president_first_name
                  speaker_last_name <- president_last_name
                  speaker_type <- "modérateur"
                }

                if (stringr::str_detect(tolower(speaker_full_name), "président") || stringr::str_detect(tolower(speaker_full_name), "president")) {
                  if (!is.na(persistent_president_name)) {
                    speaker_full_name <- persistent_president_name
                    speaker_type <- "modérateur"
                  } else {
                    speaker_full_name <- president_full_name
                    speaker_first_name <- president_first_name
                    speaker_last_name <- president_last_name
                    speaker_type <- "modérateur"
                  }
                }

              } else {
                # speaker identification not determined...

              }

              speaker_full_name <- trimws(gsub(":", "", speaker_full_name))
              speaker_full_name <- stringr::str_to_title(speaker_full_name)

              if (speaker_full_name == "Messages du lieutenant-gouverneur") speaker_full_name <- "Son Honneur le lieutenant-gouverneur"

            }


            if (stringr::str_detect(speaker_full_name, " ")) {
              speaker_first_name <- strsplit(speaker_full_name, " ")[[1]][1]
              speaker_last_name <- strsplit(speaker_full_name, " ")[[1]][2]
            } else {
              speaker_last_name <- speaker_full_name
            }

          }
        } else {
          # It's the same person as in the previous paragraph speaking
          # We will append it to the same row instead of creating an extra row for a new paragraph
          if (party_started) {
            if (!title_in_progress) {
              intervention_text <- paste(intervention_text, "\n\n", doc_text[j], sep = "")
              speech_paragraph_count <- speech_paragraph_count + 1
              intervention_text <- gsub("^:", "", intervention_text)
              intervention_text <- gsub("^NA\n\n", "", intervention_text)
            } else {
              sob_procedural_text <- paste(sob_procedural_text, current_paragraph)
            }
          }
        } #if ( grepl("^NewSpeaker:=", paragraph_start) )


        # If the next speaker is different or if it's the last record, then let's commit this observation into the dataset
        if ((j < length(doc_text) &&
             (grepl("^NewSpeaker:=", next_paragraph_start) &&
             TRUE %in% stringr::str_detect(gsub("^NewSpeaker:=", "", next_paragraph_start), patterns_new_speakers)) ||
             grepl("^Titre:=", next_paragraph_start)) &&
             !title_in_progress &&
             !is.na(intervention_text) ||
             j == length(doc_text)) {

          language <- textcat::textcat(stringr::str_replace_all(intervention_text, "[[:punct:]]", ""))
          if (!(language %in% c("english", "french"))) {
            language <- "fr"
          } else {
            language <- substr(language, 1, 2)
          }

          speech_sentence_count <- clessnverse::countSentences(paste(intervention_text, collapse = " "))
          speech_word_count <- clessnverse::countWords(intervention_text)
          speech_paragraph_count <- stringr::str_count(intervention_text, "\\n\\n") + 1

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
      collapsed_doc_text <- paste(paste(doc_text, "\n\n", sep = ""), collapse = " ")
      collapsed_doc_text <- stringr::str_replace_all(string = collapsed_doc_text, pattern = "\n\n NA\n\n", replacement = "")

      clessnverse::logit(scriptname, paste("commited event", event_id, "from", event_date, "containing", intervention_seqnum, "interventions", sep = " "), logger)

    } # version finale

  } else {
    clessnverse::logit(scriptname, paste("not a parliament debate", "", sep=" "), logger)
  } #if (grepl("actualites-salle-presse", event_url))

} #for (i in 1:nrow(result))

clessnverse::logit(scriptname, paste("reaching end of", scriptname, "script"), logger = logger)
logger <- clessnverse::logclose(logger)
