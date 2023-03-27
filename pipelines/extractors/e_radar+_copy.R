###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                                  e_radar+                        
#                                                                             
# This extractor extracts data from 13 medias the most interesting for the 
# CLESSN 
# - First it extracts the html page from the headline (home) page of each media
#   and stores the html as a file in the CLESSN data lake
# - Second, it takes all the urls from the articles contained in each page
#   and then stores them in the dtaalake also
#                                                                             
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################
medias_urls <- list(
  cbcnews = list(
    long_name  = "CBC News",
    short_name = "CBC",
    country    = "CAN",
    base       = "https://www.cbc.ca",
    front      = "/news"
  ),
  jdm = list(
    long_name  = "Le Journal de Montréal",
    short_name = "JDM",
    country    = "CAN",
    base  = "https://www.journaldemontreal.com",
    front = "/"
  ),
  radiocan = list(
    long_name  = "Radio-Canada Info",
    short_name = "RCI",
    country    = "CAN",
    base  = "https://ici.radio-canada.ca",
    front = "/info"
  ),
  tvaNouvelles = list(
    long_name  = "TVA Nouvelles",
    short_name = "TVA",
    country    = "CAN",
    base  = "https://www.tvanouvelles.ca/",
    front = "/"
  )
)


harvest_headline <- function(r, m) {
  found_supported_media <- FALSE

  if (m$short_name == "RCI") {
    RCI_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[@class="item--1"]') %>% rvest::html_nodes("a") %>% rvest::html_attr("href")
    url <- paste(m$base, RCI_extracted_headline[[1]], sep="")
    found_supported_media <- TRUE
  }

  if (m$short_name == "JDM") {
    JDM_extracted_headline <- r %>%
      rvest::html_elements(xpath = "//a") %>% 
      rvest::html_element("span") %>% 
      rvest::html_attr("data-story-url") %>%
      na.omit()
    url <- JDM_extracted_headline[[1]]
    found_supported_media <- TRUE
  }

  if (m$short_name == "CBC") {
    CBC_extracted_headline <<- r %>%
      # rvest::html_nodes(xpath = '//*[@class="primaryHeadlineLink sclt-contentpackageheadline"]')
      rvest::html_nodes(xpath = '//div[@class="card cardFeatured cardFeaturedReversed sclt-featurednewsprimarytopstoriescontentlistcard0"]') 
      %>%
      rvest::html_node("a") 
      %>%
      rvest::html_attr("href")

    clessnverse::logit(scriptname, CBC_extracted_headline, logger)
    if (length(CBC_extracted_headline) == 0) {
      CBC_extracted_headline <<- r %>%
        rvest::html_nodes(xpath = '//a[@class="primaryHeadlineLink sclt-contentpackageheadline"]') %>% 
        rvest::html_attr("href")
    }

    if (grepl("^http.*", CBC_extracted_headline[[1]])) {
      url <- CBC_extracted_headline[[1]]
    } else {
      url <- paste(m$base, CBC_extracted_headline[[1]], sep="")
    } 
    found_supported_media <- TRUE
  }

  if(m$short_name == "TVA"){
    TVA_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[@class="home-top-story"]') %>% rvest::html_nodes(xpath = '//*[@class="news_unit-link"]') %>% rvest::html_attr("href")
    
    if (grepl("^http.*", TVA_extracted_headline[[1]])) {
      url <- TVA_extracted_headline[[1]]
    } else {
      url <- paste(m$base, TVA_extracted_headline[[1]], sep="")
    }
    found_supported_media <- TRUE
  }

  if (!found_supported_media) {
    clessnverse::logit(scriptname, paste("no supported media found", m$short_name), logger)
    warning(paste("no supported media found", m$short_name))
    return()
  }

  clessnverse::logit(scriptname, paste("getting headline from", url), logger)

  r <- rvest::session(url)


  metadata <- list(
    format = "",
    timespamp = Sys.time(),
    tags = paste("news,headline,radar+", m$short_name, m$long_name, sep=","),
    pillar = "radar+",
    source = url,
    media = m$short_name,
    description = "Headline page of the medias",
    object_type = "raw_data",
    source_type = "website",
    content_type = "news_headline",
    storage_class = "lake",
    country = m$country
  )

  if (r$response$status_code == 200) {
    if (grepl("text/html", r$response$headers$`content-type`)) {
      metadata$format <- "html"
      doc <- httr::content(r$response, as = 'text')
    }

    clessnverse::logit(scriptname, paste("pushing headline", url, "to hub"), logger)

    #key = paste(digest::digest(url), gsub(" |-|:", "", Sys.time()), sep="_")
    key <- gsub(" |-|:|/|\\.", "_", paste(stringr::str_match(url, "[^/]+$"), Sys.time(), sep="_"))

    hub_response <- clessnverse::commit_lake_item(
      data = list(
        key = key,
        path = paste("radarplus/headline/", m$short_name, sep=""),
        item = doc
      ),
      metadata = metadata,
      mode = if (opt$refresh_data) "refresh" else "newonly",
      credentials
    )

    if (hub_response) {
      clessnverse::logit(scriptname, paste("successfuly pushed headline", key, "to datalake"), logger)
      nb_headline <<- nb_headline + 1
    } else {
      clessnverse::logit(scriptname, paste("error while pushing headline", key, "to datalake"), logger)
    }

  } else {
      clessnverse::logit(scriptname, paste("there was an error getting url", url), logger)
  }

} #</my_function>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    
  clessnverse::logit(scriptname, "starting main function", logger)
  
  for (m in medias_urls) {
    url <- paste(m$base, m$front, sep="")
    clessnverse::logit(scriptname, paste("getting frontpage from", url), logger)

    metadata <- list(
      format = "",
      timespamp = Sys.time(),
      tags = paste("news,frontpage,radar+", m$short_name, m$long_name, sep=","),
      pillar = "radar+",
      source = url,
      media = m$short_name,
      description = "Frontpage page of the medias where the headline sits",
      object_type = "raw_data",
      source_type = "website",
      content_type = "news_frontpage",
      storage_class = "lake",
      country = m$country
    )

    r <<- rvest::session(url)
    m <<- m

    if (r$response$status_code == 200) {
      if (grepl("text/html", r$response$headers$`content-type`)) {
        metadata$format <- "html"
        doc <- httr::content(r$response, as = 'text')
      }

      clessnverse::logit(scriptname, paste("pushing frontpage", url, "to hub"), logger)

      #key = paste(digest::digest(url), gsub(" |-|:", "", Sys.time()), sep="_")
      key <- gsub(" |-|:|/|\\.", "_", paste(stringr::str_match(url, "[^/]+$"), Sys.time(), sep="_"))
      if (opt$refresh_data) mode <- "refresh" else mode <- "newonly"

      hub_response <- clessnverse::commit_lake_item(
        data = list(
          key = key,
          path = paste("radarplus/frontpage/", m$short_name, sep=""),
          item = doc
        ),
        metadata = metadata,
        mode = mode,
        credentials = credentials
      )

      if (hub_response) {
        clessnverse::logit(scriptname, paste("successfuly pushed frontpage", key, "to datalake"), logger)
        nb_frontpage <<- nb_frontpage + 1
        harvest_headline(r, m)
      } else {
        clessnverse::logit(scriptname, paste("error while pushing frontpage", key, "to datalake"), logger)
      }

    } else {
       clessnverse::logit(scriptname, paste("there was an error getting url", url), logger)
    }
  }#</for>
  
}



tryCatch( 
  withCallingHandlers(
  {
    library(dplyr)

    status <<- 0
    final_message <<- ""
    nb_frontpage <<- 0
    nb_headline <<- 0

    if (!exists("scriptname")) scriptname <<- "e_radar+"

    # valid options for this script are
    #    log_output = c("file","console","hub")
    #    scrapind_method = c("range", "start_date", num_days, start_parliament, num_parliament) | "frontpage" (default)
    #    hub_mode = "refresh" | "skip" | "update"
    #    translate = "TRUE" | "FALSE"
    #    refresh_data = "TRUE" | "FALSE"
    #    
    #    you can use log_output = c("console") to debug your script if you want
    #    but set it to c("file") before putting in automated containerized production

    opt <<- list(
        log_output = "console",
        scraping_method = "frontpage",
        refresh_data = TRUE
    )

    if (!exists("opt")) {
      opt <<- clessnverse::process_command_line_options()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) {
      logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    }

    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    # connect to hublot
    credentials <<- hublot::get_credentials(
      Sys.getenv("HUB3_URL"), 
      Sys.getenv("HUB3_USERNAME"), 
      Sys.getenv("HUB3_PASSWORD")
    )

    # or connect to hub2
    #clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    main()
  },

  warning = function(w) {
    clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
    print(w)
    final_message <<- if (final_message == "") w else paste(final_message, "\n", w, sep="")    
    status <<- 2
  }),
    
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
    print(e)
    final_message <<- if (final_message == "") e else paste(final_message, "\n", e, sep="")    
    status <<- 1
  },
  
  finally={
    clessnverse::logit(scriptname, final_message, logger)

    clessnverse::logit(scriptname, 
      paste(
        nb_frontpage, 
        "frontpages were extracted and",
        nb_headline,
        "headlines were extracted"
      ),
      logger
    )

    clessnverse::logit(scriptname, paste("Execution of", scriptname, "program terminated"), logger)
    clessnverse::log_close(logger)
    if (exists("logger")) rm(logger)
    print(paste("exiting with status", status))
    #quit(status = status)
  }
)

