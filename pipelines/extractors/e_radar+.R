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
  nationalPost = list(
    long_name  = "National Post",
    short_name = "NP",
    country    = "CAN",
    base  = "https://nationalpost.com",
    front = "/"
  ),
  tvaNouvelles = list(
    long_name  = "TVA Nouvelles",
    short_name = "TVA",
    country    = "CAN",
    base  = "https://www.tvanouvelles.ca",
    front = "/"
  ),
  globeAndMail = list(
    long_name  = "The Globe and Mail",
    short_name = "GAM",
    country    = "CAN",
    base  = "https://www.theglobeandmail.com",
    front = "/"
  ),
  vancouverSun = list(
    long_name  = "Vancouver Sun",
    short_name = "VS",
    country    = "CAN",
    base  = "https://vancouversun.com",
    front = "/"
  ),
  laPresse = list(
    long_name  = "La Presse",
    short_name = "LAP",
    country    = "CAN",
    base  = "https://www.lapresse.ca",
    front = "/"
  ),
  leDevoir = list(
    long_name  = "Le Devoir",
    short_name = "LED",
    country    = "CAN",
    base  = "https://www.ledevoir.com",
    front = "/"
  ),
  montrealGazette = list(
    long_name  = "Montreal Gazette",
    short_name = "MG",
    country    = "CAN",
    base  = "https://montrealgazette.com",
    front = "/"
  ),
  CTVNews = list(
    long_name  = "CTV News",
    short_name = "CTV",
    country    = "CAN",
    base  = "https://www.ctvnews.ca",
    front = "/"
  ),
  globalNews = list(
    long_name  = "Global News",
    short_name = "GN",
    country    = "CAN",
    base  = "https://globalnews.ca",
    front = "/"
  ),
  theStar = list(
    long_name  = "The Toronto Star",
    short_name = "TTS",
    country    = "CAN",
    base  = "https://www.thestar.com",
    front = "/"
  )
)

pushedHeadlines <<- list()


harvest_headline <- function(r, m) {
  found_supported_media <- FALSE

  if (m$short_name == "RCI") {
    RCI_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[@class="item--1"]') %>% rvest::html_nodes("a") %>% rvest::html_attr("href")
    
    if(length(RCI_extracted_headline) > 0){
      url <- paste(m$base, RCI_extracted_headline[[1]], sep="")
      found_supported_media <- TRUE
    }
  }

  if (m$short_name == "JDM") {
    JDM_extracted_headline <- r %>%
      rvest::html_elements(xpath = "//a") %>% 
      rvest::html_element("span") %>% 
      rvest::html_attr("data-story-url") %>%
      na.omit()


    if(length(JDM_extracted_headline) > 0){
      url <- JDM_extracted_headline[[1]]
      found_supported_media <- TRUE
    }
  }

  if (m$short_name == "CBC") {
    CBC_extracted_headline <<- r %>%
        rvest::html_nodes(xpath = '//*[contains(concat(" ", @class, "="), "card cardFeatured cardFeaturedReversed")]') %>%
        rvest::html_nodes('a') %>%
        rvest::html_attr("href")

    if(length(CBC_extracted_headline) == 0){
      clessnverse::logit(scriptname, "CBC: Scraping with card cardFeatured cardFeaturedReversed sclt-featurednewsprimarytopstoriescontentlistcard0 failed, trying with primaryHeadline desktopHeadline", logger)
      CBC_extracted_headline <<- r %>%
        rvest::html_nodes(xpath = '//*[@class="primaryHeadline desktopHeadline"]') %>%
        rvest::html_nodes('a') %>%
        rvest::html_attr("href")
    }

    if(length(CBC_extracted_headline) > 0){
      if (grepl("^http.*", CBC_extracted_headline[[1]])) {
        url <- CBC_extracted_headline[[1]]
      } else {
        url <- paste(m$base, CBC_extracted_headline[[1]], sep="")
      } 
      found_supported_media <- TRUE
    }
    
  }
  
  if(m$short_name == "NP"){
    NP_extracted_headline <<- r %>%
      rvest::html_nodes(xpath = '//*[contains(concat(" ", @class, "="), "hero-feed__hero-col")]') %>%
      rvest::html_nodes(xpath = '//a[@class="article-card__link"]') %>%
      rvest::html_attr("href")

    if(length(NP_extracted_headline) > 0){
      if (grepl("^http.*", NP_extracted_headline[[1]])) {
        url <- NP_extracted_headline[[1]]
      } else {
        url <- paste(m$base, NP_extracted_headline[[1]], sep="")
      } 
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "TVA"){
    TVA_extracted_headline <- r %>% rvest::html_nodes(xpath = '//*[@class="home-top-story"]') %>% rvest::html_nodes(xpath = '//*[@class="news_unit-link"]') %>% rvest::html_attr("href")
  
    if(length(TVA_extracted_headline) > 0){
      if (grepl("^http.*", TVA_extracted_headline[[1]])) {
        url <- TVA_extracted_headline[[1]]
      } else {
        url <- paste(m$base, TVA_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "GAM"){
    GAM_extracted_headline <- r %>% 
      rvest::html_nodes(xpath = '//div[@class="default__StyledLayoutContainer-qi2b9a-0 jQBUdK top-package-chain top-package-2col"]') %>%
      rvest::html_nodes(xpath = '//a[@class="CardLink__StyledCardLink-sc-2nzf9p-0 fowrAa"]') %>%
      rvest::html_attr("href")


    if(length(GAM_extracted_headline) > 0){
      headlineIndex <- 1

      if(grepl("/podcasts/the-decibel/", GAM_extracted_headline[[headlineIndex]])){
        headlineIndex <- headlineIndex + 1
      }

      if (grepl("^http.*", GAM_extracted_headline[[headlineIndex]])) {
        url <- GAM_extracted_headline[[headlineIndex]]
      } else {
        url <- paste(m$base, GAM_extracted_headline[[headlineIndex]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "VS"){
    VS_extracted_headline <- r %>% 
      rvest::html_nodes(xpath = '//div[@class="article-card__details"]') %>%
      rvest::html_nodes(xpath = '//a[@class="article-card__link"]') %>%
      rvest::html_attr("href")

    if(length(VS_extracted_headline) == 0){
      clessnverse::logit(scriptname, "VS: scraping with article-card__link failed, trying with article-card__image-link", logger)
      VS_extracted_headline <- r %>% 
        rvest::html_nodes(xpath = '//div[@class="article-card__details"]') %>%
        rvest::html_nodes(xpath = '//a[@class="article-card__image-link"]') %>%
        rvest::html_attr("href")
    }

    if(length(VS_extracted_headline) > 0){
      if (grepl("^http.*", VS_extracted_headline[[1]])) {
        url <- VS_extracted_headline[[1]]
      } else {
        url <- paste(m$base, VS_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "LAP"){
    LAP_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//div[@class="homeHeadlinesRow__main"]') %>%
      rvest::html_nodes(xpath = '//article[@data-position="1"]') %>%
      rvest::html_nodes(xpath = '//a[@class="storyCard__cover homeHeadlinesCard__cover"]') %>%
      rvest::html_attr("href")

    if(length(LAP_extracted_headline) > 0){
      if (grepl("^http.*", LAP_extracted_headline[[1]])) {
        url <- LAP_extracted_headline[[1]]
      } else {
        url <- paste(m$base, LAP_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "LED"){
    LED_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//a[@class="card-click"]') %>%
      rvest::html_attr("href")

    if(length(LED_extracted_headline) > 0){
      if (grepl("^http.*", LED_extracted_headline[[1]])) {
        url <- LED_extracted_headline[[1]]
      } else {
        url <- paste(m$base, LED_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "MG"){
    MG_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//div[contains(concat(" ", @class, "="), "hero-feed__hero-col")]') %>%
      rvest::html_nodes(xpath = '//a[@class="article-card__link"]') %>%
      rvest::html_attr("href")

    if(length(MG_extracted_headline) > 0){
      if (grepl("^http.*", MG_extracted_headline[[1]])) {
        url <- MG_extracted_headline[[1]]
      } else {
        url <- paste(m$base, MG_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "CTV"){
    CTV_extracted_headline <- r %>%
      rvest::html_nodes("h3") %>%
      rvest::html_nodes("a") %>%
      rvest::html_attr("href")

    if(length(CTV_extracted_headline) == 0){
      clessnverse::logit(scriptname, "CTV: Initial attempt failed, trying thorugh xpaths.", logger)
      CTV_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//div[@class="c-list__item__block"]') %>%
        rvest::html_nodes(xpath = '//a[@class="c-list__item__image"]') %>%
        rvest::html_attr("href")
    }

    if(length(CTV_extracted_headline) > 0){
      if (grepl("^http.*", CTV_extracted_headline[[1]])) {
        url <- CTV_extracted_headline[[1]]
      } else {
        url <- paste(m$base, CTV_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
  }

  if(m$short_name == "GN"){
    GN_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//a[@class="c-posts__headlineLink"]') %>%
      rvest::html_attr("href")

    if(length(GN_extracted_headline) == 0){  
      clessnverse::logit(scriptname, "a.c-posts__headlineLink failed, trying a.l-highImpact__headlineLink", logger)
      GN_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//a[@class="l-highImpact__headlineLink"]') %>%
        rvest::html_attr("href")
    }

    if(length(GN_extracted_headline) == 0){
      clessnverse::logit(scriptname, "a.l-highImpact__headlineLink failed, trying div.l-section__main a.c-posts__inner", logger)
      GN_extracted_headline <- r %>%
        rvest::html_nodes(xpath = '//div[@class="l-section__main"]') %>%
        rvest::html_nodes(xpath = '//a[@class="c-posts__inner"]') %>%
        rvest::html_attr("href")
    }

    if(length(GN_extracted_headline) > 0){
      if (grepl("^http.*", GN_extracted_headline[[1]])) {
        url <- GN_extracted_headline[[1]]
      } else {
        url <- paste(m$base, GN_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
    
  }

  if(m$short_name == "TTS"){
    TTS_extracted_headline <- r %>%
      rvest::html_nodes(xpath = '//a[contains(concat(" ", @class, "="), "c-feature-mediacard")]') %>%
      rvest::html_attr("href")
    if(length(TTS_extracted_headline) > 0){
      if (grepl("^http.*", TTS_extracted_headline[[1]])) {
        url <- TTS_extracted_headline[[1]]
      } else {
        url <- paste(m$base, TTS_extracted_headline[[1]], sep="")
      }
      found_supported_media <- TRUE
    }
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
    start_timestamp = Sys.time(),
    end_timestamp = Sys.time(),
    tags = paste("news,headline,radar+", m$short_name, m$long_name, sep=","),
    pillar = "radar+",
    source = url,
    media = m$short_name,
    description = "Headline page of the medias",
    object_type = "raw_data",
    source_type = "website",
    content_type = "news_headline",
    storage_class = "lake",
    country = m$country,
    schema = opt$schema,
    hashed_html = NA,
    frontpage_root_key = NA
  )

  if (r$response$status_code == 200) {
    if (grepl("text/html", r$response$headers$`content-type`)) {
      metadata$format <- "html"
      doc <- httr::content(r$response, as = 'text')
    }

    clessnverse::logit(scriptname, paste("pushing headline", url, "to hub"), logger)

    keyUrl <- url
    if(substr(keyUrl, nchar(keyUrl) - 1 + 1, nchar(keyUrl)) == '/'){
      keyUrl <- substr(keyUrl, 1, nchar(keyUrl) - 1)
    }
    #key = paste(digest::digest(url), gsub(" |-|:", "", Sys.time()), sep="_")
    key <- gsub(" |-|:|/|\\.", "_", paste(m$short_name, stringr::str_match(keyUrl, "[^/]+$"), Sys.time(), sep="_"))

    pushedHeadlines <<- append(pushedHeadlines, key)

    # handleDuplicate("radarplus/headline", key, doc, credentials)

    pushed <- FALSE
    counter <- 0

    while(!pushed && counter < 20){
      if(counter > 0){
        Sys.sleep(20)
      }
      hub_response <- clessnverse::commit_lake_item(
        data = list(
          key = key,
          path = "radarplus/headline",
          item = doc
        ),
        metadata = metadata,
        mode = if (opt$refresh_data) "refresh" else "newonly",
        credentials
      )

      if (hub_response) {
        clessnverse::logit(scriptname, paste("successfuly pushed headline", key, "to datalake"), logger)
        nb_headline <<- nb_headline + 1
        pushed <- TRUE
      } else {
        clessnverse::logit(scriptname, paste("error while pushing headline", key, "to datalake"), logger)
        counter <- counter + 1
      }
    }
    
    if(!pushed){
      warning(paste("error while pushing headline", key, "to datalake"))
    }
  } else {
      clessnverse::logit(scriptname, paste("there was an error getting url", url), logger)
      warning(paste("there was an error getting url", url))
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
      start_timestamp = Sys.time(),
      end_timestamp = Sys.time(),
      tags = paste("news,headline,radar+", m$short_name, m$long_name, sep=","),
      pillar = "radar+",
      source = url,
      media = m$short_name,
      description = "Headline page of the medias",
      object_type = "raw_data",
      source_type = "website",
      content_type = "news_headline",
      storage_class = "lake",
      country = m$country,
      schema = opt$schema,
      keys_une = NA
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
      keyUrl <- url
      if(substr(keyUrl, nchar(keyUrl) - 1 + 1, nchar(keyUrl)) == '/'){
        keyUrl <- substr(keyUrl, 1, nchar(keyUrl) - 1)
      }

      key <- gsub(" |-|:|/|\\.", "_", paste(m$short_name, stringr::str_match(keyUrl, "[^/]+$"), Sys.time(), sep="_"))
      if (opt$refresh_data) mode <- "refresh" else mode <- "newonly"

      if(handleDuplicate("radarplus/frontpage", key, doc, credentials, m$short_name)){
        clessnverse::logit(scriptname, "DUPLICATED", logger)
      } else {
        clessnverse::logit(scriptname, "NOT DUPLICATED", logger)
      }

      pushed <- FALSE
      counter <- 0

      while(!pushed){
        if(counter > 0){
          Sys.sleep(20)
        }
        hub_response <- clessnverse::commit_lake_item(
          data = list(
            key = key,
            path = "radarplus/frontpage",
            item = doc
          ),
          metadata = metadata,
          mode = mode,
          credentials = credentials
        )

        if (hub_response) {
          clessnverse::logit(scriptname, paste("successfuly pushed frontpage", key, "to datalake"), logger)
          nb_frontpage <<- nb_frontpage + 1
          pushed <- TRUE
        } else {
          clessnverse::logit(scriptname, paste("error while pushing frontpage", key, "to datalake"), logger)
          counter <- counter + 1
        }
      }

      if(pushed){
          harvest_headline(r, m)
      } else {
          warning(paste("error while pushing frontpage", key, "to datalake"))
      }

    } else {
       clessnverse::logit(scriptname, paste("there was an error getting url", url), logger)
       warning(paste("there was an error getting url", url))
    }
  }#</for>
  
}

handleDuplicate <- function(path, key, doc, credentials, mediaSource){
  # data <- hublot::filter_lake_items(credentials, filter = filter)

  # retrieve_lake_item 
  r <- hublot::filter_lake_items(credentials, list(path = path, key__contains = mediaSource))

  if(length(r$result) == 0){
    clessnverse::logit(scriptname, "No results found with the same key", logger)
    return(FALSE)
  }

  lake_item <- hublot::retrieve_lake_item(
    # Sorted by key alphabetical ascending. Means that the last element is most recent
    id = r$result[[length(r$result)]]$id, 
    credentials = credentials
  )
  valeria_url <- lake_item[[6]]

  r <- rvest::session(valeria_url)

  if (r$response$status_code == 200) {
      if (grepl("text/html", r$response$headers$`content-type`)) {
        inHub_doc <- httr::content(r$response, as = 'text')
        
        if(doc == inHub_doc){
          # here is where I'd make the thing where I update instead of save
          return(TRUE)
        }
      }
  }

  return(FALSE)
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

    # opt <<- list(
    #     log_output = c("console,file"),
    #     scraping_method = "frontpage",
    #     refresh_data = TRUE
    # )

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
    clessnverse::logit(scriptname, w$message, logger)
    print(w)
    warnOutput <<- paste("WARNING: ", w$message)
    final_message <<- if (final_message == "") warnOutput else paste(final_message, "\n", warnOutput, sep="")    
    status <<- 2
  }),
    
  error = function(e) {
    clessnverse::logit(scriptname, e$message, logger)
    print(e)
    errorOutput <<- paste("ERROR: ", e$message)
    final_message <<- if (final_message == "") errorOutput else paste(final_message, "\n", errorOutput, sep="")    
    status <<- 1
  },
  
  finally={
    # if status == 0, no errors happened. Add the breakdown of every media source to final message
    if(status == 0){
      for (pushedHeadline in pushedHeadlines) { 
        final_message <<- if (final_message == "") pushedHeadline else paste(final_message, "\n", pushedHeadline, sep="")  
      }
    }

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
    quit(status = status)
  }
)

