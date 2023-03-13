###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                          e_eu_parliament_plenary                        
#                                                                             
# This script extracts HTML pages from the plenary sessions of the European
# parliament and stores them in the CLESSN data lake
#                                                                             
###############################################################################

###############################################################################
########################      Functions and Globals      ######################
###############################################################################

my_global_variable <- "my_value"

extract_debates <- function(url_list) {

  for (url in url_list) {
    event_id <- stringr::str_match(url, "\\CRE-(.*)\\.")[2]
    event_id <- stringr::str_replace_all(event_id, "[[:punct:]]", "")
    parliament_number <- stringr::str_match(url, "\\CRE-(.*)\\-")[2]
    parliament_number <- stringr::str_match(parliament_number, "^[^-]*")[1]
    event_date <- stringr::str_match(url, "\\CRE-(.*)\\_")[2]
    event_date <- stringr::str_match(event_date, "-(.*)$")[2]

    metadata <- list(
      format = "",
      tags = "parliament,europe,agora+,european_parliament",
      pillar = "agora+",
      source = url,
      parliament_number = parliament_number,
      event_date = event_date,
      description = "european parliament plenary session debate transcription",
      object_type = "raw_data",
      source_type = "website",
      contant_type = "parliament_debate_transcription",
      storage_class = "lake",
      institution = "european_parliament"
    )

    clessnverse::logit(scriptname, paste("grabbing", url), logger)

    r <- rvest::session(url)

    if (r$response$status_code == 200) {
      clessnverse::logit(scriptname, "successful get", logger)

      supported_format <- FALSE

      if (grepl("text/html", r$response$headers$`content-type`)) {
        metadata$format <- "html"
        doc <- httr::content(r$response, as = 'text')
        supported_format <- TRUE
      }

      if (grepl("application/xml", r$response$headers$`content-type`)) {
        metadata$format <- "xml"
        doc <- httr::content(r$response, as = 'text')
        supported_format <- TRUE
      }

      if (!supported_format) {
        clessnverse::logit(scriptname, paste("unsupported format", r$response$headers$`content-type`), logger)
        status <<- 2
        next
      }

      clessnverse::logit(scriptname, paste("pushing", event_id), logger)

      r <- clessnverse::commit_lake_item(
        data = list(
          key = event_id,
          path = "agoraplus/european_parliament",
          item = doc
        ),
        metadata = metadata,
        mode = if (opt$refresh_data) "refresh" else "newonly",
        credentials
      )

      if (r) {
        clessnverse::logit(scriptname, paste("successfuly pushed debate", event_id, "to datalake"), logger)
        harvested <<- harvested + 1
      } else {
        clessnverse::logit(scriptname, paste("error while pushing debate", event_id, "to datalake"), logger)
      }

    } else {
      clessnverse::logit(scriptname, paste("there was an error getting", url), logger)
      warning(paste("could not read", url))
      next
    }

  }#</while>

} #</my_function>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
  base_url <- "https://www.europarl.europa.eu"

  clessnverse::logit(scriptname, "starting main function", logger)
  clessnverse::logit(scriptname, "parsing options", logger)

  if(length(opt$method) == 1 && grepl(",", opt$method)) {
    # The option parameter given to the script is multivalued - parse
    opt$method <- trimws(strsplit(opt$method, ",")[[1]])
  }

  scraping_method<- opt$method[1]
  start_date <- opt$method[2]
  num_days <- as.integer(opt$method[3])
  start_parliament <- as.integer(opt$method[4])
  num_parliaments <- as.integer(opt$method[5])
  format <- opt$method[6]

  clessnverse::logit(
    scriptname, 
    paste(
      "scraping method is ", scraping_method,
      " with start_date=", start_date,
      " num_days=", num_days,
      " start_parliament=", start_parliament,
      " num_parliaments=", num_parliaments,
      sep=""
    ),
    logger
  )

  if (scraping_method == "frontpage") {
    content_url <- "/plenary/en/debates-video.html#sidesForm"
    
    source_page <- xml2::read_html(paste(base_url,content_url,sep=""))
    source_page_html <- XML::htmlParse(source_page, useInternalNodes = TRUE)
    
    urls <- rvest::html_nodes(source_page, 'a')
    urls <- urls[grep("\\.xml", urls)]
    urls <- urls[grep("https", urls)]
    
    urls_list <- rvest::html_attr(urls, 'href')
    urls_list <- as.list(urls_list)
  } else {
    if (scraping_method == "range") {
      date_vect <- seq( as.Date(start_date), by=1, len=num_days)
      
      urls_list <- list()
      nb_deb <- 0
      
      for (d in date_vect) {
        for (p in c(start_parliament:(start_parliament+num_parliaments-1))) {
          url <- paste(base_url, "/doceo/document/CRE-", p, "-", as.character(as.Date(d, "1970-01-01")), "_EN.", format,sep= '')
          clessnverse::logit(scriptname, paste("trying", url), logger)
          r <- httr::GET(url)
          if (r$status_code == 200) {
            urls_list <- c(urls_list, url)
            nb_deb <- nb_deb + 1
          }
        }
      }
    } else {
      clessnverse::logit(scriptname, paste("invalid scraping_method", scraping_method), logger)
    }
  }
  
  clessnverse::logit(
    scriptname = scriptname,
    message = paste("list of urls containing", length(urls_list), "debates"), 
    logger = logger
  )

  extract_debates(urls_list)

}



tryCatch( 
  withCallingHandlers(
  {
    library(dplyr)
    
    status <<- 0
    final_message <<- ""
    harvested <<- 0

    if (!exists("scriptname")) scriptname <<- "e_eu_parliament_plenary"

    # valid options for this script are
    #    log_output = c("file","console","hub")
    #    scraping_option = c("range", "start_date", num_days, start_parliament, num_parliament, "html" | "xml") | "frontpage" (default)
    #    translate = "TRUE" | "FALSE"
    #    refresh_data = "TRUE" | "FALSE"
    #    
    #    you can use log_output = c("console") to debug your script if you want
    #    but set it to c("file") before putting in automated containerized production

    #opt <<- list(
    #   backend = "hub",
    #   log_output = c("console"),
    #   method = c("range", "2019-07-01", 1, 9, 1, "xml"),
    #   refresh_data = TRUE
    #)

    if (!exists("opt")) {
      opt <<- clessnverse::process_command_line_options()
    }
    
    if (!exists("logger") || is.null(logger) || logger == 0) {
      logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    }

    clessnverse::logit(
      scriptname, 
      paste("options are", paste(names(opt), opt, collapse=" ", sep="=")),
      logger
    )
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    # connect to hublot
    credentials <<- hublot::get_credentials(
      Sys.getenv("HUB3_URL"), 
      Sys.getenv("HUB3_USERNAME"), 
      Sys.getenv("HUB3_PASSWORD"))
    
    # or connect to hub2
    clessnhub::login(
      Sys.getenv("HUB_USERNAME"),
      Sys.getenv("HUB_PASSWORD"),
      Sys.getenv("HUB_URL"))

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
        harvested, 
        "debates were harvested from the european parliament website"
      ),
      logger
    )

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::log_close(logger)
    if (exists("logger")) rm(logger)
    print(paste("exiting with status", status))
    if (opt$prod) quit(status = status)
  }
)