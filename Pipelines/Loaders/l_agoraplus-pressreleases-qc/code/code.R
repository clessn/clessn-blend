###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        l_agoraplus-pressreleases-qc                         #
#                                                                             #
# Script pour extraire les pages html (ou autre format - ex: xml) sur le web  #
# contenant les communiqués de presse des partis politiques du Québec         #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################
safe_httr_GET <- purrr::safely(httr::GET)


get_lake_press_releases <- function (parties_list) {
    press_releases_lake_items_list <- list()

    for (party in parties_list) {
        filter <- c(list(metadata__political_party = party), lake_items_selection_matadata)

        data <- hublot::filter_lake_items(credentials, filter = filter)

        press_releases_lake_items_list <- if (length(press_releases_lake_items_list) > 0) {
            mapply(
                c,
                press_releases_lake_items_list, 
                list(
                    file=tidyjson::spread_all(data$results)$file, 
                    key=tidyjson::spread_all(data$results)$key,
                    party_acronym=tidyjson::spread_all(data$results)$metadata.political_party
                    ),
                SIMPLIFY = FALSE
                )
        } else {
           list(
            file=tidyjson::spread_all(data$results)$file, 
            key=tidyjson::spread_all(data$results)$key,
            party_acronym=tidyjson::spread_all(data$results)$metadata.political_party
            )
        }
    }

    return(press_releases_lake_items_list)
}





extract_press_release_info <- function(party_acronym, xml_root) {

    press_release_structured <- list()

    title <- NULL  
    body <- NULL
    date <- NULL

    if (party_acronym == "CAQ") {
        title <- XML::getNodeSet(xml_root, ".//h1[@class = 'main-title']")
        title <- trimws(XML::xmlValue(title))

        body <- XML::getNodeSet(xml_root, ".//div[@class='spacing-jumbo mb-5']")
        body <- trimws(XML::xmlValue(body))
        
        date <- XML::getNodeSet(xml_root, ".//span[@class='']")
        date <- trimws(XML::xmlValue(date))
    }

    if (party_acronym == "QS") {
        title <- xml_root$name
        body <- XML::htmlTreeParse(xml_root$description, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE, encoding = "UTF-8")
        body <- XML::xmlRoot(body, skip = TRUE, addFinalizer = TRUE)
        body <- XML::xmlValue(body)
        date <- as.Date(as.numeric(xml_root$date/86400), origin = "1970-01-01")
    }

    if (party_acronym == "PLQ") {
        title <- XML::getNodeSet(xml_root, ".//h1")
        title <- trimws(XML::xmlValue(title))

        body <- XML::getNodeSet(xml_root, ".//article")
        body <- trimws(XML::xmlValue(body))
        
        date <- XML::getNodeSet(xml_root, ".//span[@class='date']")[[1]]
        date <- trimws(XML::xmlValue(date))
    }

    if (party_acronym == "PCQ") {
        title <- XML::getNodeSet(xml_root, ".//h1")
        title <- trimws(XML::xmlValue(title))

        body <- XML::getNodeSet(xml_root, ".//article")
        body <- trimws(XML::xmlValue(body))

        date <- XML::getNodeSet(xml_root, ".//div[@class='byline text-muted small']")[[1]]
        date <- trimws(XML::xmlValue(date))
        date <- stringr::str_extract(date, "\\d\\d\\-\\d\\d\\-\\d\\d\\d\\d")
    }

    if (party_acronym == "PQ") {
        title <- XML::getNodeSet(xml_root, ".//title")
        title <- trimws(XML::xmlValue(title))

        body <- XML::getNodeSet(xml_root, ".//div[@id='nouvelle-box']")
        body <- trimws(XML::xmlValue(body))

        date <- XML::getNodeSet(xml_root, ".//span[@class='date updated']")
        date <- trimws(XML::xmlValue(date))
    }

    if (!is.null(title) && !is.null(data) && !is.null(body)) {
        press_release_structured <- list(title = title,
                                         date = date,
                                         body = body,
                                         political_party = party_acronym,
                                         country = "CAN",
                                         province_or_state = "QC")
    }


    return(press_release_structured)
}





parse_press_release <- function(key, party_acronym, lake_file_url) {
    r <- safe_httr_GET(lake_file_url)

    if (r$result$status_code == 200) {
        clessnverse::logit(scriptname, paste("successful GET of", party_acronym, "press release", key), logger)

        index_html <- httr::content(r$result, as="text", encoding = "utf-8")

        if (grepl("text\\/html", r$result$headers$`content-type`)) {
            index_xml <- XML::htmlTreeParse(index_html, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
            xml_root <- XML::xmlRoot(index_xml, skip = TRUE, addFinalizer = TRUE)
        } else {
            if (grepl("(application\\/rss\\+xml)|(application\\/xml)", r$result$headers$`content-type`)) {
                index_xml <- XML::xmlTreeParse(index_html, useInternalNodes = TRUE)
                xml_root <- XML::xmlRoot(index_xml, skip = TRUE, addFinalizer = TRUE)
            } else {
                if (grepl("application/json", r$result$headers$`content-type`)) {
                    index_xml <- tidyjson::spread_all(index_html)
                    xml_root <- index_xml
                } else {
                    stop("not an xml nor an html document")
                }     
            }
        }

        clessnverse::logit(scriptname, paste("Trying to extract", party_acronym,"press release content"), logger)

        press_release_structured <- extract_press_release_info(party_acronym, xml_root)

    } else {
        clessnverse::logit(scriptname, paste("Error getting", party_acronym, "press release page from data lake with key", key), logger)
        press_release_structured <- NULL
    } #if (r$result$status_code == 200)

    return(press_release_structured)
}



###############################################################################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################


###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################


###############################################################################
######################   Get Data Sources from Dropbox   ######################
###############################################################################


###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    parties_list <- list("CAQ", "PLQ", "QS", "PCQ", "PQ")
    
    clessnverse::logit(scriptname, paste("Getting the following political parties press releases from the datalake", paste(parties_list, collapse = '\n'), sep="\n"), logger)

    warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0) 
    lakes_items_list <- get_lake_press_releases(parties_list)

    if (opt$hub_mode == "refresh") {
        # update the entire warehouse with the entire lake items set
        items_list <- lakes_items_list
    } else {
        # add only new items from the lake into the warehouse
        # that is only keys that exist in the lake and don't exist in the warehouse
        index <- which(!(lakes_items_list$key %in% warehouse_items_list$key))
        items_list <- list(file = lakes_items_list$file[index], 
                        key = lakes_items_list$key[index],
                        party_acronym = lakes_items_list$party_acronym[index])
    }
  
    clessnverse::logit(scriptname,  paste("loading", length(items_list$key) , "items to the data warehouse",  sep=" "), logger)

    for (i in 1:length(items_list$key)) {
        key <- items_list$key[i]
        lake_file_url <- items_list$file[i]
        party_acronym <- items_list$party_acronym[i]
        clessnverse::logit(scriptname,  paste("processing item #", i, "key = ", items_list$key[i], sep=" "), logger)

        press_release_structured <- parse_press_release(key, party_acronym, lake_file_url)

        if (length(press_release_structured) > 0) {
            clessnverse::logit(scriptname,  paste("committing item #", i, "key = ", items_list$key[i], sep=" "), logger)
            clessnverse::commit_warehouse_row(warehouse_table, key, press_release_structured, mode = opt$hub_mode, credentials)
        } else {
            clessnverse::logit(scriptname,  paste("could not parse item #", i, "key = ", items_list$key[i], sep=" "), logger)
        }

    }

    clessnverse::logit(scriptname, paste(i, "press releases were loaded to the data warehouse"), logger)

}




tryCatch( 
  {
    # Package
    library(dplyr) 

    # Globals : scriptname, opt, logger, credentials
    lake_path <- ""
    lake_items_selection_matadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    warehouse_table <- "political_parties_press_releases"

    if (!exists("scriptname")) scriptname <<- "l_agoraplus-pressreleases-qc"

    opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, c("file","console"), Sys.getenv("LOG_PATH"))
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))
    
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    # Call main script    
    main()
  },
  
  # Handle an error or a call to stop function in the code
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
    print(e)
  },
  
  # Terminate gracefully whether error or not
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::logclose(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)
  }
)

