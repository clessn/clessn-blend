###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        e_agoraplus-pressreleases-qc                         #
#                                                                             #
# Script pour extraire les pages html (ou autre format - ex: xml) sur le web  #
# contenant les communiqués de presse des partis politiques du Québec         #
#                                                                             #
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################

caq_url <- "https://coalitionavenirquebec.org/fr/actualites/"
plq_url <- "https://plq.org/fr/communiques-de-presse/"
qs_url  <- "https://api-wp.quebecsolidaire.net/feed?post_type=articles&types=communiques-de-presse"
pcq_url <- "https://www.conservateur.quebec/communiques"
pq_url  <- "https://pq.org/nouvelles/"

caq_base_url <- "https://coalitionavenirquebec.org"
plq_base_url <- "https://plq.org"
qs_base_url  <- "https://quebecsolidaire.net"
pcq_base_url <- "https://www.conservateur.quebec"
pq_base_url  <- "https://pq.org/nouvelles"


safe_httr_GET <- purrr::safely(httr::GET)






extract_urls_list <- function(party_acronym, xml_root, scriptname, logger) {
    urls_list <- list()

    if (party_acronym == "CAQ") {
        nodes_list <- XML::getNodeSet(xml_root, ".//div[@class = 'flex-item news-grid-item']")
        for (node in nodes_list) {
            url_node <- XML::getNodeSet(node, "a")
            urls_list <- c(urls_list, XML::xmlGetAttr(url_node[[1]], "href"))
        }
        return(urls_list)
    }

    if (party_acronym == "PLQ") {
        nodes_list <- XML::getNodeSet(xml_root, ".//div[@class = 'highlight']")
        for (node in nodes_list) {
            url_node <- XML::getNodeSet(node, "a")
            urls_list <- c(urls_list, XML::xmlGetAttr(url_node[[1]], "href"))
        }

        nodes_list <- XML::getNodeSet(xml_root, ".//div[@class = 'newsSmall']")
        for (node in nodes_list) {
            url_node <- XML::getNodeSet(node, "a")
            urls_list <- c(urls_list, XML::xmlGetAttr(url_node[[1]], "href"))
        }
        return(urls_list)
    }

    if (party_acronym == "QS") {
        nodes_list <- XML::getNodeSet(xml_root, ".//item")
        for (node in nodes_list) {
            url_node <- XML::getNodeSet(node, "link")
            urls_list <- c(urls_list, XML::xmlValue(url_node[[1]], encoding="fr_CA.UTF-8"))
        }
        return(urls_list)
    }

    if (party_acronym == "PCQ") {
        nodes_list <- XML::getNodeSet(xml_root, ".//header[@class = 'mb-3']")
        for (node in nodes_list) {
            url_node <- XML::getNodeSet(node, "..//a")
            urls_list <- c(urls_list, paste(pcq_base_url,  XML::xmlGetAttr(url_node[[1]], "href"), sep=""))
        }
        return(urls_list)
    }

    if (party_acronym == "PQ") {
        nodes_list <- XML::getNodeSet(xml_root, ".//a[@class = 'elementor-post__thumbnail__link']")
        for (node in nodes_list) {
            urls_list <- c(urls_list, XML::xmlGetAttr(node, "href"))
        }
        return(urls_list)
    }

    return(urls_list)
} #</function extract_urls_list>





scrape_party_press_release <- function(party_acronym, party_url, scriptname, logger, credentials) {
    clessnverse::logit(scriptname, paste("scraping", party_acronym, "main press release page", party_url), logger)
    r <- safe_httr_GET(party_url, httr::config(ssl_verifypeer=0), httr::timeout(30))

    if (!is.null(r$result) && r$result$status_code == 200) {
        # On extrait les section <a> qui contiennent les liens vers chaque communiqué
        clessnverse::logit(scriptname, paste("successful GET of", party_acronym, "main press release page", party_url), logger)
    
        index_html <- httr::content(r$result, as="text", encoding = "UTF-8")
    
        if (grepl("text\\/html", r$result$headers$`content-type`)) {
            index_xml <- XML::htmlTreeParse(index_html, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
        } else {
            if (grepl("application\\/rss\\+xml", r$result$headers$`content-type`)) {
                index_xml <- XML::xmlTreeParse(index_html, useInternalNodes = TRUE)
            } else {
                if (grepl("application/json", r$result$headers$`content-type`)) {
                    index_xml <- XML::xmlTreeParse(index_html, useInternalNodes = TRUE)
                } else {
                    stop("not an xml nor an html document")
                }
            }
        }

        xml_root <- XML::xmlRoot(index_xml, skip = TRUE, addFinalizer = TRUE)
    
        clessnverse::logit(scriptname, paste("Trying to extract", party_acronym,"press releases URLs from main press release page"), logger)
    
        press_releases_urls_list <- extract_urls_list(party_acronym, xml_root, scriptname, logger)

        # On loop à travers toutes les URL de ls liste des communiqués
        # On les scrape et stocke sur le hublot
        for (url in press_releases_urls_list) {
            clessnverse::logit(scriptname, paste("scraping", party_acronym, "press release page", url), logger)
            
            r <- safe_httr_GET(url, httr::config(ssl_verifypeer=0))
            
            if (!is.null(r$result) && r$result$status_code == 200) {
                clessnverse::logit(scriptname, paste ("successful GET on", party_acronym, "press release at URL", url), logger)
                html <- httr::content(r$result, as="text", encoding = "utf-8")

                if (r$result$headers$`content-type` == "application/json") {
                    # Empiricaly found trick to re-encode to utf-8 for real
                    # Because for a json structure httr::content has a bug for encoding to utf-8
                    #html <- toString(jsonlite::toJSON(tidyjson::spread_all(html)$..JSON))
                    html <- toString(jsonlite::toJSON(tidyjson::spread_all(html)$..JSON, auto_unbox = T))
                    html <- gsub("^\\[|\\]$", "", html)
                }

                if (!is.null(html)) {
                    # Construit le data pour le hub
                    key <- digest::digest(url)
                    path <- "political_party_press_releases"

                    format <- if (grepl("text\\/html", r$result$headers$`content-type`)) "html" else if (grepl("application\\/rss\\+xml", r$result$headers$`content-type`)) "xml" else if (grepl("application\\/json", r$result$headers$`content-type`)) "json" else ""

                    lake_item_metadata <- list(
                        object_type = "raw_data",
                        format = format,
                        content_type = "political_party_press_release",
                        storage_class = "lake",
                        source_type = "website",
                        source = url,
                        tags = "elxn-qc2022, vitrine_democratique, polqc",
                        description = "Communiqués de presse des partis politiques",
                        political_party = party_acronym,
                        country = "CAN",
                        province_or_state = "QC"
                    )

                    lake_item_data <- list(key = key, path = path, item = html)


                    clessnverse::commit_lake_item(
                        data = lake_item_data, 
                        metadata = lake_item_metadata, 
                        mode = opt$hub_mode, 
                        credentials = credentials)
                } else {
                    clessnverse::logit(scriptname, paste("no press release from", party_acronym, "at", urls_list[[i]]), logger)
                } #if (!is.null(html)) 
            
            } else {
                clessnverse::logit(scriptname, paste("error accessing", party_acronym, "press release page", urls_list[[i]]), logger)
                clessnverse::logit(scriptname, r$error, logger)
                warning(paste(r$error, paste("error accessing", party_acronym, "press release page", urls_list[[i]]), sep="\n"))
            }
        } #for (url in press_releases_urls_list)
        
        partyUpdateMessage = paste(length(press_releases_urls_list), "press releases were scraped from the", party_acronym, "web site")

        clessnverse::logit(scriptname, partyUpdateMessage, logger)
        cat(partyUpdateMessage)
        final_message <<- if(final_message == "") partyUpdateMessage else paste(final_message, "\n", partyUpdateMessage)
        total_press_releases_scraped <<- total_press_releases_scraped + length(press_releases_urls_list)
    } else {
        clessnverse::logit(scriptname, paste("Error getting", party_acronym, "main press release page"), logger)
        warning(paste(r$error, paste("Error getting", party_acronym, "main press release page"), sep="\n"))
    } #if (r$result$status_code == 200)
} #</function scrape_party_press_release>




###############################################################################
######################  Get Data Sources from Warehouse  ######################
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

main <- function(scriptname, logger, credentials) {
  
  urls_list <- list(
    c("CAQ", caq_url),
    c("PLQ", plq_url),
    c("QS", qs_url),
    c("PCQ", pcq_url),
    c("PQ", pq_url))
  
  clessnverse::logit(scriptname, paste("Scraping the following political parties press releases", paste(urls_list, collapse = '\n'), sep="\n"), logger)
  
  for (i in 1:length(urls_list)) {
    clessnverse::logit(scriptname, paste("scraping", urls_list[[i]][1], urls_list[[i]][2]), logger)
    scrape_party_press_release(
        party_acronym = urls_list[[i]][1],
        party_url = urls_list[[i]][2],
        scriptname,
        logger,
        credentials)
  }
  
}



final_message <<- ""


tryCatch( 
    withCallingHandlers(
    {
        library(dplyr)
        
        if (!exists("scriptname")) scriptname <<- "e_pressreleases_qc"

        opt <<- list(log_output = c("file"), hub_mode = "refresh")

        if (!exists("opt")) {
            opt <<- clessnverse::processCommandLineOptions()
        }

        if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
        
        # login to hublot
        clessnverse::logit(scriptname, "connecting to hub", logger)

        credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))
        
        
        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

        status <<- 0
        total_press_releases_scraped <<- 0
        
        main(scriptname, logger, credentials)
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
        clessnverse::logit(scriptname, paste(total_press_releases_scraped, "QC press releases were extracted from the web"), logger)
        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
        clessnverse::log_close(logger)
        rm(logger)
        print(paste("exiting with status", status))
        quit(status = status)
    }
)


