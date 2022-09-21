###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        all-persons-tweets-collector                         #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################
# This script scrapes the canadian political parties press releases           #
# from their respective web sites                                             #
# and populates the agoraplus_press_preleases table in the CLESSN HUB 2.0     #
###############################################################################
# Data Structure                                                              #
# key = générée à partir de l'URL                                             #
# type = parti politique                                                      #
# schema = v1                                                                 #
#                                                                             #
# metadata.url = URL du communiqué                                            #
# metadata.date_scraping = date du moment où on l'a scrapé                    #
#                                                                             #
# data.date = date du communiqué                                              #
# data.location = lieu du communiqué                                          #
# data.content = contenu du communiqué                                        #
###############################################################################

###############################################################################
########################      Functions and Globals      ######################
###############################################################################




###############################################################################
# Package required
# XML
# httr
# purrr
#
#


###############################################################################
# Functions
###############################################################################

safe_httr_GET <- purrr::safely(httr::GET)



###############################################################################
# installPackages
###############################################################################
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
                         "textcat",
                         "tidytext", 
                         "tibble",
                         "devtools",
                         "purrr",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r")
  
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
# LPC
###############################################################################
extractLPCUrlsList <- function(xml_root, scriptname, logger) {
  link_nodes <- XML::getNodeSet(xml_root, ".//a[@class = 'post-listing-item__link']")
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  urls_list <- list()
  
  for (i in 1:length(link_nodes)) {
    urls_list <- c(urls_list, XML::xmlGetAttr(link_nodes[[i]], "href"))
  }
  
  return(urls_list)
}

extractLPCInfo <- function(comm_root, scriptname, logger) {
  # Récupère le titre
  title <- XML::getNodeSet(comm_root, ".//title")
  title <- XML::xmlValue(title)
  
  # Récupère la date
  date <- XML::getNodeSet(comm_root, ".//p[@class='single__date']")
  date <- XML::xmlValue(date)
  date <- gsub("\t|\n", "", date)
  
  # Récupère le contenu
  text_node <- XML::getNodeSet(comm_root, ".//div[@class='post-content-container']")
  text_node <- XML::getNodeSet(text_node[[1]], ".//p")
  
  content <- ""
  
  for (i in 2:length(text_node)) {
    content <- paste(content, XML::xmlValue(text_node[[i]]), sep="\n\n")
  }
  
  content <- trimws(content)
  content <- gsub("\u00a0", "", content)
  
  
  # Récupère le lieu
  location <- substr(content, 1, stringr::str_locate(content, "\\s-|—|–\\s")[1,1][[1]]-1)
  location <- trimws(location)
  location <- stringr::str_to_title(location)
  if (!is.na(location) && nchar(location) > 32) location <- NA
  
  hub_object <- list()
  
  hub_object$data$date <- date
  hub_object$data$title <- title
  hub_object$data$content <- content
  hub_object$data$location <- location
  
  return(hub_object)
}


###############################################################################
# CPC
###############################################################################
extractCPCUrlsList <- function(xml_root, scriptname, logger) {
  link_nodes <- XML::getNodeSet(xml_root, ".//div[@class = 'grid-container grid-narrow']/a")
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  urls_list <- list()
  
  for (i in 1:length(link_nodes)) {
    urls_list <- c(urls_list, XML::xmlGetAttr(link_nodes[[i]], "href"))
  }
  
  return(urls_list)
}

extractCPCInfo <- function(comm_root, scriptname, logger) {
  # Récupère le titre
  title <- XML::getNodeSet(comm_root, ".//h1[@class='title-header']")
  title <- XML::xmlValue(title)
  
  # Récupère le contenu
  text_node <- XML::getNodeSet(comm_root, ".//div[@class='post-content']")
  text_node <- XML::getNodeSet(text_node[[1]], ".//p")
  
  # First figure out if first line is a date or the press release itself
  first_line <- XML::xmlValue(text_node[[1]])
  if (grepl("janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre", tolower(substr(first_line, 1, 20)))) {
    start_index <- 2
  } else {
    start_index <- 1
  }
  
  location_sep <- stringr::str_locate(XML::xmlValue(text_node[[start_index]]), "–")
  location <- substr(XML::xmlValue(text_node[[start_index]]), 1, location_sep[1,1][[1]]-2)
  
  content <- ""
  
  for (i in start_index:length(text_node)) {
    content <- paste(content, XML::xmlValue(text_node[[i]]), sep = "\n\n")
  }
  
  content <- trimws(content)
  content <- gsub("\u00a0", "", content)
  
  hub_object <- list()
  
  #hub_object$data$date <- date
  hub_object$data$title <- title
  hub_object$data$content <- content
  hub_object$data$location <- location
  
  return(hub_object)
  
}


###############################################################################
# NDP
###############################################################################
extractNDPUrlsList <- function(xml_root, scriptname, logger) {
  link_nodes <- XML::getNodeSet(xml_root, ".//article/a")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  urls_list <- list()
  
  for (i in 1:length(link_nodes)) {
    urls_list <- c(urls_list, XML::xmlGetAttr(link_nodes[[i]], "href"))
  }
  
  return(urls_list)
}

extractNDPInfo <- function(comm_root, scriptname, logger) {
  # Récupère le titre
  title <- XML::getNodeSet(comm_root, ".//div[@class='news2-holder news2-body article-text']")
  title <- XML::xpathApply(title[[1]], ".//h1")
  title <- XML::xmlValue(title)
  
  # Récupère le contenu
  text_node <- XML::getNodeSet(comm_root, ".//div[@class='news2-holder news2-body article-text']")
  
  text_node <- XML::xpathApply(text_node[[1]], ".//p")
  
  if (length(text_node) > 2) {
    content <- paste(XML::xmlValue(text_node), collapse = '\n\n')
  } else {
    text_node <- XML::getNodeSet(comm_root, ".//div[@class='news2-holder news2-body article-text']")
    content <- XML::xmlValue(text_node)
    content <- trimws(content)
    content <- gsub("\t\t\t\t", "\n\n",content)
    content <- gsub("\n\n\t", "\n\n",content)
    content <- gsub("\n\t\t", "\n\n",content)
  }
  
  # Récupère l'endroit
  location <- substr(content, 1, stringr::str_locate(content, "-|—|–")[1,1][[1]]-1)
  if (!is.na(location)) {
    location <- trimws(location)
    location <- stringr::str_to_title(location)
    if (nchar(location) > 25) location <- NA
  }
  
  # Récupère la date
  date <- XML::getNodeSet(comm_root, ".//div[@class='news2-holder news2-date article-text']")[[1]]
  date <- trimws(XML::xmlValue(date))
  
  hub_object <- list()
  
  hub_object$data$date <- date
  hub_object$data$title <- title
  hub_object$data$content <- content
  hub_object$data$location <- location
  
  return(hub_object)
}


###############################################################################
# GPC
###############################################################################
extractGPCUrlsList <- function(xml_root, scriptname, logger) {
  link_nodes <- XML::getNodeSet(xml_root, ".//h3[@class='media-heading']")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  urls_list <- list()
  
  for (i in 1:length(link_nodes)) {
    urls_list <- c(urls_list, XML::xmlAttrs(XML::getNodeSet(link_nodes[[i]], ".//a")[[1]])[[1]])
  }
  
  urls_list <- paste("https://www.greenparty.ca", urls_list, sep='')
  
  return(urls_list)
}

extractGPCInfo <- function(comm_root, scriptname, logger) {
  # Récupère le titre
  title <- XML::getNodeSet(comm_root, ".//h1[@class='page-header title-container text-left visible']")
  title <- XML::xmlValue(title[[1]])
  
  # Récupère le contenu
  text <- XML::getNodeSet(comm_root, ".//div[@class='field-body']")
  
  # Récupère la date
  date <- XML::getNodeSet(comm_root, ".//div[@class='pane-content']")[[1]]
  date <- trimws(XML::xmlValue(date))
  
  # Récupère le texte
  text_node <- XML::getNodeSet(text[[1]], ".//p")
  content <- paste(XML::xmlValue(text_node), collapse = '\n\n')
  content <- trimws(content)
  #text <- gsub("\u00a0", "", text)
  
  
  location <- substr(content, 1, stringr::str_locate(content, "-|—")[1,1][[1]]-1)
  location <- trimws(location)
  location <- stringr::str_to_title(location)
  
  hub_object <- list()
  
  hub_object$data$date <- date
  hub_object$data$title <- title
  hub_object$data$content <- content
  hub_object$data$location <- location
  
  return(hub_object)
}


###############################################################################
# BQ
###############################################################################
extractBQUrlsList <- function(xml_root, scriptname, logger) {
  link_nodes <- XML::getNodeSet(xml_root, ".//article")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  urls_list <- list()
  
  for (i in 1:length(link_nodes)) {
    urls_list <- c(urls_list, XML::xmlGetAttr(XML::getNodeSet(link_nodes[[i]], ".//a[@class='more']")[[1]], "href"))
  }
  
  return(urls_list)
}

extractBQInfo <- function(comm_root, scriptname, logger) {
  # Récupère le titre
  title <- XML::getNodeSet(comm_root, ".//header/h1")
  title <- XML::xmlValue(title[[1]])
  
  # Récupère le contenu
  text_node <- XML::getNodeSet(comm_root, ".//div[@class='content']")
  
  # Récupère la date
  date <- XML::xmlValue(text_node[[1]])
  date <- substr(date, 1, stringr::str_locate(date, "–")[1,1][[1]]-1)
  date <- trimws(date, which = "both")
  date <- gsub("lundi |mardi |mercredi |jeudi |vendredi |samedi |dimanche", "", date)
  
  location <- substr(date, 1, stringr::str_locate(date, ",")[1,1][[1]]-1)
  location <- gsub("\u00a0", "", location)
  if (grepl(", le", tolower(date))) kick <- 5 else kick <- 2
  date <- substr(date, stringr::str_locate(date, ",")[1,1][[1]]+kick, nchar(date))
  
  # Récupère le texte
  text_node <- XML::getNodeSet(text_node[[1]], ".//p")
  content <- paste(XML::xmlValue(text_node), collapse = '\n\n')
  content <- trimws(content)
  content <- gsub("\u00a0", "", content)
  
  hub_object <- list()
  
  hub_object$data$date <- date
  hub_object$data$title <- title
  hub_object$data$content <- content
  hub_object$data$location <- location
  
  return(hub_object)
}


###############################################################################
# PPC
###############################################################################
extractPPCUrlsList <- function(xml_root, scriptname, logger) {
  link_nodes <- XML::getNodeSet(xml_root, ".//div[@class='col-md-12']")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  urls_list <- list()
  
  for (i in 3:length(link_nodes)-2) {
    print(i)
    h3_node <- XML::getNodeSet(link_nodes[[i]], ".//h3")
    if (length(h3_node) > 0) {
      h3_node <- XML::getNodeSet(link_nodes[[i]], ".//h3")[[1]]
      urls_list <- c(urls_list, XML::xmlGetAttr(XML::getNodeSet(h3_node, ".//a")[[1]], "href"))
    }
  }
  
  return(urls_list)
}

extractPPCInfo <- function(comm_root, scriptname, logger) {
  # Récupère le titre
  title <- XML::getNodeSet(comm_root, ".//title")
  title <- XML::xmlValue(title[[1]])
  
  # Récupère le contenu
  text_node <- XML::getNodeSet(comm_root, ".//div[@class='col-md-8 platform news']")
  if (length(text_node) > 0) {
    content <- XML::xmlValue(text_node[[1]])
    content <- trimws(content)
  } else {
    content <- NA
  }
  
  # Récupère la date
  date_node <- XML::getNodeSet(comm_root, ".//strong")
  if (length(date_node) > 0) date_node <- date_node[[1]]
  date <- trimws(XML::xmlValue(date_node))
  if (length(date) > 0) {
    date <- gsub(" –","",date)
    date <- strsplit(date, ",")[[1]][3]
    date <- trimws(date)
  } else {
    date <- NA
  }
    
  
  # Récupère le lieu
  location_node <- XML::getNodeSet(comm_root, ".//strong")
  if (length(location_node) > 0) location_node <- location_node[[1]]
  location <- trimws(XML::xmlValue(location_node))
  if (length(location) > 0) {
    location <- gsub(" –","",location)
    location <- strsplit(location, ",")[[1]][2]
    location <- trimws(location)
  }
  
  
  hub_object <- list()
  
  hub_object$data$date <- date
  hub_object$data$title <- title
  hub_object$data$content <- content
  hub_object$data$location <- location
  
  return(hub_object)
}




###############################################################################
# scrapePartyPressRelease
###############################################################################
scrapePartyPressRelease <- function(party, party_url, scriptname, logger) {
  
  clessnverse::logit(scriptname, paste("scraping", party, "main press release page", party_url), logger)
  r <- safe_httr_GET(party_url)
  
  if (r$result$status_code == 200) {
    # On extrait les section <a> qui contiennent les liens vers chaque communiqué
    #clessnverse::logit(scriptname, paste("successful GET of", party, "main press release page", party_url), logger)
    
    index_html <- httr::content(r$result, as="text")
    
    index_xml <- XML::htmlTreeParse(index_html, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
    xml_root <- XML::xmlRoot(index_xml)
    
    #clessnverse::logit(scriptname, paste("Trying to extract", party,"press releases URLs from main press release page"), logger)
    
    if (party == "LPC") urls_list <- extractLPCUrlsList(xml_root, scriptname, logger)
    
    if (party == "CPC") {
      date_list_node <- XML::getNodeSet(xml_root, ".//p[@class = 'post-date']")
      
      cpc_date_list <- list()
      
      for (i in 1:length(date_list_node)) {
        cpc_date_list <- c(cpc_date_list, XML::xmlValue(date_list_node[[i]]))
      }
      
      urls_list <- extractCPCUrlsList(xml_root, scriptname, logger)
    }
    
    if (party == "NDP") urls_list <- extractNDPUrlsList(xml_root, scriptname, logger)
    if (party == "GPC") urls_list <- extractGPCUrlsList(xml_root, scriptname, logger)
    if (party == "BQ")  urls_list <- extractBQUrlsList(xml_root, scriptname, logger)
    if (party == "PPC") urls_list <- extractPPCUrlsList(xml_root, scriptname, logger)
    
    if (length(urls_list) > 0) clessnverse::logit(scriptname, paste("found", length(urls_list), "press releases URLs from",party, "main press release page"), logger) else clessnverse::logit(scriptname, paste("No URL found in", party, "main press releases page"), logger)
    
    # On loop à travers toutes les URL de ls liste des communiqués
    # On les scrape et stocke sur le hub 2.0
    if (length(urls_list) > 0) {
      for (i in 1:length(urls_list)) {
        clessnverse::logit(scriptname, paste("scraping", party, "press release page", urls_list[[i]]), logger)
        
        r <- safe_httr_GET(urls_list[[i]])
        
        if (r$result$status_code == 200) {
          #clessnverse::logit(scriptname, paste ("successful GET on", party, "press release at URL", urls_list[[i]]), logger)
          comm_html <- httr::content(r$result, as="text")
          
          comm_xml <- XML::htmlTreeParse(comm_html, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
          comm_root <- XML::xmlRoot(comm_xml)
          
          hub_object <- NULL
          
          if (party == "LPC") hub_object <- extractLPCInfo(comm_root, scriptname, logger)
          
          if (party == "CPC") {
            hub_object <- extractCPCInfo(comm_root, scriptname, logger)
            hub_object$data$date <- cpc_date_list[[i]]
          }
          
          if (party == "NDP") hub_object <- extractNDPInfo(comm_root, scriptname, logger)
          if (party == "GPC") hub_object <- extractGPCInfo(comm_root, scriptname, logger)
          if (party == "BQ")  hub_object <- extractBQInfo(comm_root, scriptname, logger)
          if (party == "PPC") hub_object <- extractPPCInfo(comm_root, scriptname, logger)
          
          if (!is.null(hub_object)) {
  
            # Construit le data pour le hub
            key <- digest::digest(urls_list[[i]])
            type <- party
            schema <- "v1"
            
            hub_object$metadata$url <- urls_list[[i]]
            hub_object$metadata$lastUpdatedOn <- Sys.time()
            
            hub_items <- NULL
            filter <- clessnhub::create_filter(key = key)
            hub_items <- clessnhub::get_items('agoraplus_press_releases', filter = filter)
            
            if (is.null(hub_items)) {
              # ce communiqué (avec cette key) n'existe pas dans le hub
              #clessnverse::logit(scriptname, paste("creating new item", key, "in table agoraplus_press_releases"), logger)
              clessnhub::create_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = hub_object$metadata, data = hub_object$data)
            } else {
              # ce communiqué (avec cette key) existe dans le hub 
              #clessnverse::logit(scriptname, paste("updating existing item", key, "in table agoraplus_press_releases"), logger)
              clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema,metadata = hub_object$metadata, data = hub_object$data)
            }
          } else {
            clessnverse::logit(scriptname, paste("no press release from", party, "at", urls_list[[i]]), logger)
          } #if (!is.null(hub_object)) 
          
        } else {
          clessnverse::logit(scriptname, paste("error accessing", party, "press release page", urls_list[[i]]), logger)
      } #for (i in 1:length(urls_list))
        }
      clessnverse::logit(scriptname, paste(i, "press releases were scraped from the", party, "web site"), logger)
      cat(i, "press releases were scraped from the", party, "web site", "\n")
    }#if (length(urls_list) > 0)
  } else {
    clessnverse::logit(scriptname, "Error getting", party, "main press release page", logger)
  }
} #</function scrapePartyPressRelease>





###############################################################################
# Main
#
#
#
main <- function(scriptname, logger) {
  
  urls_list <- list(c("LPC","https://liberal.ca/fr/category/communiques/"),
                    c("CPC","https://www.conservateur.ca/nouvelles/"),
                    c("BQ","https://www.blocquebecois.org/nouvelles/"),
                    c("GPC","https://www.greenparty.ca/fr/nouvelles/communiqués-de-presse"),
                    c("NDP","https://www.npd.ca/nouvelles"),
                    c("PPC","https://www.partipopulaireducanada.ca/nouvelles_archives"))
  
  #clessnverse::logit(scriptname, paste("Scraping the following political parties press releases", paste(urls_list, collapse = ' ')), logger)
  
  for (i in 1:length(urls_list)) {
    #cat(urls_list[[i]][1], urls_list[[i]][2], "\n")
    scrapePartyPressRelease(urls_list[[i]][1], urls_list[[i]][2], scriptname, logger)
  }
  
}



tryCatch( 
  {
    #installPackages()
    library(dplyr)

    if (!exists("scriptname")) scriptname <<- "pressreleases_plow_canadafederalparties"

    # Script command line options:
    # Possible values : update, refresh, rebuild or skip
    # - update : updates the dataframe by adding only new observations to it
    # - refresh : refreshes existing observations and adds new observations to the dataframe
    # - rebuild : wipes out completely the dataframe and rebuilds it from scratch
    # - skip : does not make any change to the dataframe
    #opt <- list(dataframe_mode = "refresh", hub_mode = "refresh", log_output = "file,console,hub", download_data = TRUE)

    if (!exists("opt")) {
      opt <- clessnverse::processCommandLineOptions()
    }
    
    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    # login to the hub
    clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    clessnverse::logit(scriptname, "connecting to hub", logger)
    
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)
    
    main(scriptname, logger)
  },
  
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
    print(e)
  },
  
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::logclose(logger)
    rm(logger)
  }
)

