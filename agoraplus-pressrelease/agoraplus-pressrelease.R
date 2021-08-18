###############################################################################
# This script scrapes the canadian political parties press releases
# from their respective web sites
# and populates the agoraplus_press_preleases table in the CLESSN HUB 2.0
#

# Data Structure
# key = générée à partir de l'URL
# type = parti politique
# schema = v1
# 
# metadata.url = URL du communiqué
# metadata.date_scraping = date du moment où on l'a scrapé
#
# data.date = date du communiqué
# data.location = lieu du communiqué
# data.content = contenu du communiqué
#


###############################################################################
# Package required
# XML
# httr
# purrr
#
#


###############################################################################
# Functions
#
#
#
safe_httr_GET <- purrr::safely(httr::GET)



###############################################################################
# Main
#
#
#


clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
#clessnhub::connect()

plc_url <- "https://liberal.ca/fr/category/communiques/"
pcc_url <- "https://www.conservateur.ca/nouvelles/"
#blq_url <- "https://www.blocquebecois.org/tous-les-communiques/"
blq_url <- "https://www.blocquebecois.org/nouvelles/" 
grn_url <- "https://www.greenparty.ca/fr/nouvelles/communiqués-de-presse"
npd_url <- "https://www.npd.ca/nouvelles"
ppc_url <- "https://www.partipopulaireducanada.ca/nouvelles_archives"




###############################################################################
# PLC
plc_r <- safe_httr_GET(plc_url)

if (plc_r$result$status_code == 200) {
  # On extrait les section <a> qui contiennent les liens vers chaque communiqué
  plc_index <- httr::content(plc_r$result, as="text")
  
  plc_index_xml <- XML::htmlTreeParse(plc_index, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
  plc_xml_root <- XML::xmlRoot(plc_index_xml)
  
  plc_links_list <- XML::getNodeSet(plc_xml_root, ".//a[@class = 'post-listing-item__link']")
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  plc_url_list <- list()
  
  for (i_plc_links_list in 1:length(plc_links_list)) {
    plc_url_list <- c(plc_url_list, XML::xmlGetAttr(plc_links_list[[i_plc_links_list]], "href"))
  }
  
  # On loop à travers toutes les URL de ls liste des communiqués
  # On les scrape et stocke sur le hub 2.0
  for (i_plc_url_list in 1:length(plc_url_list)) {
    r <- safe_httr_GET(plc_url_list[[i_plc_url_list]])
    
    if (r$result$status_code == 200) {
      plc_comm <- httr::content(r$result, as="text")
      
      plc_comm_xml <- XML::htmlTreeParse(plc_comm, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      plc_comm_root <- XML::xmlRoot(plc_comm_xml)
      
      # Récupère le titre
      plc_comm_title <- XML::getNodeSet(plc_comm_root, ".//title")
      plc_comm_title <- XML::xmlValue(plc_comm_title)
      
      # Récupère la date
      plc_comm_date <- XML::getNodeSet(plc_comm_root, ".//p[@class='single__date']")
      plc_comm_date <- XML::xmlValue(plc_comm_date)
      plc_comm_date <- gsub("\t|\n", "", plc_comm_date)
      
      # Récupère le contenu
      plc_comm_text <- XML::getNodeSet(plc_comm_root, ".//div[@class='post-content-container']")
      plc_comm_text <- XML::getNodeSet(plc_comm_text[[1]], ".//p")
      
      text <- ""
      
      for (i_plc_comm_text in 2:length(plc_comm_text)) {
        text <- paste(text, XML::xmlValue(plc_comm_text[[i_plc_comm_text]]), sep="\n\n")
      }
      
      plc_comm_text <- trimws(text)
      plc_comm_text <- gsub("\u00a0", "", plc_comm_text)
      
      
      # Récupère le lieu
      plc_comm_location <- substr(plc_comm_text, 1, stringr::str_locate(plc_comm_text, "-|—|–")[1,1][[1]]-1)
      plc_comm_location <- trimws(plc_comm_location)
      plc_comm_location <- stringr::str_to_title(plc_comm_location)
      if (nchar(plc_comm_location) > 25) plc_comm_location <- NA
      
      # Construit le data pour le hub
      key <- digest::digest(plc_url_list[[i_plc_url_list]])
      type <- "LPC"
      schema <- "v1"
      
      metadata <- list("date"=Sys.time(), "url"=plc_url_list[[i_plc_url_list]])
      data <- list("date"=plc_comm_date, "location"=plc_comm_location, "title"=plc_comm_title, "content"=plc_comm_text)
      
      hub_items <- NULL
      filter <- clessnhub::create_filter(key = key)
      hub_items <- clessnhub::get_items('agoraplus_press_releases', filter = filter)
      
      if (is.null(hub_items)) {
        # ce communiqué (avec cette key) n'existe pas dans le hub
        clessnhub::create_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
      } else {
        # ce communiqué (avec cette key) existe dans le hub 
        clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
        print("ce communiqué existe déjà")
      }
      
    } else {
      stop(paste("Erreur pour accéder à la page du communiqué", plc_url_list[[i_plc_url_list]], sep=": "))
    }
  } #for (i in 1:length(plc_url_list))
  
} else {
  stop("Erreur pour accéder à la page des communiqués du PLC")
}

###############################################################################
# PCC
pcc_r <- safe_httr_GET(pcc_url)

if (pcc_r$result$status_code == 200) {
  # On extrait les sections qui contiennent les liens vers chaque communiqué
  pcc_index <- httr::content(pcc_r$result, as="text")
  
  pcc_index_xml <- XML::htmlTreeParse(pcc_index, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
  pcc_xml_root <- XML::xmlRoot(pcc_index_xml)
  
  pcc_links_list <- XML::getNodeSet(pcc_xml_root, ".//div[@class = 'grid-container grid-narrow']/a")

  # On construit une liste avec les URL des communiqués de presse de la page principale
  pcc_url_list <- list()
  
  for (i_pcc_links_list in 1:length(pcc_links_list)) {
    pcc_url_list <- c(pcc_url_list, XML::xmlGetAttr(pcc_links_list[[i_pcc_links_list]], "href"))
  }
  
  pcc_date_list <- XML::getNodeSet(pcc_xml_root, ".//p[@class = 'post-date']")
  
  date_list <- list()
  
  for (i_pcc_date_list in 1:length(pcc_date_list)) {
    date_list <- c(date_list, XML::xmlValue(pcc_date_list[[i_pcc_date_list]]))
  }
  
  pcc_date_list <- date_list
  
  
  # On loop à travers toutes les URL de ls liste des communiqués
  # On les scrape et stocke sur le hub 2.0
  for (i_pcc_url_list in 1:length(pcc_url_list)) {
    r <- safe_httr_GET(pcc_url_list[[i_pcc_url_list]])

    if (r$result$status_code == 200) {
      pcc_comm <- httr::content(r$result, as="text")
      
      pcc_comm_xml <- XML::htmlTreeParse(pcc_comm, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      pcc_comm_root <- XML::xmlRoot(pcc_comm_xml)

      # Récupère le titre
      pcc_comm_title <- XML::getNodeSet(pcc_comm_root, ".//h1[@class='title-header']")
      pcc_comm_title <- XML::xmlValue(pcc_comm_title)
      
      # Récupère la date
      pcc_comm_date <- pcc_date_list[[i_pcc_url_list]]

      # Récupère le contenu
      pcc_comm_text <- XML::getNodeSet(pcc_comm_root, ".//div[@class='post-content']")
      pcc_comm_text <- XML::getNodeSet(pcc_comm_text[[1]], ".//p")
      
      # First figure out if first line is a date or the press release itself
      first_line <- XML::xmlValue(pcc_comm_text[[1]])
      if (grepl("janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre", tolower(substr(first_line, 1, 20)))) {
        start_index <- 2
      } else {
        start_index <- 1
      }
      
      location_sep <- stringr::str_locate(XML::xmlValue(pcc_comm_text[[start_index]]), "–")
      pcc_comm_location <- substr(XML::xmlValue(pcc_comm_text[[start_index]]), 1, location_sep[1,1][[1]]-2)

      text <- ""
      
      for (i_pcc_comm_text in start_index:length(pcc_comm_text)) {
        text <- paste(text, XML::xmlValue(pcc_comm_text[[i_pcc_comm_text]]), sep = "\n\n")
      }
      
      pcc_comm_text <- trimws(text)
      pcc_comm_text <- gsub("\u00a0", "", pcc_comm_text)
      
      
      # Construit le data pour le hub
      key <- digest::digest(pcc_url_list[[i_pcc_url_list]])
      type <- "CPC"
      schema <- "v1"
      
      metadata <- list("date"=Sys.time(), "url"=pcc_url_list[[i_pcc_url_list]])
      data <- list("date"=pcc_comm_date, "Location"=pcc_comm_location, "title"=pcc_comm_title, "content"=pcc_comm_text)
      
      hub_items <- NULL
      filter <- clessnhub::create_filter(key = key)
      hub_items <- clessnhub::get_items('agoraplus_press_releases', filter = filter)
      
      if (is.null(hub_items)) {
        # ce communiqué (avec cette key) n'existe pas dans le hub
        clessnhub::create_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
      } else {
        # ce communiqué (avec cette key) existe dans le hub 
        clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
        print("ce communiqué existe déjà")
      }
      
    } else {
      stop(paste("Erreur pour accéder à la page du communiqué", pcc_url_list[[i_pcc_url_list]], sep=": "))
    }
  } #for (i in 1:length(plc_url_list))
  
} else {
  stop("Erreur pour accéder à la page des communiqués du PLC")
}     
  
  
###############################################################################
# BLOC
blq_r <- safe_httr_GET(blq_url)

if (blq_r$result$status_code == 200) {
  # On extrait les sections qui contiennent les liens vers chaque communiqué
  blq_index <- httr::content(blq_r$result, as="text")
  
  blq_index_xml <- XML::htmlTreeParse(blq_index, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
  blq_xml_root <- XML::xmlRoot(blq_index_xml)
  
  blq_links_list <- XML::getNodeSet(blq_xml_root, ".//article")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  blq_url_list <- list()
  
  for (i_blq_links_list in 1:length(blq_links_list)) {
    blq_url_list <- c(blq_url_list, XML::xmlGetAttr(XML::getNodeSet(blq_links_list[[i_blq_links_list]], ".//a[@class='more']")[[1]], "href"))
  }
  # On loop à travers toutes les URL de ls liste des communiqués
  # On les scrape et stocke sur le hub 2.0
  for (i_blq_url_list in 1:length(blq_url_list)) {
    r <- safe_httr_GET(blq_url_list[[i_blq_url_list]])
    
    if (r$result$status_code == 200) {
      blq_comm <- httr::content(r$result, as="text")
      blq_comm_xml <- XML::htmlTreeParse(blq_comm, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      blq_comm_root <- XML::xmlRoot(blq_comm_xml)
      
      # Récupère le titre
      blq_comm_title <- XML::getNodeSet(blq_comm_root, ".//header/h1")
      blq_comm_title <- XML::xmlValue(blq_comm_title[[1]])
      
     # Récupère le contenu
      blq_comm_text <- XML::getNodeSet(blq_comm_root, ".//div[@class='content']")

      # Récupère la date
      blq_comm_date <- XML::xmlValue(blq_comm_text[[1]])
      blq_comm_date <- substr(blq_comm_date, 1, stringr::str_locate(blq_comm_date, "–")[1,1][[1]]-1)
      blq_comm_date <- trimws(blq_comm_date, which = "both")
      blq_comm_date <- gsub("lundi |mardi |mercredi |jeudi |vendredi |samedi |dimanche", "", blq_comm_date)
      
      blq_comm_location <- substr(blq_comm_date, 1, stringr::str_locate(blq_comm_date, ",")[1,1][[1]]-1)
      blq_comm_location <- gsub("\u00a0", "", blq_comm_location)
      if (grepl(", le", tolower(blq_comm_date))) kick <- 5 else kick <- 3
      blq_comm_date <- substr(blq_comm_date, stringr::str_locate(blq_comm_date, ",")[1,1][[1]]+kick, nchar(blq_comm_date))
      
      # Récupère le texte
      blq_comm_text <- XML::getNodeSet(blq_comm_text[[1]], ".//p")
      blq_comm_text <- paste(XML::xmlValue(blq_comm_text), collapse = '\n\n')
      blq_comm_text <- trimws(blq_comm_text)
      blq_comm_text <- gsub("\u00a0", "", blq_comm_text)
      
      
      # Construit le data pour le hub
      key <- digest::digest(blq_url_list[[i_blq_url_list]])
      type <- "BQ"
      schema <- "v1"
      
      metadata <- list("date"=Sys.time(), "url"=blq_url_list[[i_blq_url_list]])
      data <- list("date"=blq_comm_date, "location"=blq_comm_location, "title"=blq_comm_title, "content"=blq_comm_text)
      
      hub_items <- NULL
      filter <- clessnhub::create_filter(key = key)
      hub_items <- clessnhub::get_items('agoraplus_press_releases', filter = filter)
      
      if (is.null(hub_items)) {
        # ce communiqué (avec cette key) n'existe pas dans le hub
        clessnhub::create_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
      } else {
        # ce communiqué (avec cette key) existe dans le hub 
        clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
        print("ce communiqué existe déjà")
      }
      
    } else {
      stop(paste("Erreur pour accéder à la page du communiqué", blq_url_list[[i_blq_url_list]], sep=": "))
    }
  }
}



###############################################################################
# GREEN
grn_r <- safe_httr_GET(grn_url)

if (grn_r$result$status_code == 200) {
  # On extrait les sections qui contiennent les liens vers chaque communiqué
  grn_index <- httr::content(grn_r$result, as="text")
  
  grn_index_xml <- XML::htmlTreeParse(grn_index, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
  grn_xml_root <- XML::xmlRoot(grn_index_xml)
  
  grn_links_list <- XML::getNodeSet(grn_xml_root, ".//h3[@class='media-heading']")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  grn_url_list <- list()
  
  for (i_grn_links_list in 1:length(grn_links_list)) {
    grn_url_list <- c(grn_url_list, XML::xmlAttrs(XML::getNodeSet(grn_links_list[[i_grn_links_list]], ".//a")[[1]])[[1]])
  }
  
  grn_url_list <- paste("https://www.greenparty.ca", grn_url_list, sep='')
  
  # On loop à travers toutes les URL de ls liste des communiqués
  # On les scrape et stocke sur le hub 2.0
  for (i_grn_url_list in 1:length(grn_url_list)) {
    r <- safe_httr_GET(grn_url_list[[i_grn_url_list]])
    
    if (r$result$status_code == 200) {
      grn_comm <- httr::content(r$result, as="text")
      grn_comm_xml <- XML::htmlTreeParse(grn_comm, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      grn_comm_root <- XML::xmlRoot(grn_comm_xml)
      
      # Récupère le titre
      grn_comm_title <- XML::getNodeSet(grn_comm_root, ".//h1[@class='page-header title-container text-left visible']")
      grn_comm_title <- XML::xmlValue(grn_comm_title[[1]])
      
      # Récupère le contenu
      grn_comm_text <- XML::getNodeSet(grn_comm_root, ".//div[@class='field-body']")
      
      # Récupère la date
      grn_comm_date <- XML::getNodeSet(grn_comm_root, ".//div[@class='pane-content']")[[1]]
      grn_comm_date <- trimws(XML::xmlValue(grn_comm_date))
      
      # Récupère le texte
      grn_comm_text <- XML::getNodeSet(grn_comm_text[[1]], ".//p")
      grn_comm_text <- paste(XML::xmlValue(grn_comm_text), collapse = '\n\n')
      grn_comm_text <- trimws(grn_comm_text)
      #grn_comm_text <- gsub("\u00a0", "", grn_comm_text)
      
      
      grn_comm_location <- substr(grn_comm_text, 1, stringr::str_locate(grn_comm_text, "-|—")[1,1][[1]]-1)
      grn_comm_location <- trimws(grn_comm_location)
      grn_comm_location <- stringr::str_to_title(grn_comm_location)
      
      
      # Construit le data pour le hub
      key <- digest::digest(grn_url_list[[i_grn_url_list]])
      type <- "GPC"
      schema <- "v1"
      
      metadata <- list("date"=Sys.time(), "url"=grn_url_list[[i_grn_url_list]])
      data <- list("date"=grn_comm_date, "location"=grn_comm_location, "title"= grn_comm_title, "content"=grn_comm_text)
      
      hub_items <- NULL
      filter <- clessnhub::create_filter(key = key)
      hub_items <- clessnhub::get_items('agoraplus_press_releases', filter = filter)
      
      if (is.null(hub_items)) {
        # ce communiqué (avec cette key) n'existe pas dans le hub
        clessnhub::create_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
      } else {
        # ce communiqué (avec cette key) existe dans le hub 
        clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
        print("ce communiqué existe déjà")
      }
      
    } else {
      stop(paste("Erreur pour accéder à la page du communiqué", grn_url_list[[i_grn_url_list]], sep=": "))
    }
  }
}


npd_r <- safe_httr_GET(npd_url)

if (npd_r$result$status_code == 200) {
  # On extrait les sections qui contiennent les liens vers chaque communiqué
  npd_index <- httr::content(npd_r$result, as="text")
  
  npd_index_xml <- XML::htmlTreeParse(npd_index, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
  npd_xml_root <- XML::xmlRoot(npd_index_xml)
  
  npd_links_list <- XML::getNodeSet(npd_xml_root, ".//article/a")
  
  
  # On construit une liste avec les URL des communiqués de presse de la page principale
  npd_url_list <- list()
  
  for (i_npd_links_list in 1:length(npd_links_list)) {
    npd_url_list <- c(npd_url_list, XML::xmlGetAttr(npd_links_list[[i_npd_links_list]], "href"))
  }
  
  # On loop à travers toutes les URL de ls liste des communiqués
  # On les scrape et stocke sur le hub 2.0
  for (i_npd_url_list in 1:length(npd_url_list)) {
    r <- safe_httr_GET(npd_url_list[[i_npd_url_list]])
    
    if (r$result$status_code == 200) {
      npd_comm <- httr::content(r$result, as="text")
      npd_comm_xml <- XML::htmlTreeParse(npd_comm, asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      npd_comm_root <- XML::xmlRoot(npd_comm_xml)
      
      # Récupère le titre
      npd_comm_title <- XML::getNodeSet(npd_comm_root, ".//div[@class='news2-holder news2-body article-text']")
      npd_comm_title <- XML::xpathApply(npd_comm_title[[1]], ".//h1")
      npd_comm_title <- XML::xmlValue(npd_comm_title)
      
      # Récupère le contenu
      npd_comm_text <- XML::getNodeSet(npd_comm_root, ".//div[@class='news2-holder news2-body article-text']")

      npd_comm_text <- XML::xpathApply(npd_comm_text[[1]], ".//p")
      
      if (length(npd_comm_text) > 2) {
        npd_comm_text <- paste(XML::xmlValue(npd_comm_text), collapse = '\n\n')
      } else {
        npd_comm_text <- XML::getNodeSet(npd_comm_root, ".//div[@class='news2-holder news2-body article-text']")
        npd_comm_text <- XML::xmlValue(npd_comm_text)
        npd_comm_text <- trimws(npd_comm_text)
        npd_comm_text <- gsub("\t\t\t\t", "\n\n",npd_comm_text)
        npd_comm_text <- gsub("\n\n\t", "\n\n",npd_comm_text)
        npd_comm_text <- gsub("\n\t\t", "\n\n",npd_comm_text)
      }
      
      # Récupère l'endroit
      npd_comm_location <- substr(npd_comm_text, 1, stringr::str_locate(npd_comm_text, "-|—|–")[1,1][[1]]-1)
      if (!is.na(npd_comm_location)) {
        npd_comm_location <- trimws(npd_comm_location)
        npd_comm_location <- stringr::str_to_title(npd_comm_location)
        if (nchar(npd_comm_location) > 25) npd_comm_location <- NA
      }
      
      # Récupère la date
      npd_comm_date <- XML::getNodeSet(npd_comm_root, ".//div[@class='news2-holder news2-date article-text']")[[1]]
      npd_comm_date <- trimws(XML::xmlValue(npd_comm_date))
      
      # Construit le data pour le hub
      key <- digest::digest(npd_url_list[[i_npd_url_list]])
      type <- "NPD"
      schema <- "v1"
      
      metadata <- list("date"=Sys.time(), "url"=npd_url_list[[i_npd_url_list]])
      data <- list("date"=npd_comm_date, "location"=npd_comm_location, "title"=npd_comm_title, "content"=npd_comm_text)
      
      hub_items <- NULL
      filter <- clessnhub::create_filter(key = key)
      hub_items <- clessnhub::get_items('agoraplus_press_releases', filter = filter)
      
      if (is.null(hub_items)) {
        # ce communiqué (avec cette key) n'existe pas dans le hub
        clessnhub::create_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
      } else {
        # ce communiqué (avec cette key) existe dans le hub 
        clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
        print("ce communiqué existe déjà")
      }
      
    } else {
      stop(paste("Erreur pour accéder à la page du communiqué", npd_url_list[[i_npd_url_list]], sep=": "))
    }
  }
}


ppc_r <- safe_httr_GET(ppc_url)

if (ppc_r$result$status_code == 200) {
  
}
