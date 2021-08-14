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
safe_RCurl_getURL <- purrr::safely(RCurl::getURL)
safe_httr_GET <- purrr::safely(httr::GET)



###############################################################################
# Main
#
#
#
#plc
#pcc
#blq
#grn
#npd
#ppc

clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

plc_url <- "https://liberal.ca/fr/category/communiques/"
pcc_url <- "https://www.conservateur.ca/nouvelles/"
blq_url <- "https://www.blocquebecois.org/tous-les-communiques/"
grn_url <- "https://www.greenparty.ca/fr/nouvelles/communiqués-de-presse"
npd_url <- "https://www.npd.ca/nouvelles"
ppc_url <- "https://www.partipopulaireducanada.ca/nouvelles_archives"


plc_r <- safe_httr_GET(plc_url)
pcc_r <- safe_httr_GET(pcc_url)
blq_r <- safe_httr_GET(blq_url)
grn_r <- safe_httr_GET(grn_url)
npd_r <- safe_httr_GET(npd_url)
ppc_r <- safe_httr_GET(ppc_url)

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
      
      plc_comm_location <- XML::xmlValue(XML::getNodeSet(plc_comm_text[[2]], "em"))
      
      text <- ""
      
      for (i_plc_comm_text in 2:length(plc_comm_text)) {
        text <- paste(text, XML::xmlValue(plc_comm_text[[i_plc_comm_text]]), sep="\n\n")
      }
      
      plc_comm_text <- text
      
      # Construit le data pour le hub
      key <- digest::digest(plc_url_list[[i_plc_url_list]])
      type <- "plc"
      schema <- "v1"
      
      metadata <- list("date"=Sys.time(), "url"=plc_url_list[[i_plc_url_list]])
      data <- list("date"=plc_comm_date, "place"=plc_comm_location, "content"=plc_comm_text)
      
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



if (pcc_r$result$status_code == 200) {
  
}



if (blq_r$result$status_code == 200) {
  
}



if (grn_r$result$status_code == 200) {
  
}



if (npd_r$result$status_code == 200) {
  
}



if (ppc_r$result$status_code == 200) {
  
}
