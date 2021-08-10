###############################################################################
# This script scrapes the canadian liberal MPs on their website
# https://liberal.ca/fr/vos-candidats-liberaux/
# and populates the Persons table in the CLESSN HUB 2.0
#


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
rm_accent <- function(str,pattern="all") {
  if(!is.character(str))
    str <- as.character(str)
  
  pattern <- unique(pattern)
  
  if(any(pattern=="Ç"))
    pattern[pattern=="Ç"] <- "ç"
  
  symbols <- c(
    acute = "áéíóúÁÉÍÓÚýÝ",
    grave = "àèìòùÀÈÌÒÙ",
    circunflex = "âêîôûÂÊÎÔÛ",
    tilde = "ãõÃÕñÑ",
    umlaut = "äëïöüÄËÏÖÜÿ",
    cedil = "çÇ"
  )
  
  nudeSymbols <- c(
    acute = "aeiouAEIOUyY",
    grave = "aeiouAEIOU",
    circunflex = "aeiouAEIOU",
    tilde = "aoAOnN",
    umlaut = "aeiouAEIOUy",
    cedil = "cC"
  )
  
  accentTypes <- c("´","`","^","~","¨","ç")
  
  if(any(c("all","al","a","todos","t","to","tod","todo")%in%pattern)) 
    return(chartr(paste(symbols, collapse=""), paste(nudeSymbols, collapse=""), str))
  
  for(i in which(accentTypes%in%pattern))
    str <- chartr(symbols[i],nudeSymbols[i], str) 
  
  return(str)
}




###############################################################################
# Main
#
#
#
safe_RCurl_getURL <- purrr::safely(RCurl::getURL)
safe_httr_GET <- purrr::safely(httr::GET)

base_url <- "https://www.noscommunes.ca"
index_url <- "https://www.noscommunes.ca/Members/en/search/xml"
index_html_url <- "https://www.noscommunes.ca/Members/en/search"

liberal_url <- "https://liberal.ca/your-liberal-candidates/"

npd_url <- "https://www.ndp.ca/team"

bloc_url <- "http://www2.blocquebecois.org/deputes-et-deputees/page"

conserv_base_url <- "https://www.conservateur.ca/team-member/"
conserv_url <- "https://www.conservateur.ca/equipe/mps/"

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

# Get the list of all MPs from the house of commons web site
#r <- safe_httr_GET(index_url)
#mps_index_xml_raw <- httr::content(r$result, encoding = "UTF-8")
#mps_top_xml_node <- XML::xmlParse(mps_index_xml_raw, useInternalNodes = TRUE)
#mps_xml_nodes <- XML::xmlRoot(mps_top_xml_node)
r <- safe_httr_GET(index_html_url)
if (r$result$status_code == 200) {
  mp_index_html_parsed <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
  xml_root <- XML::xmlRoot(mp_index_html_parsed)
  #mps_xml_nodes_list <- XML::getNodeSet(xml_root, "/html//body/main/div/div[@id = 'tiles-view-component']/div[@id = 'mip-tile-view']/div/div/div/div[@class = 'ce-mip-mp-tile-container ']")
  mps_xml_nodes_list <- XML::getNodeSet(xml_root, ".//a[@class = 'ce-mip-mp-tile']")
} else {
  stop(paste("Unable ro retrieve MPs index page", index_html_url))
}

# Loop through every MP
for (i in 1:length(mps_xml_nodes_list)) {
  currentNode <- mps_xml_nodes_list[[i]]
  
  full_name      <- NA
  first_name     <- NA
  last_name      <- NA
  district       <- NA
  province       <- NA
  party          <- NA
  mp_url         <- NA
  mp_functions   <- NA
  id             <- NA
  key            <- NA
  is_female      <- NA
  is_minister    <- NA
  twitter_url    <- NA
  twitter_handle <- NA
  
  
  
  # Get info info from nocommunes.ca
  full_name  <- XML::xpathApply(currentNode, ".//div[@class='ce-mip-mp-name']", XML::xmlValue)
  full_name  <- full_name[[1]]
  first_name <- strsplit(full_name[[1]][1], " ", perl = T)[[1]][1] 
  last_name  <- strsplit(full_name[[1]][1], " ", perl = T)[[1]][sapply(strsplit(full_name[[1]], " "), length)]
  district   <- XML::xpathApply(currentNode, ".//div[@class='ce-mip-mp-constituency']", XML::xmlValue)[[1]]
  province   <- XML::xpathApply(currentNode, ".//div[@class='ce-mip-mp-province']", XML::xmlValue)[[1]]
  party      <- XML::xpathApply(currentNode, ".//div[@class='ce-mip-mp-party']", XML::xmlValue)[[1]]
  mp_url     <- XML::xmlGetAttr(currentNode, "href")
  mp_url     <- paste(base_url, mp_url, sep='')
  id         <- stringr::str_match(mp_url, "\\((.*)\\)")[2]
  

  is_female  <- gender::gender(stringi::stri_trans_general(strsplit(first_name, "\\s|\\-")[[1]][1], "Latin-ASCII"))$gender == "female"
  if ( length(is_female) == 0 ) { is_female <- FALSE }
  if (is_female) is_female <- 1 else is_female <- 0
  
  
  # Get current MP's functions from ourcommons.ca
  functions <- NA
  r <- safe_httr_GET(mp_url)
  if (r$result$status_code == 200) {
    mp_ncfile_html_parsed <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
    html_root <- XML::xmlRoot(mp_ncfile_html_parsed)
    functions_node <- XML::getNodeSet(html_root, ".//ul[@class = 'ce-mip-roles-list']")
    for (i_functions in 1:length(functions_node)){
      #branch <- dplyr::case_when(i_functions == 1 ~ ".//li",
      #                           TRUE ~ ".//span")
      branch <- dplyr::case_when(#length(names(functions_node[[i_functions]])) == 2 ~ ".//li",
                                 #length(names(functions_node[[i_functions]])) > 2 ~ ".//span",
                                 length(XML::getNodeSet(functions_node[[i_functions]], ".//a")) > 0 ~ ".//span",
                                 TRUE ~ ".//li"
                                )
      
      
      if (is.na(functions)) {
        functions <- paste(XML::xpathApply(functions_node[[i_functions]], branch, XML::xmlValue), collapse = " | ")
      } else{
        functions <- paste(functions, paste(XML::xpathApply(functions_node[[i_functions]], branch, XML::xmlValue), collapse = " | "), sep=" | ")
      }
    }
  } else {
    print(paste("MP not found:", paste(first_name, last_name), "at url", mp_url))
  }
  
  is_minister <- grepl("^Minister|\\| Minister|Prime Minister",  functions)
  if (is_minister) is_minister <- 1 else is_minister <- 0
  
  
  # Get info from party web site
  if (party == "Conservative") {
    
    ######  TODO : enlever les accents pour construire l'URL
    r <- safe_httr_GET(paste(conserv_base_url, tolower(gsub("\\'","",rm_accent(first_name))), "-", tolower(gsub("\\'","",rm_accent(last_name))), "/", sep = ''))
    
    if (r$result$status_code == 200) {
      mp_file_html_parsed <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      html_root <- XML::xmlRoot(mp_file_html_parsed)
      social <- XML::getNodeSet(html_root, "/html//body/main/section[@class = 'section section--social-share']")
      twitter_url <- sapply(social, XML::xpathSApply, "./div//li/a[@data-type='twitter']", XML::xmlAttrs)
      if (length(twitter_url[[1]]) > 0) {
        twitter_url <- twitter_url[2,1]
        twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
      } else {
        print(paste("no twitter account for MP", paste(first_name, last_name), "at url", mp_url))
      }
    } else {
      print(paste("MP not found:", paste(first_name, last_name), "at url", paste(conserv_base_url, tolower(first_name), "-", tolower(last_name), sep = '')))
    }
  }
  
  if (party == "Liberal") {
    r <- safe_httr_GET(liberal_url)
    
    if (r$result$status_code == 200) {
      mp_file_html_parsed <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      html_root <- XML::xmlRoot(mp_file_html_parsed)
      lib_mps_xml_node_list  <- XML::getNodeSet(html_root, ".//div[@class = 'person__item']")
      lib_mps_name_list <- sapply(lib_mps_xml_node_list, XML::xpathSApply, ".//h2[@class = 'person__name']", XML::xmlValue)
      lib_mp_index <- grep(full_name, lib_mps_name_list)
      
      if (length(lib_mp_index) > 0 ) {
        social <- XML::getNodeSet(html_root, ".//div[@class = 'person__social-link-container']")
        twitter_url <- XML::xpathApply(social[[lib_mp_index]], ".//a[@class='person__social-link person__social-link--twitter']", XML::xmlAttrs)
        if (length(twitter_url) > 0) { 
          twitter_url <- twitter_url[[1]][1]
          twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url)) 
        } else {
          print(paste("no twitter account for MP", paste(first_name, last_name), "at url", liberal_url))
        }
      } else {
        print(paste("MP not found:", paste(first_name, last_name), "at url", liberal_url))
      }
    } else {
      print(paste("URL not found for MP", paste(first_name, last_name), "at url", liberal_url))
    }
  }
  
  if (party == "NDP") {
    r <- safe_httr_GET(npd_url)
    
    if (r$result$status_code == 200) {
      mp_file_html_parsed <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      html_root <- XML::xmlRoot(mp_file_html_parsed)
      npd_mps_xml_node_list  <- XML::getNodeSet(html_root, ".//div[@class = 'campaign-civics-list-inner']")
      npd_mps_name_list <- sapply(npd_mps_xml_node_list, XML::xpathSApply, ".//div[@class = 'campaign-civics-list-title civic-name']", XML::xmlValue)
      npd_mp_index <- grep(full_name, npd_mps_name_list)
      
      if (length(npd_mp_index) > 0) {
        social <- XML::getNodeSet(html_root, ".//div[@class = 'civic-data']")
        attrs <- XML::xmlAttrs(social[[npd_mp_index]])
        twitter_index <- which(names(attrs) == "data-twitter-link")
        twitter_url <- attrs[twitter_index]
        if (length(twitter_url) > 0) { 
          twitter_url <- twitter_url[[1]][1]
          twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url)) 
        } else {
          print(paste("no twitter account for MP", paste(first_name, last_name), "at url", npd_url))
        }
      } else {
        print(paste("MP not found:", paste(first_name, last_name), "at url", npd_url))
      }
    } else {
      print(paste("URL not found for MP", paste(first_name, last_name), "at url", npd_url))
    }
  }
  
  if (party == "Bloc Québécois") {
    
    page_num <- 1
    r <- safe_httr_GET(paste(bloc_url, page_num, sep='/'))
    mp_file_html_parsed <- httr::content(r$result, as = 'text')
    
    while(!grepl(last_name, mp_file_html_parsed)) {
      page_num <- page_num + 1
      r <- safe_httr_GET(paste(bloc_url, page_num, sep='/'))
      mp_file_html_parsed <- httr::content(r$result, as = 'text')
    }

    if (r$result$status_code == 200) {
      mp_file_html_parsed <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      html_root <- XML::xmlRoot(mp_file_html_parsed)
      bloc_mps_xml_node_list  <- XML::getNodeSet(html_root, ".//h2")
      bloc_mps_name_list <- gsub("\t|\n|\\s", "", XML::xmlValue(bloc_mps_xml_node_list))#sapply(bloc_mps_xml_node_list, XML::xpathSApply, ".//h2", XML::xmlValue)
      bloc_mp_index <- grep(paste(first_name,last_name,sep=''), bloc_mps_name_list)
      bloc_mps_xml_node_list  <- XML::getNodeSet(html_root, ".//div[@class='infos']")
      

      if (length(bloc_mp_index) > 0) {
        social <- XML::getNodeSet(bloc_mps_xml_node_list[[bloc_mp_index]], ".//a[@class='fb']")
        attrs <- sapply(social, XML::xpathSApply, ".//i[@class = 'fa fa-twitter-square']", XML::xmlAttrs)
        twitter_index <- grep("fa fa-twitter-square", attrs)
        if (length(twitter_index) > 0) {
          twitter_url <- XML::xmlAttrs(social[[twitter_index]])
          twitter_url <- twitter_url[which(names(twitter_url) == "href")]
          if (length(twitter_url) > 0) { 
            twitter_url <- twitter_url[[1]][1]
            twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
            if (grepl("\\?", twitter_handle)) {
              twitter_handle <- substr(twitter_handle, 1,sapply(regexpr("\\?", twitter_handle, perl=TRUE), tail, 1)-1)
            }
          } else {
            print(paste("no twitter account for MP", paste(first_name, last_name), "at url", bloc_url))
          }
        } else {
          print(paste("no twitter account for MP", paste(first_name, last_name), "at url", bloc_url))        
        }
      } else {
        print(paste("no twitter account for MP", paste(first_name, last_name), "at url", bloc_url))
      }
    } else {
      print(paste("URL not found for MP", paste(first_name, last_name), "at url", bloc_url))
    }
  }
  
  # normalize data
  
  
  
  
  
  
  
  # insert or update the info in the hub 2.0
  data_to_commit <- list(firstName = first_name, 
                         lastName = last_name,
                         fullName = full_name, 
                         isFemale = is_female,
                         currentFunctionsList = functions, 
                         currentParty = party,
                         currentDistrict = district, 
                         currentProvinceOrState = province,
                         isMinister = is_minister, 
                         twitterHandle = twitter_handle, 
                         twitterID = NA, 
                         twitterAccountProtected = NA)
  
  metadata_to_commit <- list(source = "scrapeCanadaMPs.R", country = "CA", province_or_state = province)
  
  key <- id
  type <- "mp"
  schema <- "v2"
  
  test <- NULL
  test <- clessnhub::get_items('persons', filter = list(key = key))
  
  if (!is.null(test) > 0) {
    clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit) 
    print(paste("updating existing item in hub with twitter handle", twitter_handle, "and key", key, "for", full_name))
  } else {
    clessnhub::create_item('persons', key = key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
    print(paste("creating new items in hub with twitter handle", twitter_handle, "and key", key, "for", full_name))
  }

}