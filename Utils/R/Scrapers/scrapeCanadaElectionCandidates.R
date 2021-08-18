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

#######################################
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


#######################################
safe_httr_GET <- purrr::safely(httr::GET)


#######################################
scrape_plc_candidates <- function(url, xml_root, df_existing_persons) {
  candidates_nodes <- XML::getNodeSet(xml_root, ".//article[@class = 'person__item-container']")
  
  cnt <- 0
  
  for (j in 1:length(candidates_nodes)) {
    
    full_name <- NA
    first_name <- NA
    last_name <- NA
    district <- NA 
    party <- NA
    is_female <- NA
    twitter_id <- NA
    twitter_handle <- NA
    twitter_url <- NA
    twitter_account_protected <- NA
    source <- NA
    country <- NA
    province <- NA
    key <- NA
    type <- NA
    schema <- NA
    
    # extract candidate node
    current_node <- candidates_nodes[[j]]
    
    # extract data from node
    full_name <- XML::xmlValue(XML::xpathApply(current_node, ".//h2[@class='person__name']"))
    first_name <- strsplit(full_name, " ")[[1]][1]
    last_name <- paste(strsplit(full_name, " ")[[1]][2:length(strsplit(full_name, " ")[[1]])], collapse=" ")
    full_name <- paste(last_name, first_name, sep=', ')
    district <- XML::xmlValue(XML::xpathApply(current_node, ".//h3[@class='person__riding-name']"))
    country <- "CA"
    province <- as.character(stringr::str_match( XML::xmlGetAttr(current_node, "data-groups"), "[A-Z][A-Z]"))

    twitter_node <- XML::xpathApply(current_node, ".//a[@class='person__social-link person__social-link--twitter']")
    if (length(twitter_node) > 0) twitter_url <- XML::xmlGetAttr(twitter_node[[1]], "href")
    twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
    if (grepl("\\?", twitter_handle)) {
      twitter_handle <- substr(twitter_handle, 1, sapply(regexpr("[a-z]\\?[a-z|A-Z]", twitter_handle, perl=TRUE), tail, 1))
    }
    if (!is.na(twitter_handle)) twitter_handle <- gsub("\\/", "", twitter_handle)
    
    is_female <- gender::gender(rm_accent(first_name))$gender == "female"
    if (length(is_female) == 0) is_female <- NA
    
    # find out if already in hub as candidate OR ALSO as MP
    matching_existing_persons_row <- which(df_existing_persons$data.firstName == first_name & df_existing_persons$data.lastName == last_name)
    
    if (length(matching_existing_persons_row) == 0) {
      # no match found - we can add the candidate to the hub
      key <- digest::digest(full_name)
      type <- "candidate"
      schema <- "v2"
      
      metadata_to_commit <- list(source=url, country=country, province_or_state=province)
      data_to_commit <- list(fullName=full_name, firstName=first_name, lastName=last_name, currentDistrict=district, 
                             currentParty="Liberal", currentProvinceOrState=province, isFemale=is_female,
                             twitterID=NA, twitterHandle=twitter_handle, twitterAccountProtected=NA)
      
      # commit tu hub
      clessnhub::create_item('persons', key, type, schema, metadata_to_commit, data_to_commit)
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat(paste(metadata_to_commit, collapse=' * '), '\n')
      cat(paste(data_to_commit, collapse=' * '), '\n')
      
    } else {
      # match found - no touching anything OR overwrite existing CANDIDATE but no touching MP?
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat('This person',full_name, 'is already in the hub', '\n')

      if (is.na(df_existing_persons$data.twitterHandle[matching_existing_persons_row]) && !is.na(twitter_handle)) {
        # no twitter handle for this MP - add it and update item in hub
        df_existing_persons$data.twitterHandle[matching_existing_persons_row] <- twitter_handle
        key <- df_existing_persons$key[matching_existing_persons_row]
        type <- df_existing_persons$type[matching_existing_persons_row]
        schema <- df_existing_persons$schema[matching_existing_persons_row]
        data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
        metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
        cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, sep=' ')
        clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
      }
    }
  }
}


#######################################
scrape_pcc_candidates <- function(url, xml_root, df_existing_persons) {
  candidates_nodes <- XML::getNodeSet(xml_root, ".//div[@class = 'candidate-card']")
  
  cnt <- 0
  
  for (j in 1:length(candidates_nodes)) {
    
    full_name <- NA
    first_name <- NA
    last_name <- NA
    district <- NA 
    party <- NA
    is_female <- NA
    twitter_id <- NA
    twitter_handle <- NA
    twitter_url <- NA
    twitter_account_protected <- NA
    source <- NA
    country <- NA
    province <- NA
    key <- NA
    type <- NA
    schema <- NA
    
    # extract candidate node
    current_node <- candidates_nodes[[j]]
    
    # extract data from node
    first_name <- XML::xmlValue(XML::xpathApply(current_node, ".//h3"))[1]
    last_name <- XML::xmlValue(XML::xpathApply(current_node, ".//h3"))[2]
    full_name <- paste(last_name, first_name, sep=', ')
    
    district <- XML::xmlValue(XML::xpathApply(current_node, ".//p[@class='riding-title']"))
    country <- "CA"
    
    province <- NA
    
    twitter_node <- XML::xpathApply(current_node, ".//a[@data-type='twitter']")
    if (length(twitter_node) > 0) twitter_url <- XML::xmlGetAttr(twitter_node[[1]], "href")
    twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
    if (grepl("\\?", twitter_handle)) {
      twitter_handle <- substr(twitter_handle, 1, sapply(regexpr("[a-z]\\?[a-z|A-Z]", twitter_handle, perl=TRUE), tail, 1))
    }
    
    is_female <- gender::gender(rm_accent(first_name))$gender == "female"
    if (length(is_female) == 0) is_female <- NA
    if (first_name == "Erin" && last_name == "O'Toole") is_female <- 0
    
    # find out if already in hub as candidate OR ALSO as MP
    matching_existing_persons_row <- which(df_existing_persons$data.firstName == first_name & df_existing_persons$data.lastName == last_name)
    
    if (length(matching_existing_persons_row) == 0) {
      # no match found - we can add the candidate to the hub
      key <- digest::digest(full_name)
      type <- "candidate"
      schema <- "v2"
      
      metadata_to_commit <- list(source=url, country=country, province_or_state=province)
      data_to_commit <- list(fullName=full_name, firstName=first_name, lastName=last_name, currentDistrict=district, 
                             currentParty="CPC", currentProvinceOrState=province, isFemale=is_female,
                             twitterID=NA, twitterHandle=twitter_handle, twitterAccountProtected=NA)
      
      # commit tu hub
      clessnhub::create_item('persons', key, type, schema, metadata_to_commit, data_to_commit)
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat(paste(metadata_to_commit, collapse=' * '), '\n')
      cat(paste(data_to_commit, collapse=' * '), '\n')
      
    } else {
      # match found - no touching anything OR overwrite existing CANDIDATE but no touching MP?
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat('This person',full_name, 'is already in the hub', '\n')

      if (is.na(df_existing_persons$data.twitterHandle[matching_existing_persons_row]) && !is.na(twitter_handle)) {
        # no twitter handle for this MP - add it and update item in hub
        df_existing_persons$data.twitterHandle[matching_existing_persons_row] <- twitter_handle
        key <- df_existing_persons$key[matching_existing_persons_row]
        type <- df_existing_persons$type[matching_existing_persons_row]
        schema <- df_existing_persons$schema[matching_existing_persons_row]
        data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
        metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
        cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, sep=' ')
        clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
      }
    }#if (length(matching_existing_persons_row) == 0) 
  }#for (j in 1:length(candidates_nodes))
}


#######################################
scrape_blq_candidates <- function(url, url_suffix, xml_root, df_existing_persons) {
  
  base_url <- url
  cnt <- 0
  for (i_page in 1:15) {
    
    url <- paste(base_url,url_suffix,i_page,sep='')
    
    r <- safe_httr_GET(url)
    
    if (r$result$status_code == 200) {
      parsed_html <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      xml_root <- XML::xmlRoot(parsed_html)    
      candidates_nodes <- XML::getNodeSet(xml_root, ".//article")
      if (length(candidates_nodes) > 0) {
        print(length(candidates_nodes))
        for (j in 1:length(candidates_nodes)) {
          
          full_name <- NA
          first_name <- NA
          last_name <- NA
          district <- NA 
          party <- NA
          is_female <- NA
          twitter_id <- NA
          twitter_handle <- NA
          twitter_url <- NA
          twitter_account_protected <- NA
          source <- NA
          country <- NA
          province <- NA
          key <- NA
          type <- NA
          schema <- NA
          
          # extract candidate node
          current_node <- candidates_nodes[[j]]
          
          # extract data from node
          full_name <- XML::xmlValue(XML::xpathApply(current_node, ".//h1"))[2]
          first_name <- strsplit(full_name, " ")[[1]][1]
          last_name <- paste(strsplit(full_name, " ")[[1]][2:length(strsplit(full_name, " ")[[1]])], collapse=" ")
          full_name <- paste(last_name, first_name, sep=', ')
          district <- XML::xmlValue(XML::xpathApply(current_node, ".//h2"))
          country <- "CA"
          province <- NA
          twitter_node <- XML::xpathApply(current_node, ".//li[@class='twitter']")
          if (length(twitter_node) > 0) {
            twitter_node <- XML::xpathApply(twitter_node[[1]], ".//a")
            twitter_url <- XML::xmlGetAttr(twitter_node[[1]], "href")
            twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
            if (grepl("\\?", twitter_handle)) {
              twitter_handle <- substr(twitter_handle, 1, sapply(regexpr("[a-z]\\?[a-z|A-Z]", twitter_handle, perl=TRUE), tail, 1))
            }
          }
          else {
            twitter_handle <- NA
          }
          is_female <- gender::gender(rm_accent(first_name))$gender == "female"
          if (length(is_female) == 0) is_female <- NA
          
          # find out if already in hub as candidate OR ALSO as MP
          matching_existing_persons_row <- which(df_existing_persons$data.firstName == first_name & df_existing_persons$data.lastName == last_name)
          
          if (length(matching_existing_persons_row) == 0) {
            # no match found - we can add the candidate to the hub
            key <- digest::digest(full_name)
            type <- "candidate"
            schema <- "v2"
            
            metadata_to_commit <- list(source=url, country=country, province_or_state=province)
            data_to_commit <- list(fullName=full_name, firstName=first_name, lastName=last_name, currentDistrict=district, 
                                   currentParty="BQ", currentProvinceOrState=province, isFemale=is_female,
                                   twitterID=NA, twitterHandle=twitter_handle, twitterAccountProtected=NA)
            
            # commit tu hub
            clessnhub::create_item('persons', key, type, schema, metadata_to_commit, data_to_commit)
            cnt <- cnt+1
            cat('\n',cnt,'\n')
            cat('=================================================================================\n')
            cat(paste(metadata_to_commit, collapse=' * '), '\n')
            cat(paste(data_to_commit, collapse=' * '), '\n')
            
          } else {
            # match found - no touching anything OR overwrite existing CANDIDATE but no touching MP?
            cnt <- cnt+1
            cat('\n',cnt,'\n')
            cat('=================================================================================\n')
            cat('This person',full_name, 'is already in the hub', '\n')

            if (is.na(df_existing_persons$data.twitterHandle[matching_existing_persons_row]) && !is.na(twitter_handle)) {
              # no twitter handle for this MP - add it and update item in hub
              df_existing_persons$data.twitterHandle[matching_existing_persons_row] <- twitter_handle
              key <- df_existing_persons$key[matching_existing_persons_row]
              type <- df_existing_persons$type[matching_existing_persons_row]
              schema <- df_existing_persons$schema[matching_existing_persons_row]
              data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
              metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
              cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, sep=' ')
              clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
            }
          }
        } #for (j in 1:length(candidates_nodes))
      }#if (length(candidates_nodes) > 0)
    } else {
      print(paste("could nor read page", url))
    }#if (r$result$status_code == 200)
  } #for (i_page in 1:15)
}


#######################################
scrape_npd_candidates <- function(url, xml_root, df_existing_persons) {
  candidates_nodes <- XML::getNodeSet(xml_root, ".//div[@class = 'campaign-civics-list-inner']")
  
  cnt <- 0
  
  for (j in 1:length(candidates_nodes)) {
    
    full_name <- NA
    first_name <- NA
    last_name <- NA
    district <- NA 
    party <- NA
    is_female <- NA
    twitter_id <- NA
    twitter_handle <- NA
    twitter_url <- NA
    twitter_account_protected <- NA
    source <- NA
    country <- NA
    province <- NA
    key <- NA
    type <- NA
    schema <- NA
    
    # extract candidate node
    current_node <- candidates_nodes[[j]]
    
    # extract data from node
    first_name <- XML::xmlGetAttr(XML::xpathApply(current_node, ".//div[@class='civic-data']")[[1]], "data-firstname")
    last_name <- XML::xmlGetAttr(XML::xpathApply(current_node, ".//div[@class='civic-data']")[[1]], "data-lastname")
    full_name <- paste(last_name, first_name, sep=', ')
    district <- XML::xmlGetAttr(XML::xpathApply(current_node, ".//div[@class='civic-data']")[[1]], "data-riding-name")
    country <- "CA"
    province <- NA
    twitter_url <- XML::xmlGetAttr(XML::xpathApply(current_node, ".//div[@class='civic-data']")[[1]], "data-twitter-link")
    if (length(twitter_url) > 0 || nchar(twitter_url) != 0) {
      twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
      if (grepl("\\?", twitter_handle)) {
        twitter_handle <- substr(twitter_handle, 1, sapply(regexpr("[a-z]\\?[a-z|A-Z]", twitter_handle, perl=TRUE), tail, 1))
      }
    } else {
      twitter_handle <- NA
    }
    
    is_female <- gender::gender(rm_accent(first_name))$gender == "female"     
    if (length(is_female) == 0) is_female <- NA
    
    # find out if already in hub as candidate OR ALSO as MP
    matching_existing_persons_row <- which(df_existing_persons$data.firstName == first_name & df_existing_persons$data.lastName == last_name)
    
    if (length(matching_existing_persons_row) == 0) {
      # no match found - we can add the candidate to the hub
      key <- digest::digest(full_name)
      type <- "candidate"
      schema <- "v2"
      
      metadata_to_commit <- list(source=url, country=country, province_or_state=province)
      data_to_commit <- list(fullName=full_name, firstName=first_name, lastName=last_name, currentDistrict=district, 
                             currentParty="NPD", currentProvinceOrState=province, isFemale=is_female,
                             twitterID=NA, twitterHandle=twitter_handle, twitterAccountProtected=NA)
      
      # commit tu hub
      clessnhub::create_item('persons', key, type, schema, metadata_to_commit, data_to_commit)
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat(paste(metadata_to_commit, collapse=' * '), '\n')
      cat(paste(data_to_commit, collapse=' * '), '\n')
      
    } else {
      # match found - no touching anything OR overwrite existing CANDIDATE but no touching MP?
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat('This person',full_name, 'is already in the hub', '\n')

      if (is.na(df_existing_persons$data.twitterHandle[matching_existing_persons_row]) && !is.na(twitter_handle)) {
        # no twitter handle for this MP - add it and update item in hub
        df_existing_persons$data.twitterHandle[matching_existing_persons_row] <- twitter_handle
        key <- df_existing_persons$key[matching_existing_persons_row]
        type <- df_existing_persons$type[matching_existing_persons_row]
        schema <- df_existing_persons$schema[matching_existing_persons_row]
        data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
        metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
        cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, sep=' ')
        clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
      }
    }
  }
}


#######################################
scrape_grn_candidates <- function(url, xml_root, df_existing_persons) {
  candidates_nodes <- XML::getNodeSet(xml_root, ".//div[@class = 'candidate-modal hidden']")
  
  cnt <- 0
  
  for (j in 1:length(candidates_nodes)) {
    
    full_name <- NA
    first_name <- NA
    last_name <- NA
    district <- NA 
    party <- NA
    is_female <- NA
    twitter_id <- NA
    twitter_handle <- NA
    twitter_url <- NA
    twitter_account_protected <- NA
    source <- NA
    country <- NA
    province <- NA
    key <- NA
    type <- NA
    schema <- NA
    
    # extract candidate node
    current_node <- candidates_nodes[[j]]
    contact_node <- XML::xpathApply(current_node, ".//div[@class='candidate-contact']")
    candidate_social <- XML::xpathApply(current_node, ".//div[@class='candidate-modal hidden']")
    
    # extract data from node
    full_name <- XML::xmlValue(XML::xpathApply(current_node, ".//h4[@class='modal-title']"))
    full_name <- substr(full_name, 1, sapply(regexpr("[a-z]\\sin\\s[a-z|A-Z]", full_name, perl=TRUE),tail,1))
    first_name <- strsplit(full_name, " ")[[1]][1]
    last_name <- paste(strsplit(full_name, " ")[[1]][2:length(strsplit(full_name, " ")[[1]])], collapse=" ")
    full_name <- paste(last_name, first_name, sep=', ')

    district <- XML::xmlValue(XML::xpathApply(current_node, ".//h4[@class='modal-title']"))
    district <- substr(district, sapply(regexpr("[a-z]\\sin\\s[a-z|A-Z]", district, perl=TRUE),tail,1)+5, nchar(district))
    
    country <- "CA"
    province <- NA

    social_node <- XML::xpathApply(current_node, ".//div[@class='candidate-social-media']")
    if (length(social_node) > 0) social_urls <- XML::xpathApply(social_node[[1]], ".//a")
    twitter_node <- XML::xpathApply(social_node[[1]], ".//a[@title='Twitter']")
    if (length(twitter_node) > 0 ) twitter_url <- XML::xmlGetAttr(twitter_node[[1]], "href")
    if (!is.na(twitter_url)) twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
    if (grepl("\\?", twitter_handle)) {
      twitter_handle <- substr(twitter_handle, 1, sapply(regexpr("[a-z]\\?[a-z|A-Z]", twitter_handle, perl=TRUE), tail, 1))
    }
    
    is_female <- gender::gender(rm_accent(first_name))$gender == "female"     
    if (length(is_female) == 0) is_female <- NA
    
    # find out if already in hub as candidate OR ALSO as MP
    matching_existing_persons_row <- which(df_existing_persons$data.firstName == first_name & df_existing_persons$data.lastName == last_name)
    
    if (length(matching_existing_persons_row) == 0) {
      # no match found - we can add the candidate to the hub
      key <- digest::digest(full_name)
      type <- "candidate"
      schema <- "v2"
      
      metadata_to_commit <- list(source=url, country=country, province_or_state=province)
      data_to_commit <- list(fullName=full_name, firstName=first_name, lastName=last_name, currentDistrict=district, 
                             currentParty="Liberal", currentProvinceOrState=province, isFemale=is_female,
                             twitterID=NA, twitterHandle=twitter_handle, twitterAccountProtected=NA)
      
      # commit tu hub
      clessnhub::create_item('persons', key, type, schema, metadata_to_commit, data_to_commit)
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat(paste(metadata_to_commit, collapse=' * '), '\n')
      cat(paste(data_to_commit, collapse=' * '), '\n')
      
    } else {
      # match found - no touching anything OR overwrite existing CANDIDATE but no touching MP?
      cnt <- cnt+1
      cat('\n',cnt,'\n')
      cat('=================================================================================\n')
      cat('This person',full_name, 'is already in the hub', '\n')

      if (is.na(df_existing_persons$data.twitterHandle[matching_existing_persons_row]) && !is.na(twitter_handle)) {
        # no twitter handle for this MP - add it and update item in hub
        df_existing_persons$data.twitterHandle[matching_existing_persons_row] <- twitter_handle
        key <- df_existing_persons$key[matching_existing_persons_row]
        type <- df_existing_persons$type[matching_existing_persons_row]
        schema <- df_existing_persons$schema[matching_existing_persons_row]
        data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
        metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
        cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, sep=' ')
        clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
      }
    }
  }
}


#######################################
scrape_ppc_candidates <- function(url, xml_root, df_existing_persons) {
  candidates_nodes <- XML::getNodeSet(xml_root, ".//div[@class = 'row']")
  
  cnt <- 0
  
  for (j in 1:length(candidates_nodes)) {
    
    full_name <- NA
    first_name <- NA
    last_name <- NA
    district <- NA 
    party <- NA
    is_female <- NA
    twitter_id <- NA
    twitter_handle <- NA
    twitter_url <- NA
    twitter_account_protected <- NA
    source <- NA
    country <- NA
    province <- NA
    key <- NA
    type <- NA
    schema <- NA
    
    # extract candidate node
    current_node <- candidates_nodes[[j]]
    
    if (length(XML::xmlChildren(candidates_nodes[[j]])) == 11) {
       # It's a candidate node  
      # extract data from node
      full_name <- XML::xmlValue(XML::xpathApply(current_node, ".//p"))
      if (grepl("Vacant", full_name)) next 
      first_name <- strsplit(full_name, " ")[[1]][1]
      last_name <- paste(strsplit(full_name, " ")[[1]][2:length(strsplit(full_name, " ")[[1]])], collapse=" ")
      full_name <- paste(last_name, first_name, sep=', ')
      
      district <- XML::xmlValue(XML::xpathApply(current_node, ".//h4"))
      country <- "CA"
      province <- NA
      
      social_node <- XML::xpathApply(current_node, ".//a")
      for (k in 1:length(social_node)){
         if (grepl("twitter", XML::xmlGetAttr(social_node[[k]], "href"))) twitter_node <- social_node[[k]]  
      }
      
      if (length(twitter_node) > 0) twitter_url <- XML::xmlGetAttr(twitter_node, "href")
      twitter_handle <- substr(twitter_url, sapply(regexpr("[a-z]/[a-z|A-Z]", twitter_url, perl=TRUE), tail, 1)+2, nchar(twitter_url))
      if (grepl("\\?", twitter_handle)) {
        twitter_handle <- substr(twitter_handle, 1, sapply(regexpr("[a-z]\\?[a-z|A-Z]", twitter_handle, perl=TRUE), tail, 1))
      }
      
      is_female <- gender::gender(rm_accent(first_name))$gender == "female"
      if (length(is_female) == 0) is_female <- NA
      
      
      # find out if already in hub as candidate OR ALSO as MP
      matching_existing_persons_row <- which(df_existing_persons$data.firstName == first_name & df_existing_persons$data.lastName == last_name)
      
      if (length(matching_existing_persons_row) == 0) {
        # no match found - we can add the candidate to the hub
        key <- digest::digest(full_name)
        type <- "candidate"
        schema <- "v2"
        
        metadata_to_commit <- list(source=url, country=country, province_or_state=province)
        data_to_commit <- list(fullName=full_name, firstName=first_name, lastName=last_name, currentDistrict=district, 
                               currentParty="PPC", currentProvinceOrState=province, isFemale=is_female,
                               twitterID=NA, twitterHandle=twitter_handle, twitterAccountProtected=NA)
        
        # commit tu hub
        clessnhub::create_item('persons', key, type, schema, metadata_to_commit, data_to_commit)
        cnt <- cnt+1
        cat('\n',cnt,'\n')
        cat('=================================================================================\n')
        cat(paste(metadata_to_commit, collapse=' * '), '\n')
        cat(paste(data_to_commit, collapse=' * '), '\n')
        
      } else {
        # match found - no touching anything OR overwrite existing CANDIDATE but no touching MP?
        cnt <- cnt+1
        cat('\n',cnt,'\n')
        cat('=================================================================================\n')
        cat('This person',full_name, 'is already in the hub', '\n')
        
        if (is.na(df_existing_persons$data.twitterHandle[matching_existing_persons_row]) && !is.na(twitter_handle)) {
          # no twitter handle for this MP - add it and update item in hub
          df_existing_persons$data.twitterHandle[matching_existing_persons_row] <- twitter_handle
          key <- df_existing_persons$key[matching_existing_persons_row]
          type <- df_existing_persons$type[matching_existing_persons_row]
          schema <- df_existing_persons$schema[matching_existing_persons_row]
          data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
          metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
          cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, sep=' ')
          clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
        }
      } #if (length(matching_existing_persons_row) == 0)
    } #if (length(XML::xmlChildren(candidates_nodes[[i]])) == 11)
  }#for (j in 1:length(candidates_nodes))
}

###############################################################################
# Main
#
#
#
urls_list <- list(
  plc_url="https://liberal.ca/your-liberal-candidates/",
  plc_url_suffix=NULL,
  
  npd_url="https://www.ndp.ca/team",
  npd_url_suffix=NULL,
  
  blq_url="https://www.blocquebecois.org/les-candidates-et-candidats/",
  plc_url_suffix="page/",
  
  pcc_url="https://www.conservateur.ca/candidates/",
  pcc_url_suffix=NULL,
  
  grn_url="https://www.greenparty.ca/en/candidates",
  grn_url_suffix=NULL,
  
  ppc_url="https://www.peoplespartyofcanada.ca/our_candidates",
  ppc_url_suffix=NULL
)

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

filter <- clessnhub::create_filter(type="mp", schema="v2")
dfMPs <- clessnhub::get_items('persons', filter = filter)
filter <- clessnhub::create_filter(type="candidate", schema="v2")
dfCandidates <- clessnhub::get_items('persons', filter = filter)

dfExistingPersons <- if (!is.null(dfCandidates)) dfMPs %>% dplyr::full_join(dfCandidates) else dfMPs


# Loop through the list of URLs
for (i in 1:length(urls_list)) {
  if (!grepl("suffix", names(urls_list)[[i]])) {
    r <- safe_httr_GET(urls_list[[i]])
    if (r$result$status_code == 200) {
      parsed_html <- XML::htmlTreeParse(httr::content(r$result), asText = TRUE, isHTML = TRUE, useInternalNodes = TRUE)
      xml_root <- XML::xmlRoot(parsed_html)
      
      if (grepl("plc", names(urls_list)[[i]])) {
        scrape_plc_candidates(as.character(urls_list[[i]]), xml_root, dfExistingPersons)
      } 
      
      if (grepl("pcc", names(urls_list)[[i]])) {
        scrape_pcc_candidates(as.character(urls_list[[i]]), xml_root, dfExistingPersons)
      } 
      
      if (grepl("blq", names(urls_list)[[i]])) {
        scrape_blq_candidates(as.character(urls_list[[i]]), as.character(urls_list[[i+1]]), xml_root, dfExistingPersons)
      }
      
      if (grepl("npd", names(urls_list)[[i]])) {
        scrape_npd_candidates(as.character(urls_list[[i]]), xml_root, dfExistingPersons)
      }
      
      if (grepl("grn", names(urls_list)[[i]])) {
        scrape_grn_candidates(as.character(urls_list[[i]]), xml_root, dfExistingPersons)
      }
      
      if (grepl("ppc", names(urls_list)[[i]])) {
        scrape_ppc_candidates(as.character(urls_list[[i]]), xml_root, dfExistingPersons)
      } 
      
    } else {
      stop(paste("Unable ro retrieve index page", urls_list[[i]]))
    }
  }
}
