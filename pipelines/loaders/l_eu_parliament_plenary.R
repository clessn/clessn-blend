###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                           l_eu_parliament_plenary                        
#                                                                             
# This script retrieves html or xml files from the datalake in the path
# agoraplus/european_parliament and parses them to insert interventions in the
# agoraplus_european_parliament table.
#                                                                             
###############################################################################


###############################################################################
########################       Auxiliary Functions       ######################
###############################################################################

clntxt <- function(x) {
  x <- gsub("\\\"", "", x)
  return(x)
}


"%contains_one_of%" <- function(vec_y, x) {
   #checks that at least one the words in x is in a string within the vector of strings vec_y
   for (y in vec_y) {
      if (all(strsplit(y, " ")[[1]] %in% strsplit(x, " ")[[1]])) return(TRUE)
   }
   return(FALSE)
}

which_contains_one_of <- function(vec_y, x) {
  #checks that at least one the words in x is in a string within the vector of strings vec_y
  found <- FALSE
  best <- ""

  for (y in vec_y) {
    if (all(strsplit(y, " ")[[1]] %in% strsplit(x, " ")[[1]])) {
      found <- TRUE
      if (nchar(y) > nchar(best)) best <- y
    }
  }

  if (found) {
    return(best)
  } else {
    return("")
  }
}


detect_president_change <- function(x) {
  # return (  clessnverse::rm_accents(tolower(president)) %contains_one_of% clessnverse::rm_accents(gsub("\\.|\\:", "", tolower(x))) ||
  #           clessnverse::rm_accents(tolower(vicepresident)) %contains_one_of% clessnverse::rm_accents(tolower(x)) ||
  #           clessnverse::rm_accents(tolower(presidency)) %contains_one_of% clessnverse::rm_accents(tolower(x)) ||
  #           clessnverse::rm_accents(tolower(presidency_of_the_hon)) %contains_one_of% clessnverse::rm_accents(tolower(x)) )
  return (grepl(paste(clessnverse::rm_accents(tolower(president)), collapse="|"), clessnverse::rm_accents(gsub("\\.|\\:", "", tolower(x)))) ||
          grepl(paste(clessnverse::rm_accents(tolower(vicepresident)), collapse="|"), clessnverse::rm_accents(tolower(x))) ||
          grepl(paste(clessnverse::rm_accents(tolower(presidency)), collapse="|"), clessnverse::rm_accents(tolower(x))) ||
          grepl(paste(clessnverse::rm_accents(tolower(presidency_of_the_hon)), collapse="|"), clessnverse::rm_accents(tolower(x))) )

}


extract_president_name <- function(x) {
  x <- gsub("\\.|\\:", "", x)
  x <- tolower(x)

  # pattern <- which_contains_one_of(clessnverse::rm_accents(tolower(presidency_of_the_hon)), clessnverse::rm_accents(x))
  # r <- gsub(tolower(pattern), "", clessnverse::rm_accents(x))

  # pattern <- which_contains_one_of(clessnverse::rm_accents(tolower(presidency)), clessnverse::rm_accents(tolower(r)))
  # r <- gsub(tolower(pattern), "", clessnverse::rm_accents(r))

  # pattern <- which_contains_one_of(clessnverse::rm_accents(tolower(vicepresident)), clessnverse::rm_accents(tolower(r)))
  # r <- gsub(tolower(pattern), "", clessnverse::rm_accents(r))

  # pattern <- which_contains_one_of(clessnverse::rm_accents(tolower(president)), clessnverse::rm_accents(tolower(r)))
  # r <- gsub(tolower(pattern), "", clessnverse::rm_accents(r))

  r <- gsub(tolower(paste(clessnverse::rm_accents(presidency_of_the_hon),collapse = "|")), "", clessnverse::rm_accents(x))
  r <- gsub(tolower(paste(clessnverse::rm_accents(presidency),collapse = "|")), "", clessnverse::rm_accents(r))
  r <- gsub(tolower(paste(clessnverse::rm_accents(vicepresident),collapse = "|")), "", clessnverse::rm_accents(r))
  r <- gsub(tolower(paste(clessnverse::rm_accents(president),collapse = "|")), "", clessnverse::rm_accents(r))

  
  #r <- gsub(pattern, "", r)
  r <- stringr::str_to_title(trimws(r))

  return(r)
}

add_speaker_to_the_hub <- function(df_speaker) {
  person_metadata_row <- list(
    "source"="https://www.europarl.europa.eu/meps/fr/download/advanced/xml?name=",
    "country"=df_speaker$country,
    "institution"="European Parliament",
    "province_or_state"=df_speaker$country,
    "twitterAccountHasBeenScraped"="0")

  row <- list(
    ".id" = gsub(" ", "_", tolower(df_speaker$fullname)),
    "full_name"=df_speaker$fullname,
    "gender"= if (!is.na(speaker_gender) && !is.null(speaker_gender) && speaker_gender=="female") as.character(1) else as.character(0),
    "lastName"=speaker_last_name,
    "firstName"=speaker_first_name,
    "twitterID"=NA_character_,
    "isMinister"="0",
    "twitterName"=NA_character_,
    "currentParty"=df_speaker$party,
    "twitterHandle"=NA_character_,
    "currentMinister"=NA_character_,
    "currentPolGroup"=df_speaker$polgroup,
    "twitterLocation"=NA_character_,
    "twitterPostsCount"=NA_character_,
    "twitterProfileURL"=NA_character_,
    "twitterListedCount"=NA_character_,
    "twitterFriendsCount"=NA_character_,
    "currentFunctionsList"=NA_character_,
    "twitterFollowersCount"=NA_character_,
    "currentProvinceOrState"=df_speaker$country,
    "twitterAccountVerified"=NA_character_,
    "twitterProfileImageURL"=NA_character_,
    "twitterAccountCreatedAt"=NA_character_,
    "twitterAccountCreatedOn"=NA_character_,
    "twitterAccountProtected"=NA_character_,
    "twitterProfileBannerURL"=NA_character_,
    "twitterUpdateDateStamps"=NA_character_,
    "twitterUpdateTimeStamps"=NA_character_)
  
  
  if (add_speaker_to_the_hub) {
    speaker_key <- if (!is.na(df_speaker$mepid)) paste("EU-",df_speaker$mepid,sep='') else paste("EU-",digest::digest(speaker_full_name),sep='')
    clessnverse::logit(scriptname=scriptname, message=paste("adding", speaker_full_name,  "/",full_name_native, "-", speaker_key, "to the hub"), logger = logger)
    clessnverse::logit(scriptname=scriptname, message="speaker metadata:", logger = logger)
    clessnverse::logit(scriptname=scriptname, message=paste(person_metadata_row, collapse = " * "), logger = logger)
    clessnverse::logit(scriptname=scriptname, message="speaker data:", logger = logger)
    clessnverse::logit(scriptname=scriptname, message=paste(person_data_row, collapse = " * "), logger = logger)
    clessnverse::logit(scriptname=scriptname, message="\n", logger = logger)
    if (opt$hub_mode != "skip") clessnhub::create_item("persons", speaker_key, "mp", "v3", person_metadata_row, person_data_row)
    names(person_metadata_row) <- paste("metadata.", names(person_metadata_row),sep='')
    names(person_data_row) <- paste("data.", names(person_data_row),sep='')
    dfPersons <- dfPersons %>% rbind(cbind(data.frame(key=speaker_key, type="mp", schema="v3", uuid=""), as.data.frame(person_metadata_row), as.data.frame(person_data_row)))
  }
  
  if (update_speaker_in_the_hub) {
    speaker_key <- df_speaker$mepid

    clessnverse::logit(scriptname=scriptname, message=paste("updating", speaker_full_name, "/",full_name_native, "-", speaker_key, "in the hub"), logger = logger)
    clessnverse::logit(scriptname=scriptname, message="speaker metadata:", logger = logger)
    clessnverse::logit(scriptname=scriptname, message=paste(person_metadata_row, collapse = " * "), logger = logger)
    clessnverse::logit(scriptname=scriptname, message="speaker data:", logger = logger)
    clessnverse::logit(scriptname=scriptname, message=paste(person_data_row, collapse = " * "), logger = logger)
    clessnverse::logit(scriptname=scriptname, message="\n", logger = logger)
    if (opt$hub_mode != "skip") clessnhub::edit_item("persons", speaker_key, "mp", "v3", person_metadata_row, person_data_row)
    names(person_metadata_row) <- paste("metadata.", names(person_metadata_row),sep='')
    names(person_data_row) <- paste("data.", names(person_data_row),sep='')
    #dfPersons[df_persons_row,] <- cbind(data.frame(key=dfPersons$key[df_persons_row], type=dfPersons$type[df_persons_row], schema=dfPersons$schema[df_persons_row], dfPersons$uuid[df_persons_row]), as.data.frame(person_metadata_row), as.data.frame(person_data_row))
    dfPersons <- dfPersons %>% rows_update(cbind(data.frame(key=dfPersons$key[df_persons_row], 
                                                            type=dfPersons$type[df_persons_row], 
                                                            schema=dfPersons$schema[df_persons_row],
                                                            uuid=dfPersons$uuid[df_persons_row]), 
                                                            as.data.frame(person_metadata_row),
                                                            as.data.frame(person_data_row)), by = "key")
  }
}

###############################################################################
###############################################################################
#####################           core functions          #######################
###############################################################################
###############################################################################

process_debate_xml <- function(lake_item, xml_core) {

  event_url <- lake_item$metadata$source
  event_id <- lake_item$key


  # Get the length of all branches of the XML document
  core_xml_nbchapters <- length(names(xml_core))

    
  ###############################
  # Columns of the simple dataset
  event_source_type <- "Débats et vidéos | Plénière | Parlement européen"
  
  event_date <- substr(XML::xmlGetAttr(xml_core[[2]][["TL-CHAP"]], "VOD-START"),1,10)
  if (length(event_date) == 0) event_date <- substr(XML::xmlGetAttr(xml_core[[2]][["NUMERO"]], "VOD-START"),1,10)

  event_start_time <- substr(XML::xmlGetAttr(xml_core[[2]][["TL-CHAP"]], "VOD-START"),12,19)
  if (length(event_start_time) == 0) event_start_time <- substr(XML::xmlGetAttr(xml_core[[2]][["NUMERO"]], "VOD-START"),12,19)

  event_end_time <- substr(XML::xmlGetAttr(xml_core[[core_xml_nbchapters]][["TL-CHAP"]], "VOD-END"),12,19)
  if (length(event_end_time) == 0) event_end_time <- substr(XML::xmlGetAttr(xml_core[[core_xml_nbchapters]][["NUMERO"]], "VOD-END"),12,19)
  
  event_title <- NA  
  
  ####################################
  # The colums of the detailed dataset
  intervention_seqnum <- NA
  speaker_first_name <- NA
  speaker_last_name <- NA
  speaker_full_name <- NA
  full_name_native <- NA
  speaker_gender <- NA
  speaker_is_minister <- NA
  speaker_type <- NA
  speaker_party <- NA
  speaker_polgroup <- NA
  speaker_country <- NA
  intervention_type <- NA
  intervention_lang <- NA
  intervention_word_count <- NA
  intervention_sentence_count <- NA
  intervention_paragraph_count <- NA
  header_text <- NA
  header_text_en <- NA
  intervention_text <- NA
  intervention_text_en <- NA
  
  df_speaker <- data.frame()

  ########################################################
  # Go through the xml document chapter by chapter
  # and strip out any relevant info
  intervention_seqnum <- 0
    
  current_speaker_full_name <- ""
  
  for (j in 1:core_xml_nbchapters) {
    
    # New chapter
    chapter_node <- xml_core[[j]]
    chapter_number <- XML::xmlGetAttr(chapter_node, "NUMBER")
    chapter_title <- XML::xmlValue(XML::xpathApply(chapter_node, "//CHAPTER/TL-CHAP[@VL='EN']")[j])
    
    if (length(chapter_node[["TL-CHAP"]]) == 2) {
      # Here there is a URL within the title of the chapter => most likely a linked document 
      chapter_tabled_docid <- stringr::str_split(XML::xmlGetAttr(chapter_node[["TL-CHAP"]][[2]], "redmap-uri"), "/")[[1]][3]
      
      tabled_document_url <- paste("https://www.europarl.europa.eu/doceo/document/", chapter_tabled_docid, "_EN.html", sep = "")
      tabled_document_html <- RCurl::getURL(tabled_document_url)
      tabled_document_html_table <- XML::readHTMLTable(tabled_document_html)
      
      list1_id <- grep("Texts adopted", tabled_document_html_table)[1]
      list2_id <- grep("Texts adopted", tabled_document_html_table[[list1_id]])[2]
      chapter_adopted_docid <- tabled_document_html_table[[list1_id]][[list2_id]][which(grepl("Texts adopted",tabled_document_html_table[[list1_id]][[list2_id]]))]
      if (!is.null(chapter_adopted_docid)) {
        chapter_adopted_docid <- stringr::str_split(chapter_adopted_docid, " ")[[1]][length(stringr::str_split(chapter_adopted_docid, " ")[[1]])]
      } else {
        chapter_adopted_docid <- NA
      }
      
    } else {
      chapter_tabled_docid <- NA
      chapter_adopted_docid <- NA
    }
      
    chapter_nodes_list <- names(chapter_node)

    if ( "PRES" %in% chapter_nodes_list ) {
      president_node <- xml_core[[j]][[length(xml_core[[j]])]]
      president_name <- XML::xmlValue(president_node)
      # CONTINUE #########president_name <- 
    }
    
    if ( "INTERVENTION" %in% chapter_nodes_list ) {
      # There is one or more interventions in this section.
      # From potentially multiple speakers
      
      # We have to loop through every intervention
      for (k in which(chapter_nodes_list == "INTERVENTION")) {
          intervention_seqnum <- intervention_seqnum + 1
          
          speaker_first_name <- NA
          speaker_last_name <- NA
          speaker_full_name <- NA
          full_name_native <- NA
          speaker_gender <- NA
          speaker_type <- NA
          speaker_party <- NA
          speaker_polgroup <- NA
          speaker_country <- NA
          chapter_tabled_docid <- NA
          chapter_adopted_docid <- NA
          intervention_type <- NA
          intervention_lang <- NA
          intervention_word_count <- NA
          intervention_sentence_count <- NA
          intervention_paragraph_count <- NA
          intervention_header <- NA
          intervention_header_en <- NA
          intervention_text <- NA
          intervention_translated_text <- NA      
          
          df_speaker <- data.frame()
          df_persons_row <- 0
          add_speaker_to_the_hub <- FALSE
          update_speaker_in_the_hub <- FALSE
          
          # Strip out the speaker info
          intervention_node <- xml_core[[j]][[k]]
          speaker_node <- intervention_node[["ORATEUR"]]
          
          speaker_full_name <- XML::xmlGetAttr(speaker_node, "LIB")
          speaker_full_name <- gsub("\u00a0", "", speaker_full_name)
          speaker_full_name <- stringr::str_to_title(speaker_full_name)
          speaker_last_name <- trimws(stringr::str_split(speaker_full_name, "\\|")[[1]][[2]], "both")
          speaker_first_name <- trimws(stringr::str_split(speaker_full_name, "\\|")[[1]][[1]], "both")
          speaker_full_name <- trimws(stringr::str_remove(speaker_full_name, "\\|\\s"))
          speaker_full_name <- stringr::str_squish(speaker_full_name)
  
          speaker_gender <- paste("", gender::gender(clessnverse::splitWords(speaker_first_name)[1])$gender, sep = "")
          if ( speaker_gender == "" ) speaker_gender <- NA
          
          #Get the speaker from the hub
          #If not, get it from the parliament search 
          #If not use the data in the xml structure without properties such as political party etc
          
          df_speaker <- dfPersons[which(tolower(dfPersons$data.fullName) == tolower(speaker_full_name)),]
          df_persons_row <- which(tolower(dfPersons$data.fullName) == tolower(speaker_full_name))
          
          # If we did not find the speaker fullname in the hub, we can try looking for the native spelling of his name also
          if ( nrow(df_speaker) == 0 ) {
            df_speaker <- dfPersons[which(tolower(dfPersons$data.fullNameNative) == tolower(speaker_full_name)),]
            df_persons_row <- which(tolower(dfPersons$data.fullNameNative) == tolower(speaker_full_name))
            keep_full_name_native <- FALSE
          } else {
            keep_full_name_native <- TRUE
          }
        
          if (nrow(df_speaker) == 0) {
            # Not found in hub
            clessnverse::logit(scriptname = scriptname, message = paste("searching for", speaker_full_name, "in the parliament database"), logger = logger)

            df_speaker <- clessnverse::getEuropeMepData(speaker_full_name)
            
            add_speaker_to_the_hub <- FALSE
            update_speaker_in_the_hub <- FALSE
            
            if (is.na(df_speaker$mepid)) {
              # Not found in the parliament database
              # Not much to do...
              clessnverse::logit(scriptname = scriptname, message = paste("could not find", speaker_full_name, "in the parliament database"), logger = logger)

              speaker_country <- countrycode::codelist$country.name.en[which(countrycode::codelist$iso2c == XML::xmlGetAttr(speaker_node, "LG"))]
              if (length(speaker_country) == 0) speaker_country <- NA
              
              speaker_party <- case_when(XML::xmlGetAttr(speaker_node, "PP") != "NULL" ~  XML::xmlGetAttr(speaker_node, "PP"), TRUE ~ NA_character_)
              speaker_mepid <- case_when(XML::xmlGetAttr(speaker_node, "MEPID") != "0" ~  XML::xmlGetAttr(speaker_node, "MEPID"), TRUE ~ NA_character_)
              
              speaker_polgroup <- NA
              
              if (!is.null(XML::xmlGetAttr(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]], "NAME")) && 
                  XML::xmlGetAttr(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]], "NAME")=="I") {
                if ( grepl("winner|presidente?|president,\\s|president-in-office|chancellor|president\\sof|minister|his\\sholiness|secretary|king|ombudsman|chair\\sof\\sthe", clessnverse::rm_accent(tolower(XML::xmlValue(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]])))) ) {
                  speaker_type <- XML::xmlValue(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]])
                } else {
                  speaker_polgroup <- XML::xmlValue(intervention_node[[which(names(intervention_node) == "PARA")[1]]][[1]])
                }
              }
              
              full_name_native <- NA
              df_speaker <- data.frame(mepid=speaker_mepid, fullname=speaker_full_name, country=speaker_country, polgroup=speaker_polgroup, party=speaker_party)

              add_speaker_to_the_hub <- TRUE
              
            } else {
              #### found in the parliament database ####
              #### Add it to the hub!
              add_speaker_to_the_hub <- TRUE
              clessnverse::logit(scriptname = scriptname, message = paste("found", speaker_full_name, "in the parliament database"), logger = logger)
              full_name_native <- stringr::str_to_title(df_speaker$fullname)
            } #is.na(df_speaker$mepid)
            
          } else {
            # Found in hub => convert data structure to match as if we went to find it from parliament web site
            # There might be more that one row in the result because of the html scraper
            # We'll take the one the most complete
            fullest_row <- min(rowSums(is.na(df_speaker)))
            fullest_row <- which(rowSums( is.na(df_speaker)) == fullest_row)
            if (length(fullest_row) > 1) fullest_row <- fullest_row[1]
            df_speaker <- df_speaker[fullest_row,]
            
            if (keep_full_name_native) full_name_native <- df_speaker$data.fullNameNative else full_name_native <- df_speaker$data.fullName
            
            names(df_speaker)[names(df_speaker)=="key"] <- "mepid"
            names(df_speaker)[names(df_speaker)=="data.currentParty"] <- "party"
            names(df_speaker)[names(df_speaker)=="data.fullName"] <- "fullname"
            names(df_speaker)[names(df_speaker)=="data.currentPolGroup"] <- "polgroup"
            names(df_speaker)[names(df_speaker)=="metadata.country"] <- "country"
            
            add_speaker_to_the_hub <- FALSE
            update_speaker_in_the_hub <- FALSE
            
            # Check if the data is complete - update the hub from the parliament database if not
            if ( is.na(df_speaker$party) || is.na(df_speaker$polgroup) || is.na(df_speaker$country) || df_speaker$fullname != df_speaker$data.fullNameNative ) {
              # We have an incomplete MP record - try and complete it from the parliament DB
              df_speakerCheck <- clessnverse::getEuropeMepData(speaker_full_name)

              if (!is.na(df_speakerCheck$polgroup) && df_speakerCheck$polgroup == "Renew Europe Group") df_speakerCheck$polgroup <- "Group Renew Europe"
              
              if ( sum(colSums(is.na(df_speakerCheck))) == ncol(df_speakerCheck) ) {
                # could not find anything better on the parliament web site
              } else {
                if ( df_speakerCheck$country != df_speaker$country || df_speakerCheck$polgroup != df_speaker$polgroup || df_speakerCheck$party != df_speaker$party ) {
                  update_speaker_in_the_hub <- TRUE
                  speaker_key <- df_speaker$mepid
                  df_speaker <- df_speakerCheck
                  df_speaker$mepid <- speaker_key
                } else {
                  update_speaker_in_the_hub <- FALSE
                }
              }
            }
          } #if (nrow(df_speaker) == 0) {
          
          #df_speaker$fullname <- stringr::str_to_title(df_speaker$fullname)
          
          if (update_speaker_in_the_hub || add_speaker_to_the_hub) {
            add_speaker_to_the_hub(df_speaker)
          } #if (update_speaker_in_the_hub || add_speaker_to_the_hub)
          
          if (!is.na(df_speaker$mepid)) {
            speaker_mepid <- df_speaker$mepid
            speaker_party <- df_speaker$party
            speaker_polgroup <- df_speaker$polgroup
            speaker_country <- df_speaker$country
          } else {
            speaker_mepid <- NA
            speaker_party <- case_when(XML::xmlGetAttr(speaker_node, "PP") != "NULL" ~  XML::xmlGetAttr(speaker_node, "PP"),
                                      TRUE ~ NA_character_)
            speaker_polgroup <- NA
            speaker_country <- countrycode::codelist$country.name.en[which(countrycode::codelist$iso2c == XML::xmlGetAttr(speaker_node, "LG"))]
            if (length(speaker_country) == 0) speaker_country <- NA
          }
          
          if (!is.na(speaker_polgroup) && stringr::str_detect(speaker_polgroup, ":")) speaker_polgroup <- trimws(stringr::str_split(speaker_polgroup, ":")[[1]][2])
          
          intervention_type <- trimws(gsub("\\.|\\,|\\s\\–", "", XML::xmlValue(speaker_node[["EMPHAS"]])))
          
          if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), tolower(speaker_full_name)) ) intervention_type <- NA
          if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "\\((.*)\\)$") ) intervention_type <- NA
          
          if ( !is.na(intervention_type) && intervention_type != "" && !(TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_no_translate)) &&
              (is.na(textcat::textcat(tolower(intervention_type))) || textcat::textcat(tolower(intervention_type)) != "english" || tolower(intervention_type) == "im namen der ppe-fraktion") ) {
            
            #if (opt$translate) { 
                if ( TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) ) {
                  t_index <- which(stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) == TRUE) 
                  intervention_type_translated <- intervention_types_manual_translated[[t_index]]
                  if (intervention_type_translated == "On Behalf Of The") {
                    group_name <- gsub(intervention_types_manual_translate[[t_index]], "", tolower(intervention_type))
                    group_name <- trimws(group_name)
                    group_name <- stringr::str_to_title(group_name)
                    intervention_type_translated <- paste(intervention_type_translated, group_name, "Group")
                  }
                  
                  #if ((i %% 5)== 0) 
                  clessnverse::logit(scriptname = scriptname, message = paste("self-translating intervention_type", intervention_type, "to", intervention_type_translated), logger = logger)
                  
                  intervention_type <- intervention_type_translated
                  intervention_type <- gsub("-Gruppen", "", intervention_type)
                  intervention_type <- gsub("-Fraktion", "", intervention_type)
                  intervention_type <- gsub("-Fractie", "", intervention_type)
                } else {
                  clessnverse::logit(scriptname = scriptname, message = paste("translating intervention_type", intervention_type), logger = logger)
                  intervention_type <- clessnverse::translateText(intervention_type, engine="azure", target_lang="en",fake=!opt$translate)[2]
                  #clessnverse::logit(scriptname = scriptname, message = paste("translated intervention_type", intervention_type), logger = logger)
                }
            #}
            
          }
          
          if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteure|rapporteur|representative")) ) {
            intervention_type <- NA
            speaker_type <- "Rapporteur"
          }
          
          if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "member")) ) {
            speaker_type <- intervention_type
            intervention_type <- NA
          }
          
          if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "winner|president,\\s|president-in-office|chancellor|president\\sof|minister|his\\sholiness|secretary|king|ombudsman|chair\\sof\\sthe") ) {
            speaker_type <- intervention_type
            intervention_type <- "Speech"
          }
          
          if ( !is.na(intervention_type) && tolower(intervention_type) == "president" ) {
            intervention_type <- "Moderation"
            speaker_type <- "President"
          } 
          
          if ( is.na(speaker_type) ) speaker_type <- "Member of the Commission"
          
          if ( !is.null(intervention_type) && !is.na(intervention_type) ) {
            intervention_type <- gsub("\\(", "", intervention_type)
            intervention_type <- gsub("\\)", "", intervention_type)
            intervention_type <- gsub("\"", "", intervention_type)
            intervention_type <- gsub("\\\\u2012", "", intervention_type)
            intervention_type <- gsub("\\\\ U2012", "", intervention_type)
            intervention_type <- gsub("blue-card", "Blue Card", intervention_type)
            intervention_type <- gsub("Blue-Card", "Blue Card", intervention_type)
            intervention_type <- gsub("Blue-card", "Blue Card", intervention_type)
            intervention_type <- stringr::str_squish(intervention_type)
            intervention_type <- trimws(intervention_type)
            intervention_type <- stringr::str_to_title(intervention_type)
          }
          
          if ( !is.null(speaker_type) && !is.na(speaker_type) ) {
            if (tolower(speaker_type) == "member of the commission") speaker_type <- "Member of the Commission"
          }
          
          speaker_district <- NA
          speaker_media <- NA    
          
          #intervention_type <- NA
          intervention_lang <- XML::xmlValue(speaker_node[["LG"]])
          intervention_word_count <- 0
          intervention_sentence_count <- 0
          intervention_paragraph_count <- 0
          intervention_text <- ""
          intervention_translated_text <- ""
          
          # Strip out the intervention by looping through paragraphs
          for (l in which(names(intervention_node) == "PARA")) {

            if (is.na(intervention_type)) {
              intervention_type <- trimws(XML::xmlValue(intervention_node[["PARA"]][["EMPHAS"]]))
              
              if ( !is.na(intervention_type) && intervention_type != "" && !(TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_no_translate)) &&
                  (is.na(textcat::textcat(tolower(intervention_type))) || textcat::textcat(tolower(intervention_type)) != "english" || tolower(intervention_type) == "im namen der ppe-fraktion") ) { 
                
                #if (opt$translate) { 
                  if ( TRUE %in% stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) ) {
                    t_index <- which(stringr::str_detect(tolower(intervention_type), intervention_types_manual_translate) == TRUE) 
                    intervention_type_translated <- intervention_types_manual_translated[[t_index]]
                    if (intervention_type_translated == "On Behalf Of The") {
                      group_name <- gsub(intervention_types_manual_translate[[t_index]], "", tolower(intervention_type))
                      group_name <- trimws(group_name)
                      group_name <- stringr::str_to_title(group_name)
                      intervention_type_translated <- paste(intervention_type_translated, group_name, "Group")
                    }
                    
                    if ((i %% 5)== 0) clessnverse::logit(scriptname = scriptname, message = paste("self-translating", intervention_type, "to", intervention_type_translated), logger = logger)
                    
                    intervention_type <- intervention_type_translated
                    intervention_type <- gsub("-Gruppen", "", intervention_type)
                    intervention_type <- gsub("-Fraktion", "", intervention_type)
                    intervention_type <- gsub("-Fractie", "", intervention_type)
                  } else {
                    clessnverse::logit(scriptname = scriptname, message = paste("translating intervention_type", intervention_type), logger = logger)
                    intervention_type <- clessnverse::translateText(intervention_type, engine="azure", target_lang="en",fake=!opt$translate)[2]
                    #clessnverse::logit(scriptname = scriptname, message = paste("translated", intervention_type), logger = logger)
                  }
                #}
                
              }
              
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteure|rapporteur|representative")) ) {
                intervention_type <- NA
                speaker_type <- "Rapporteur"
              }
              
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "member")) ) {
                speaker_type <- intervention_type
                intervention_type <- NA
              }
              
              if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "winner|president,\\s|president-in-office|chancellor|president\\sof|minister|his\\sholiness|secretary|king|ombudsman|chair\\sof\\sthe") ) {
                speaker_type <- intervention_type
                intervention_type <- "Speech"
              }
              
              if ( !is.na(intervention_type) && tolower(intervention_type) == "president" ) {
                intervention_type <- "Moderation"
                speaker_type <- "President"
              } 
              
              if ( is.na(speaker_type) ) speaker_type <- "Member of the Commission"
              
            } #if (is.na(intervention_type)) {
            
            if ( !is.null(intervention_type) && !is.na(intervention_type) ) {
              intervention_type <- gsub("\\(", "", intervention_type)
              intervention_type <- gsub("\\)", "", intervention_type)
              intervention_type <- gsub("\"", "", intervention_type)
              intervention_type <- gsub("\\\\u2012", "", intervention_type)
              intervention_type <- gsub("\\\\ U2012", "", intervention_type)
              intervention_type <- gsub("blue-card", "Blue Card", intervention_type)
              intervention_type <- gsub("Blue-Card", "Blue Card", intervention_type)
              intervention_type <- gsub("Blue-card", "Blue Card", intervention_type)
              intervention_type <- stringr::str_squish(intervention_type)
              intervention_type <- trimws(intervention_type)
              intervention_type <- stringr::str_to_title(intervention_type)
            }
            
            if ( !is.null(speaker_type) && !is.na(speaker_type) ) {
              if (tolower(speaker_type) == "member of the commission") speaker_type <- "Member of the Commission"
            }
            
            intervention_text <- paste(intervention_text, XML::xmlValue(intervention_node[[l]]), sep = " ")
          } #for (l in which(names(intervention_node) == "PARA"))
          
          if (stringr::str_detect(intervention_text, "\\. – ")) {
            #intervention_text <- stringr::str_split(intervention_text, "\\. – ")[[1]][2]
            intervention_text <- gsub("\\. – ", "", intervention_text)
          }
          
          if (stringr::str_detect(intervention_text, "^– ")) {
            #intervention_text <- stringr::str_split(intervention_text, "^– ")[[1]][2]
            intervention_text <- gsub("^– ", "", intervention_text)
          }
          
          intervention_text <- trimws(intervention_text, "left")
          intervention_text <- gsub("^– ", "", intervention_text) 
          intervention_word_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), word, txt, token="words",format="text"))
          intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
          intervention_paragraph_count <- length(which(names(intervention_node) == "PARA"))
          
          # Translation
          if (grepl("\\\\", intervention_text)) intervention_text <- gsub("\\\\"," ", intervention_text)
          intervention_text <- gsub("^NA\n\n", "", intervention_text)
          intervention_text <- gsub("^\n\n", "", intervention_text)
          
          
          
          if (opt$translate) {
            if (textcat::textcat(intervention_text) != "english") {
              clessnverse::logit(scriptname = scriptname, message = paste("translating intervention_text", substr(intervention_text, 1, 50)), logger = logger)
              intervention_translation <- clessnverse::translateText(text=intervention_text, engine="azure", target_lang="en", fake=!opt$translate)
              intervention_translated_text <- intervention_translation[2]
              clessnverse::logit(scriptname = scriptname, message = paste("translated intervention_text", substr(intervention_translated_text, 1, 50)), logger = logger)
            } 
          } else {
            intervention_translation <- NA
            intervention_translated_text <- NA
          }
          
          if (!is.na(speaker_type)  && speaker_type == "breaking point") speaker_type <- NA
          if (!is.na(speaker_party) && stringr::str_detect(clessnverse::rm_accent(tolower(speaker_party)), "independent|independant")) speaker_party <- "Independent"
          if (is.null(intervention_translated_text)) intervention_translated_text <- NA
          
          # commit 
          row_to_commit <- list(
              .schema = opt$schema, .lake_item_format = "xml",
              .lake_item_path = lake_item$path, .lake_item_key = lake_item$key,
              .url = lake_item$metadata$source,
              event_id = event_id,  
              event_date = event_date, 
              event_start_time = event_start_time, event_end_time = event_end_time, 
              event_title = event_title, 
              subject_of_business_id = chapter_number, subject_of_business_title = chapter_title, 
              president_name = president_name,
              intervention_id = paste(gsub("dp", "", event_id),intervention_seqnum,sep=''),
              intervention_seq_num = intervention_seqnum,
              intervention_header = header_text, intervention_header_en = header_text_en,
              intervention_lang = intervention_lang,
              intervention_text = intervention_text, intervention_text_en = intervention_text_en)
          
          # commit intervention to the Hub
          clessnverse::logit(scriptname, paste("committing row", paste(gsub("dp", "", event_id),intervention_seqnum,sep='')), logger)

          if (opt$backend == "hub") {
            nb_attempts <- 0
            write_success <- FALSE
            while (!write_success && nb_attempts < 20) {
              tryCatch(
                {
                  r <- clessnverse::commit_warehouse_row(
                    table=wh_table,
                    key=paste(event_id,"-", intervention_seqnum, "-", opt$schema, sep=""),
                    row=row_to_commit,
                    refresh_data=opt$refresh_data,
                    credentials=credentials 
                  )

                  if (!is.null(r) && !is.na(r) && r) write_success <- TRUE 
                },

                error=function(e) {
                  clessnverse::logit(scriptname, paste("error rwiting to hub:", e$message, "on attempt", nb_attempts, ". sleeping 30 seconds"), logger)
                  Sys.sleep(30)
                },

                finally= {
                  nb_attempts <- nb_attempts + 1
                }
              )
            }
          } else {
            my_df <<- my_df %>% rbind(as.data.frame(row_to_commit))
          }          
          
          intervention_count <- intervention_count + 1
          
          current_speaker_full_name <- speaker_full_name
          
            # previous_header_text <- header_text
            # intervention_text <- NA
            # intervention_text_en <- NA
            # intervention_lang <- NA
  
      } #for (k in which(chapter_nodes_list == "INTERVENTION"))
      
    } #if ( "INTERVENTION" %in% chapter_nodes_list )
  
  } #for (j in 1:core_xml_nbchapters)
} #</function process_debate_xml>



process_debate_html <- function(lake_item, xml_core) {
  event_url <- lake_item$metadata$source
  event_id <- lake_item$key

  # Get the length of all branches of the XML document
  core_xml_chapters <- XML::getNodeSet(xml_core[[5]], ".//table[@class='doc_box_header']")
  core_xml_nbchapters <- length(core_xml_chapters)
  
  if ( core_xml_nbchapters == 0 ) {
    # Get the length of all branches of the XML document
    core_xml_chapters <- XML::getNodeSet(xml_core[[6]], ".//table[@class='doc_box_header']")
    core_xml_nbchapters <- length(core_xml_chapters)
  }

    
  ###############################
  # Global level columns
  event_source_type <- "Débats et vidéos | Plénière | Parlement européen"
  event_date <- lake_item$metadata$event_date
  
  for (start_time_index in 2:core_xml_nbchapters) {
    if ( TRUE %in% grepl("Video\u00a0of\u00a0the\u00a0speeches", XML::xmlValue(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a"))) ||
         TRUE %in% grepl("Video of the speeches", XML::xmlValue(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a")))) {
      break
    }
  }
  
  event_start_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a")[[3]])[["href"]]
  event_start_time <- stringr::str_match(event_start_time, "\\d{2}:\\d{2}:\\d{2}")
  event_start_time <- event_start_time[1]
  
  if ( is.na(event_start_time) ) {
    event_start_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[start_time_index]][["tr"]][["td"]], ".//a")[[5]])[["href"]]
    event_start_time <- stringr::str_match(event_start_time, "\\d{2}:\\d{2}:\\d{2}")
    event_start_time <- event_start_time[1]
  }
    
  
  last_chapter <- grep("Closure of the sitting", lapply(core_xml_chapters, XML::xmlValue))[2]
  
  if (is.na(last_chapter)) last_chapter <- grep("Interruption of the sitting", lapply(core_xml_chapters, XML::xmlValue))[2]
  if (is.na(last_chapter)) last_chapter <- grep("End of session", lapply(core_xml_chapters, XML::xmlValue))[2]
  if (is.na(last_chapter)) last_chapter <- grep("Adjournment of the session", lapply(core_xml_chapters, XML::xmlValue))[2]
  
  event_end_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[last_chapter]][["tr"]][["td"]], ".//a")[[3]])
  
  if ("href" %in% names(event_end_time)) {
    event_end_time <- XML::xmlAttrs(XML::getNodeSet(core_xml_chapters[[last_chapter]][["tr"]][["td"]], ".//a")[[3]])[["href"]]
    event_end_time <- stringr::str_match(event_end_time, "\\d{2}:\\d{2}:\\d{2}")
    event_end_time <- event_end_time[1]
  } else {
    event_end_time <- NA
  }
  
  if ( is.na(event_end_time) ) {
    event_end_time <- XML::getNodeSet(core_xml_chapters[[last_chapter]][["tr"]][["td"]], ".//p")
    event_end_time <- XML::xmlValue(event_end_time)
    event_end_time_index <- which(!is.na(stringr::str_match(event_end_time, "\\d{2}\\.\\d{2}")))
    
    if (length(event_end_time_index)>0) {
      event_end_time <- stringr::str_match(event_end_time, "\\d{2}\\.\\d{2}")[[event_end_time_index]]
      event_end_time <- stringr::str_replace(event_end_time, "\\.", ":") 
      event_end_time <- paste(event_end_time, ":00", sep='')
    }
  }  
  
  event_title <- XML::getNodeSet(xml_core, ".//td[@class='doc_title']")
  event_title <- if (!is.null(event_title) && length(event_title) > 0) event_title[[1]] else NULL
  event_title <- if (!is.null(event_title)) XML::xmlValue(event_title) else NA

  intervention_seqnum <- 1

  ####################################
  # Intervention level columns
  intervention_word_count <- NA
  intervention_sentence_count <- NA
  intervention_paragraph_count <- NA
  intervention_text <- NA
  intervention_text_en <- NA
  intervention_lang <- NA
  president_name <- NA
  previous_header_text <- NA
  header_text <- NA

  pattern1 <- " – | − | - | — | - | - | – | ‒ | – | – | – | – "
  pattern2 <- " –| −| -| —| -| -| –| ‒| –| –| –| –"
  pattern3 <- "– |− |- |— |- |- |– |‒ |– |– |– |– "
  pattern4 <- "– |− |- |— |- |- |– |‒ |– |– |– |– "
  pattern5 <- "\\. –|\\. −|\\. -|\\. —|\\. -|\\. -|\\. –|\\. ‒|\\. –|\\. –|\\. –|\\. –"
  pattern6 <- "\\.–|\\.−|\\.-|\\.—|\\.-|\\.-|\\.–|\\.‒|\\.–|\\.–|\\.–|\\.–"
  pattern7 <- "\\)\\.\\s"

  pattern <- paste(pattern1, pattern2, pattern3, pattern4, pattern5, pattern6, pattern7, sep = "|")
  pattern_start <- "^–|^−|^-|^—|^-|^-|^–|^‒|^–|^–|^–|^–"
  pattern_end <- "–$|−$|-$|—$|-$|-$|–$|‒$|–$|–$|–$|–$"


  for (j in 2:core_xml_nbchapters) {
    # We start at 2 because the first one is the TOC

    # New chapter
    chapter_node <- core_xml_chapters[[j]][["tr"]][["td"]]
    
    title_node <- XML::getNodeSet(chapter_node, ".//td[@class='doc_title']")
    chapter_title <- XML::xmlValue(title_node)
    chapter_title <- stringr::str_remove(chapter_title, "\n")
    chapter_title <- trimws(chapter_title)
    chapter_number <- stringr::str_match(chapter_title, "^(0|[1-9][0-9]*)\\.(.*)")[[2]]
    chapter_title <- stringr::str_match(chapter_title, "^(0|[1-9][0-9]*)\\.(.*)")[[3]] 
    chapter_title <- trimws(chapter_title)

    chapter_nodes_list <- names(chapter_node)
    
    if ( "a" %in% chapter_nodes_list ) {
      # There is potentially one or more interventions in this section.
      # From potentially multiple speakers
      
      # We have to loop through every intervention
      chapter_nodes_list <- which(chapter_nodes_list == "a")
      
      for (k in 1:length(chapter_nodes_list)) {

        #Find out if there is an intervention in this section
        chapter_container_node <- chapter_node[chapter_nodes_list[[k]]+2]
        content_node <- XML::getNodeSet(chapter_container_node$table, ".//p[@class='contents']")
        
        if (length(content_node) == 0) next

        for (l in 1:length(content_node)) {

          content_type <- XML::xmlAttrs(content_node[[l]][[1]])
          next_content_type <- if (l < length(content_node)) XML::xmlAttrs(content_node[[l+1]][[1]]) else NULL

          if ( !is.null(content_type) && content_type[[1]] == "bold" && length(XML::xmlAttrs(content_node[[l]])) == 2 &&  XML::xmlAttrs(content_node[[l]])[[2]] == "center" ) { 
            # Prolocolar node (change of president)
            node_value <- XML::xmlValue(content_node[[l]])

            #if (grepl(tolower("ANTONIO TAJANI"), tolower(node_value))) stop()

            if ( !is.null(node_value) && !is.na(node_value) && nchar(node_value) > 0 && detect_president_change(node_value) ) {
                president_name <- extract_president_name(node_value)
            }
          }
          
          first_parag <- NA
          
          # We have a new speaker taking the stand - first thing : get his info and then his first paragraph
          if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type ) {
            intervention_word_count <- NA
            intervention_sentence_count <- NA
            intervention_paragraph_count <- NA
            intervention_text <- NA
            
            header_text <- NA

            hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,250), pattern)[1]

            if (is.na(hyphen_pos)) {
              hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,250), "\\)\\. ")[1] + 1
              header_text <- previous_header_text
            } else {
              header_text <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,hyphen_pos)))
              header_text <- gsub("\\.", "", header_text)
              header_text <- gsub('^(\u00a0*)','', header_text)
              header_text <- gsub("\\s+", " ", stringr::str_trim(header_text))
              header_text <- gsub(pattern_start, "", header_text)
              header_text <- gsub(pattern_end, "", header_text)
              headet_text <- trimws(header_text)
            }

            #if (grepl(tolower("Ana Gomes"), tolower(header_text)) && intervention_seqnum > 40) stop()
            
            if ( !is.na(header_text) && nchar(header_text) == 0 ) next

            key <- paste(event_id, "-", intervention_seqnum, "-", opt$schema, sep = "")
            
            if (!opt$refresh_data) {
              tryCatch(
                {
                  clessnverse::logit(scriptname, paste("checking if ", key," already exists", sep=""), logger)
                  item_check <- hublot::filter_table_items(
                    table_name = paste("clhub_tables_warehouse_", wh_table, sep=''), 
                    filter = list(key = key),
                    credentials = credentials
                  )

                  if (length(item_check$results) == 1) {
                    clessnverse::logit(
                      scriptname, 
                      paste("item ", key, " already exists, skipping...", sep=""),
                      logger
                    )

                    intervention_seqnum <- intervention_seqnum + 1
                    next
                  }
                },
                error=function(e) {},
                finally={}
              ) # tryCatch
            } #if (!opt$refresh_data)

            # Now strip the intervention itself                          
            if (!is.na(hyphen_pos)) {
              first_parag <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),hyphen_pos+2,nchar(XML::xmlValue(content_node[[l]])))))
              first_parag <- gsub("\\.", "", first_parag)
              first_parag <- gsub('^(\u00a0*)','', first_parag)
              first_parag <- gsub(pattern_start, "", first_parag)
              first_parag <- gsub(pattern_end, "", first_parag)
            } else {
              first_parag <- stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)–(\\s*)(.*)$")))[3]
              if (is.na(first_parag)) first_parag <- stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)−(\\s*)(.*)$")))[3]
              first_parag <- gsub(pattern_start, "", first_parag)
              first_parag <- gsub(pattern_end, "", first_parag)              
            }
            
            
            
            if (length(first_parag) == 0) first_parag <- NA
            
            intervention_text <- NA              
          } else {
            # Same speaker
            key <- paste(event_id, "-", intervention_seqnum, "-", opt$schema, sep = "")

            if (!opt$refresh_data) {
              tryCatch(
                {
                  clessnverse::logit(scriptname, paste("checking if ", key," already exists", sep-""), logger)
                  item_check <<- clessnhub::get_item('agoraplus_interventions', key)

                  if (!is.null(item_check)) {
                    clessnverse::logit(
                      scriptname, 
                      paste("item ", key, " already exists, skipping...", sep=""),
                      logger
                    )

                    intervention_seqnum <- intervention_seqnum + 1
                    next
                  }
                },
                error=function(e) {},
                finally={}
              )
            }
          }# if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type )
          
          # Here we have an intervention - it is either  a new intervention (if first_parag is not NA) or the continuation of the same intervention
          if ( is.null(content_type) || !is.na(first_parag) ) {

            if ( !is.na(first_parag) ) {
              #New
              intervention_text <- first_parag
            } else {
              #Continuation
              #intervention_text <- if (is.na(intervention_text)) intervention_text else paste(intervention_text, XML::xmlValue(content_node[[l]][[1]]), sep="\n\n")
              intervention_text <- if (is.na(intervention_text)) intervention_text else paste(intervention_text, XML::xmlValue(content_node[[l]]), sep="\n\n")
            }
            
            if ( !is.na(intervention_text) && intervention_text == "\n\n " )  intervention_text <- NA
            
            first_parag <- NA
          }            
          
          # Look at the next paragraph to see if it is a new intervention
          if ( "/doceo/data/img/arrow_summary.gif" %in% next_content_type || l == length(content_node) && !is.na(intervention_text) && nchar(intervention_text) > 0 ) {
            # next is new              
            intervention_text <- trimws(intervention_text, "left")
            intervention_word_count <- nrow(tidytext::unnest_tokens(tibble::tibble(txt=intervention_text), word, txt, token="words",format="text"))
            intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble::tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
            intervention_paragraph_count <- stringr::str_count(intervention_text, "\n\n") + 1

            # Clean
            if (grepl("\\\\", intervention_text)) intervention_text <- gsub("\\\\"," ", intervention_text)
            intervention_text <- gsub("^NA\n\n", "", intervention_text)
            intervention_text <- gsub("^\n\n", "", intervention_text)

            if (grepl("\\\\", header_text)) header_text <- gsub("\\\\"," ", header_text)
            header_text <- gsub("^NA\n\n", "", header_text)
            header_text <- gsub("^\n\n", "", header_text)

            # Translate
            if (opt$translate) {
              intervention_lang <- clessnverse::detect_language("fastText", intervention_text)
              
              if (!is.na(intervention_lang) && intervention_lang != "en") {
                tryCatch(
                  {
                    intervention_text_en <- clessnverse::translate_text(
                      text = clntxt(intervention_text), 
                      engine = "deeptranslate",
                      source_lang = intervention_lang, 
                      target_lang = "en", 
                      translate = TRUE
                    )
                  },
                  error = function(e) {
                    clessnverse::logit(scriptname, "there was a warning with the deeptranslate_api : text to translate + error below:", logger)
                    status <<- 2
                    warning("there was an error with the deeptranslate_api : see logs")
                    clessnverse::logit(scriptname, clntxt(intervention_text), logger)
                    clessnverse::logit(scriptname, e$message, logger)
                    intervention_text_en <<- clessnverse::translate_text(
                      text = clntxt(intervention_text), 
                      engine = "azure",
                      source_lang = intervention_lang, 
                      target_lang = "en", 
                      translate = TRUE
                    )

                    if(!is.null(intervention_text_en) && !is.na(intervention_text_en) && nchar(intervention_text_en)) {
                      clessnverse::logit(scriptname, "manage to recover the error.  translation below:", logger)
                      clessnverse::logit(scriptname, intervention_text_en, logger)
                    } else {
                      clessnverse::logit(scriptname, "unable to recover translation error.  must stop...", logger)
                      status <<- 1
                      stop("unable to recover translation error.  must stop...")
                    }
                  },
                  finally={}
                )
              } else {
                if (!is.na(intervention_text) && intervention_lang == "en") intervention_text_en <- intervention_text
              }

              header_text_lang <- clessnverse::detect_language("fastText", header_text)

              if (!is.na(header_text_lang) && header_text_lang != "en") {
                tryCatch(
                  {
                    header_text_en <- clessnverse::translate_text(
                      text = clntxt(header_text), 
                      engine = "deeptranslate",
                      source_lang = header_text_lang, 
                      target_lang = "en", 
                      translate = TRUE
                    )
                  },
                  error = function(e) {
                    clessnverse::logit(scriptname, "there was a warning with the deeptranslate_api: text to translate + error below:", logger)
                    status <<- 2
                    warning("there was an error with the deeptranslate_api : see logs")
                    clessnverse::logit(scriptname, clntxt(header_text), logger)
                    clessnverse::logit(scriptname, e$message, logger)
                    header_text_en <<- clessnverse::translate_text(
                      text = clntxt(header_text), 
                      engine = "azure",
                      source_lang = header_text_lang, 
                      target_lang = "en", 
                      translate = TRUE
                    )

                    if(!is.null(header_text_en) && !is.na(header_text_en) && nchar(header_text_en)) {
                      clessnverse::logit(scriptname, "manage to recover the error.  translation below:", logger)
                      clessnverse::logit(scriptname, header_text_en, logger)
                    } else {
                      clessnverse::logit(scriptname, "unable to recover translation error.  must stop...", logger)
                      status <<- 1
                      stop("unable to recover translation error.  must stop...")
                    }

                  },
                  finally={}
                )
              } else {
                if (!is.na(header_text) && header_text_lang == "en") header_text_en <- header_text
              }

            } else {
              intervention_lang = "xx"
              intervention_text_en = "not translated because opt$translate = FALSE"
            }
                          
            row_to_commit <- list(
              .schema = opt$schema, .lake_item_format = "html",
              .lake_item_path = lake_item$path, .lake_item_key = lake_item$key,
              .url = lake_item$metadata$source,
              event_id = event_id,  
              event_date = event_date, 
              event_start_time = event_start_time, event_end_time = event_end_time, 
              event_title = event_title, 
              subject_of_business_id = chapter_number, subject_of_business_title = chapter_title, 
              president_name = president_name,
              intervention_id = paste(gsub("dp", "", event_id),intervention_seqnum,sep=''),
              intervention_seq_num = intervention_seqnum,
              intervention_header = header_text, intervention_header_en = header_text_en,
              intervention_lang = intervention_lang,
              intervention_text = intervention_text, intervention_text_en = intervention_text_en
            )

            # commit intervention to the Hub
            clessnverse::logit(scriptname, paste("committing row", paste(gsub("dp", "", event_id),intervention_seqnum,sep='')), logger)

            if (opt$backend == "hub") {
              nb_attempts <- 0
              write_success <- FALSE
              while (!write_success && nb_attempts < 20) {
                tryCatch(
                  {
                    r <- clessnverse::commit_warehouse_row(
                      table=wh_table,
                      key=paste(event_id,"-", intervention_seqnum, "-", opt$schema, sep=""),
                      row=row_to_commit,
                      refresh_data=opt$refresh_data,
                      credentials=credentials 
                    )

                    if (!is.null(r) && !is.na(r) && r) write_success <- TRUE 
                  },

                  error=function(e) {
                    clessnverse::logit(scriptname, paste("error rwiting to hub:", e$message, "on attempt", nb_attempts, ". sleeping 30 seconds"), logger)
                    Sys.sleep(30)
                  },

                  finally= {
                    nb_attempts <- nb_attempts + 1
                  }
                )
              }
            } else {
              my_df <<- my_df %>% rbind(as.data.frame(row_to_commit))
            }

            nb_interventions <<- nb_interventions + 1
            clessnverse::logit(scriptname, "committing row done", logger)
            intervention_seqnum <- intervention_seqnum + 1

            previous_header_text <- header_text
            intervention_text <- NA
            intervention_text_en <- NA
            intervention_lang <- NA

          } else {
            # Next is same speaker or a new president?
          } #if ( "/doceo/data/img/arrow_summary.gif" %in% next_content_type || l == length(content_node) && !is.na(intervention_text) ) {
        } #for (l in 1:length(content_node))
      } # for (k in 1:length(chapter_nodes_list))
    } # if ("a" %in% chapter_nodes_list)
  } #for (j in 2:core_xml_nbchapters)
} #</function process_debate_html>



strip_and_load_debate <- function(lake_item) {
  clessnverse::logit(
    scriptname, 
    paste(
      "starting to load debate.  key=", lake_item$key,
      " at url=", lake_item$file,
      " from source=", lake_item$metadata$source,
      " event_date=", lake_item$metadata$event_date,
      " parliament_number=", lake_item$metadata$parliament_number,
      sep = ''
    ), 
    logger
  )

  i_attempt <- 1
  get_success <- FALSE

  while (!get_success && i_attempt < 20) {
    tryCatch(
      {
          clessnverse::logit(scriptname, paste("getting", lake_item$file), logger)
          r <- httr::GET(lake_item$file)
          nb_lake_items <<- nb_lake_items + 1
          if (r$status == 200) get_success <- TRUE
      },
      error = function(e) {
        msg <- paste("could not get", lake_item$file, "from datalake item", lake_item$key, "in path", datalake_path, ". error msg: ", e$message)
        status <<- 1 # this means error
        if (final_message == "") {
           final_message <<- msg
        } else {
           final_message <<- paste(final_message, "\n", msg)
        }
      }, 
      warning = function(w) {
        msg <- paste("A warning occured while trying to HTTP GET", lake_item$file, "warning msg: ", w$message)
        status <<- 2 # this means a warning
        if (final_message == "") {
           final_message <<- msg
        } else {
           final_message <<- paste(final_message, "\n", msg)
        }
      },
      finally = {
        i_attempt <- i_attempt + 1
      }
    )#</tryCatch>
  }#</while>

  # Here we have html or xml content in r
  if (r$header$`Content-Type` == "text/html") {
    clessnverse::logit(scriptname, paste("extracting nodes from lake item html file", lake_item$key), logger)
    doc_content <- httr::content(r, encoding = "UTF-8")
    parsed_xml <- XML::htmlParse(doc_content)
    xml_root <- XML::xmlRoot(parsed_xml)
    xml_head <- xml_root[[1]]
    xml_core <- xml_root[[2]]
    clessnverse::logit(scriptname, paste("properly extracted nodes from lake item", lake_item$key), logger)
    process_debate_html(lake_item, xml_core)
    nb_debates <<- nb_debates + 1
  } else {
      if (r$header$`Content-Type` == "application/xml") {
      clessnverse::logit(scriptname, paste("extracting nodes from lake item xml file", lake_item$key), logger)
      doc_content <- httr::content(r, encoding = "UTF-8")

      doc_xml <- XML::xmlTreeParse(doc_content, useInternalNodes = FALSE)
      top_xml <- XML::xmlRoot(doc_xml)
      xml_head <- top_xml[[1]]
      xml_core <- top_xml[[2]]

      # parsed_xml <- XML::htmlParse(doc_content)
      # xml_root <- XML::xmlRoot(parsed_xml)
      # xml_head <- xml_root[[1]]
      # xml_core <- xml_root[[2]]
      clessnverse::logit(scriptname, paste("properly extracted nodes from lake item", lake_item$key), logger)
      process_debate_xml(lake_item, xml_core)
      nb_debates <<- nb_debates + 1
    }
    stop(paste("content-type", r$header$`Content-Type`, "not supported for lake item", lake_item$key, "in file", lake_item$file))
  }

} #</my_function>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    
  clessnverse::logit(scriptname, "starting main function", logger)
  clessnverse::logit(scriptname, paste("retrieving debates trasnscriptions from datelake with path = ", datalake_path, sep=''), logger)

  clessnverse::logit(scriptname, "parsing options", logger)
  if(length(opt$method) == 1 && grepl(",", opt$method)) {
    # The option parameter given to the script is multivalued - parse
    opt$method <- trimws(strsplit(opt$method, ",")[[1]])
  }

  if (opt$method[[1]] == "date_range") {
    my_filter <- list(
      metadata__event_date__gte = opt$method[[2]],
      metadata__event_date__lte = opt$method[[3]]
    )
  } else {
    my_filter <- list()
  }

  if (opt$backend != "hub") my_df <<- data.frame()

  r <- hublot::filter_lake_items(
    credentials, 
    c( 
      list(path=datalake_path),
      my_filter
    )
  )

  clessnverse::logit(scriptname, paste("found", length(r$results), "lake items"), logger)

  for (lake_item in r$results) {
 
    current_lake_item <- hublot::retrieve_lake_item(lake_item$id, credentials)
    strip_and_load_debate(current_lake_item)

  }

  clessnverse::logit(scriptname, "ending main function", logger)  
}



tryCatch( 
  withCallingHandlers(
  {
    `%>%` <- dplyr::`%>%`

    Sys.setlocale("LC_TIME", "fr_CA.utf8")

    
    president <<- tolower(unique(c(
      "president","президент","predsjednik","başkan","prezident","formand","presidentti","président",
      "präsident","Πρόεδρος","elnök","preside","uachtarán","Presidente","prezidents","prezidentas",
      "presidint","prezydent","presedinte","predsednik","presidentea","presidente","chairman","chair",
      "présidente","Präsident","President", "Preşedinte", "Preşedintele", "Presedintele", "in the chair",
      "Mistopredseda",  "Präsidentin", "Presedintia", "Speaker"
      )))

    vicepresident <<- tolower(unique(c(
      "vice-président","вицепрезидент","dopredsjednik","Başkan Vekili","víceprezident","vicepræsident",
      "asepresident","varapresidentti","Vizepräsident","αντιπρόεδρος","alelnök","Leasuachtarán",
      "vicepresidente","viceprezidents","viceprezidentas","Vizepresident","Viċi President","onderdirecteur",
      "fise-presidint","wiceprezydent","vice-presidente","vice-preşedinte","podpredsedníčka","podpredsednik",
      "lehendakariordea","vicepresident","vice President","vice-president","vice-présidente", "Vicepreşedinte",
      "Wiceprzewodniczacy"
    )))


    president_of_the_commission <<- tolower(unique(c(
      "president de la commission","президент на комисията","predsjednik komisije",
      "komisyon başkanı","prezident de la Commission","formand for kommissionen",
      "komisjoni president","komission puheenjohtaja","président de la commission",
      "Präsident der Kommission","πρόεδρος της επιτροπής","bizottság elnöke",
      "Uachtarán an choimisiúin","presidente della commissione","komisijas prezidents",
      "komisijos pirmininkas","President vun der Kommissioun","voorzitter van de commissie",
      "foarsitter fan de kommisje","przewodniczący komisji","presidente da comissão",
      "preşedinte de la commission","prezident komisie","predsednik komisije",
      "batzordeko presidentea","president de la comissió","presidente da comisión",
      "presidente de la comisión","ordförande för kommissionen","chairman of the commission",
      "president of the commission",
      "председател на комисията","predsjednik odbora","komite başkanı",
      "předseda výboru","formand for udvalget","komisjoni esimees",
      "komitean puheenjohtaja","présidente de la commission","Vorsitzender des Ausschusses",
      "cathaoirleach an choiste","presidente del comitato","komitejas priekšsēdētājs",
      "komiteto pirmininkas","President vum Comité","president tal-kumitat",
      "preşedintele comitetului","predseda výboru","predsednik odbora",
      "batzordeko lehendakaria","president del comitè","presidente do comité",
      "presidente del comité","nämndens ordförande"    
    )))


    member_of_the_commission <- tolower(unique(c(
      "committee member","член на Комисията","član Odbora","Komite Üyesi",    
      "člen komise","udvalgsmedlem","komisjoni liige","komitean jäsen",
      "membre de la commission","Ausschussmitglied","μέλος επιτροπής","bizottsági tag",
      "ball coiste","membro del Comitato","komitejas loceklis","komiteto narys",
      "Comitésmember","membru tal-kumitat","Commissie lid","kommisjelid",
      "członek Komisji","membro do Comitê","membru al Comitetului","člen výboru",
      "član komisije","batzordekidea","membre del comitè","membro do comité",
      "miembro del Comité","kommittéledamot","committee member","member of the commission"
    )))


    president_of_the_committee <<- tolower(unique(c(
      "komite başkanı","předseda výboru","formand for udvalget",
      "komisjoni esimees","komitean puheenjohtaja","président du comité",
      "vorsitzender des ausschusses","πρόεδρος της επιτροπής","a bizottság elnöke",
      "cathaoirleach an choiste","presidente del comitato","komitejas priekšsēdētājs",
      "komiteto pirmininkas","president vum comité","president tal-kumitat",
      "voorzitter van de commissie","foarsitter fan de kommisje","przewodniczący komitetu",
      "presidente do comitê","președinte al comitetului","predseda výboru",
      "predsednik odbora","batzordeburua","president del comitè",
      "presidente do comité","presidente del comité","utskottets ordförande",
      "chair of the committee","president of the committee", "председател на комисията","predsjednik odbora",
      "komite başkanı","předseda výboru","formand for udvalget",
      "présidente du comité","πρόεδρος της επιτροπής","bizottság elnöke",
      "cathaoirleach an choiste","presidente del comitato","komitejas priekšsēdētājs",
      "komiteto pirmininkas","president tal-kumitat","voorzitter van de commissie",
      "foarsitter fan de kommisje","przewodniczący komisji","presidente da comissão",
      "preşedintele comitetului","predsednik odbora","batzordeko burua",
      "president de la comissió","presidente do comité","presidente del comité",
      "ordförande i kommittén"
    )))

    presidency_of_the_hon <<- tolower(unique(c(
      "presidency of the Hon","председателството на Hon","predsjedništvo č","Cumhurbaşkanlığı","předsednictví Hon",
      "præsidentskab for Hon","presidentuuri au","kunniapuheenjohtaja","présidence de l'honorable","Präsidentschaft des Hon",
      "προεδρία του Ον","elnöksége a Hon","uachtaránacht an Oinigh","presidenza dell'On","presidenza dell’On","prezidentūra god",
      "prezidentu gerb","Présidence vum Hon","presidenza tal-Onor","voorzitterschap van de Hon","presidintskip fan de Hon",
      "prezydentura Hon","presidência do Exmo","președinția Onorului","predsedníctvo Hon","predsedstvo Hon",
      "presidentetza Hon","presidència de l'Excm","presidencia do Excmo","presidencia del Excmo","ordförandeskapet för Hon"
    )))

    presidency <<- tolower(unique(c(
      "presidency","президентство","predsjedništvo","başkanlık","předsednictví","formandskab","eesistumine","puheenjohtajuus","présidence",
      "Präsidentschaft","προεδρία","elnökség","uachtaránacht","presidenza","prezidentūra","prezidentūra","Présidence","presidenza", "vorsitz",
      "voorzitterschap","presidintskip","przewodnictwo","presidência","preşedinţie","predsedníctvo","predsedstvo","lehendakaritza","presidència",
      "presidencia","presidencia","ordförandeskap",
      "presidency of","председателство на","predsjedništvo od","başkanlığı","předsednictví",
      "formandskab for","aasta presidentuur","puheenjohtajakausi","présidence de","Präsidentschaft von",
      "προεδρία του","elnöksége","uachtaránacht na","presidenza di","gada prezidentūra",
      "pirmininkavimas","Présidence vun","presidenza ta","voorzitterschap van","presidintskip fan",
      "prezydencja","presidência de","preşedinţia lui","predsedníctvo","predsedovanje",
      "ren presidentetza","presidència de","presidencia de","presidencia de","ordförandeskapet för",
      "presidency of mrs","председателство на г-жа","predsjedništvo gđe","başkanlığı hanım",
      "předsednictví mrs","formandskab for mrs","presidendiks pr","rouvan puheenjohtajakausi",
      "présidence de mme","präsidentschaft von mrs","προεδρία της κας","elnöksége mrs",
      "uachtaránacht mrs","presidenza della sig","kundzes prezidentūra","prezidentūra p",
      "présidence vun mme","presidenza tas-sinjura","voorzitterschap van mevr","presidintskip fan mrs",
      "prezydentura p","presidência da sra","președinția doamnei","predsedníctvo p",
      "predsedovanje ge","andrearen presidentetza","presidència de la sra","presidencia da sra",
      "presidencia de la sra","presidentskapet för mrs",
      "chair of mr","presidency of mr","председател на mr","stolica gosp","başkanı sn",
      "křeslo mr","formand for mr","juhataja hr","puheenjohtaja mr",
      "présidence de monsieur","vorsitzende von hr","πρόεδρος του κ","elnök úr",
      "cathaoirleach mr","presidente del sig","priekšsēdētājs mr","pirmininkas p",
      "president vum mr","president tas-sur","stoel van dhr","foarsitter fan mr",
      "krzesło p","cadeira do sr","scaunul dlui","predseda p",
      "predsednik g","jaunaren burua","president del sr","presidente do sr",
      "silla del sr","ordförande för mr"   
    )))


    wh_table <<- "agoraplus_european_parliament"
    
    status <<- 0
    final_message <<- ""
    nb_interventions <<- 0
    nb_debates <<- 0
    nb_lake_items <<- 0

    datalake_path <<- "agoraplus/european_parliament"
    
    if (!exists("scriptname")) scriptname <<- "l_eu_parliament_plenary"

    # valid options for this script are
    #    log_output = c("file","console","hub")
    #    loading_method = c("date_range", "start_date", "end_date") | "all" | c("parliament_num", parl_num) | c("year", year) | "all"
    #    translate = "TRUE" | "FALSE"
    #    refresh_data = "TRUE" | "FALSE"
    #    
    #    you can use log_output = c("console") to debug your script if you want
    #    but set it to c("file") before putting in automated containerized production

    # opt <<- list(
    #  backend = "hub",
    #  log_output = c("console"),
    #  method = c("date_range", "2016-01-01", "2016-12-31"),
    #  schema = "test",
    #  refresh_data = TRUE,
    #  translate = TRUE
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
      Sys.getenv("HUB3_PASSWORD"))
    
    # or connect to hub2
    clessnhub::login(
       Sys.getenv("HUB_USERNAME"),
       Sys.getenv("HUB_PASSWORD"),
       Sys.getenv("HUB_URL"))

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    clessnverse::logit(scriptname, "getting people", logger)
    df_people <- clessnverse::get_warehouse_table(
      table_name = 'people',
      data_filter = list(data__institution = "European Parliament"),
      credentials = credentials
    )

    
    main()
  },

  warning = function(w) {
    print(w) 
    if (final_message == "") {
      final_message <<- w$message
     } else {
      final_message <<- paste(final_message, "\n", w$message, sep="")
     }
     
    status <<- 2
  }),
    
  error = function(e) {
    print(e)
    if (final_message == "") {
      final_message <<- e$message
     } else {
      final_message <<- paste(final_message, "\n", e$message, sep="")
     }

    status <<- 1
  },

  finally={
    clessnverse::logit(scriptname, final_message, logger)

    clessnverse::logit(scriptname, 
      paste(
          nb_lake_items, 
          "lake items were found",
          nb_debates,
          "debates were loaded in the data warehouse, totalling",
          nb_interventions,
          "interventions"
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
