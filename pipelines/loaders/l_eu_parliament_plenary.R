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
########################      Functions and Globals      ######################
###############################################################################


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
  # Columns of the simple dataset
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
  
  
  # president_name  <- XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//p")[[1]])
  # president_title <- tail(strsplit(XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//p")[[1]]), split = " ")[[1]], 1)
  # president_name  <- stringr::str_match(tolower(president_name), "^(.*)(\\.|\\:|sidence de mme|puhetta johti)(.*)(\\s)(.*)$")[4]
  
  president_name  <- XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//span")[[1]])
  president_title <- tail(strsplit(XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//span")[[2]]), split = " ")[[1]], 1)
  president_name  <- stringr::str_match(tolower(president_name), "(\\.|\\:|sidence de mme|puhetta johti)(.*)$")[3]
  
  
  
  # if ( stringr::str_detect(tolower(president_name), "^présidence de") ) {
  #   president_name <- trimws(stringr::str_match(tolower(president_name), "présidence de (.*)")[[2]])
  #   president_name <- trimws(stringr::str_split(president_name, "\\.")[[1]][2])
  # } else {
  #   if ( stringr::str_detect(tolower(president_name), "^presidência: ") ) {
  #     president_name <- trimws(stringr::str_match(tolower(president_name), "presidência:(.*)\\svice-presidente")[[2]])
  #   } else {
  #     if ( stringr::str_detect(tolower(president_name), "^przewodnictwo: ") ) {
  #       president_name <- trimws(stringr::str_match(tolower(president_name), "przewodnictwo:(.*)\\swiceprzewodnicząca")[[2]])
  #     } else {
  #       president_name <- trimws(stringr::str_match(tolower(president_name), "(.*)president")[[2]])
  #       president_name <- trimws(stringr::str_split(president_name, "\\.")[[1]][2])
  #     }
  #   }
  # }
  
  president_name <- stringr::str_to_title(president_name)
  president_name <- trimws(president_name)
  
  
  event_title <- NA
  event_subtitle <- NA
  event_word_count <- NA
  event_sentence_count <- NA
  event_paragraph_count <- NA
  event_content <- NA
  event_translated_content <- NA
  event_doc_text <- NA
  
  
  ####################################
  # The colums of the detailed dataset
  event_chapter <- NA
  intervention_seqnum <- NA
  speaker_first_name <- NA
  speaker_last_name <- NA
  speaker_full_name <- NA
  speaker_gender <- NA
  speaker_is_minister <- NA
  speaker_type <- NA
  speaker_polgroup <- NA
  speaker_party <- NA
  speaker_district <- NA
  speaker_country <- NA
  speaker_media <- NA
  chapter_tabled_docid <- NA
  intervention_type <- NA
  intervention_lang <- NA
  intervention_word_count <- NA
  intervention_sentence_count <- NA
  intervention_paragraph_count <- NA
  intervention_text <- ""
  intervention_translated_text <- NA
  
  dfSpeaker <- data.frame()

  ########################################################
  # Go through the xml document chapter by chapter
  # and strip out any relevant info
  intervention_seqnum <- 1
  
  event_content <- ""
  event_translated_content <- ""
  
  previous_speaker_full_name <- ""

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
    
    ##################################################
    chapter_tabled_docid <- NA
    chapter_adopted_docid <- NA
    
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

        
        if (length(content_node) > 0) {
          for (l in 1:length(content_node)) {

            content_type <- XML::xmlAttrs(content_node[[l]][[1]])
            next_content_type <- if (l < length(content_node)) XML::xmlAttrs(content_node[[l+1]][[1]]) else NULL
            
            first_parag <- NULL
            
            # We have a new speaker taking the stand - first thing : get his info and then his first paragraph
            if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type ) {
              speaker_first_name <- NA
              speaker_last_name <- NA
              speaker_full_name <- NA
              speaker_full_name_native <- NA
              speaker_gender <- NA
              speaker_is_minister <- NA
              speaker_type <- NA
              speaker_mepid <- NA
              speaker_polgroup <- NA
              speaker_party <- NA
              speaker_district <- NA
              speaker_country <- NA
              speaker_media <- NA
              chapter_tabled_docid <- NA
              chapter_adopted_docid <- NA
              intervention_type <- NA
              intervention_lang <- NA
              intervention_word_count <- NA
              intervention_sentence_count <- NA
              intervention_paragraph_count <- NA
              intervention_text <- ""
              intervention_translated_text <- NA
              
              dfSpeaker <- data.frame()
              
              hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,75), " – | − | - | — | - | - | – | ‒ ")[1]            
              
              if (is.na(hyphen_pos)) {
                hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,75), "\\)\\. ")[1] + 1
              }
              
              if (!is.na(hyphen_pos)) {
                speaker_text <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,hyphen_pos)))
                speaker_text <- gsub("\\.", "", speaker_text)
                speaker_text <- gsub('^(\u00a0*)','', speaker_text)
              } else {
                speaker_text <- previous_speaker_full_name
              }
              
              
              if ( !is.na(speaker_text) && nchar(speaker_text) == 0 ) next
              
              if (opt$refresh_data != FALSE) {
                tryCatch(
                  {
                    clessnverse::logit(scriptname, paste("checking if ",event_id, "-", intervention_seqnum, "beta"," already exists", sep-""), logger)
                    item_check <- hublot::filter_table_items(
                      table_name = paste("clhub_tables_warehouse_", wh_table, sep=''), 
                      filter = list(key = paste(event_id, "-", intervention_seqnum, "beta", sep="")),
                      credentials = credentials
                    )

                    if (length(item_check$results) == 1) {
                      clessnverse::logit(
                        scriptname, 
                        paste(
                          "item ", 
                          event_id, "-", intervention_seqnum, "beta",
                          " already exists, skipping...", 
                          sep=""),
                        logger)

                      intervention_seqnum <- intervention_seqnum + 1
                      next
                    }
                  },
                  error=function(e) {},
                  finally={}
                )

              }

                
              if (length(stringr::str_split(speaker_text,",")[[1]]) == 2) {
                speaker_full_name <- stringr::str_split(speaker_text,",")[[1]][1]
                speaker_full_name <- trimws(speaker_full_name, "both")
                
                intervention_type <- stringr::str_split(speaker_text,",")[[1]][2:length(stringr::str_split(speaker_text,",")[[1]])]
                intervention_type <- trimws(intervention_type)
                intervention_type <- paste(intervention_type, collapse = ', ')
                intervention_type <- trimws(intervention_type)                
              } else {
                if (length(stringr::str_split(speaker_text,",")[[1]]) == 1) {
                  speaker_full_name <- speaker_text
                  intervention_type <- speaker_text 
                } else {
                  speaker_full_name <- paste(stringr::str_split(speaker_text,",")[[1]][2], stringr::str_split(speaker_text,",")[[1]][1])
                  speaker_full_name <- trimws(speaker_full_name, "both")
                  
                  intervention_type <- stringr::str_split(speaker_text,",")[[1]][3:length(stringr::str_split(speaker_text,",")[[1]])]
                  intervention_type <- trimws(intervention_type)
                  intervention_type <- paste(intervention_type, collapse = ', ')
                  intervention_type <- trimws(intervention_type)
                }
              }
              
              if ( !is.na(intervention_type) && intervention_type != "" && tolower(intervention_type) != "president" ) {
                if (opt$translate && !is.null(intervention_type) && !is.na(intervention_type)) { 
                  clessnverse::logit(scriptname, paste("detecting language of string", intervention_type), logger)
                  detected_lang <- clessnverse::detect_language("fastText", intervention_type)
                  if (detected_lang != "en") {
                    intervention_type_translated <- clessnverse::translate_text(
                      text=intervention_type,
                      engine="deeptranslate",
                      source_lang=detected_lang,
                      target_lang="en",
                      translate=opt$translate)
                  } else {
                    intervention_type_translated <- intervention_type
                  }

                  if (!is.na(intervention_type_translated)) intervention_type <- intervention_type_translated

                } else {
                  if (is.null(intervention_type) || is.na(intervention_type)) intervention_type <- NA_character_
                }
              }
                
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteur") || 
                                                stringr::str_detect(tolower(intervention_type), "reporter")) ) {
                intervention_type <- NA
                speaker_type <- "Reporter"
              }
                
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "auteur") ||
                                                stringr::str_detect(tolower(intervention_type), "author")) ) {
                intervention_type <- NA
                speaker_type <- "Author"
              }
                  
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "membre") ||
                                                stringr::str_detect(tolower(intervention_type), "member")) ) {
                speaker_type <- intervention_type
                intervention_type <- NA
              }
                
              if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "chancellor|president\\sof|minister|his\\sholiness|secretary|king") ) {
                speaker_type <- intervention_type
                intervention_type <- "Speech"
              }
              
              if ( !is.na(intervention_type) && (tolower(intervention_type) == "president" || tolower(intervention_type) == "chairman" )) {
                intervention_type <- "moderation"
                speaker_type <- "President"
                speaker_full_name <- president_name
              } 
              
              if ( is.na(speaker_type) ) {
                speaker_type <- intervention_type
                intervention_type <- "Speech"
              }
              
              speaker_text <- NA    
              
              speaker_full_name <- gsub(":", "", speaker_full_name)
              
              if ( stringr::str_detect(tolower(speaker_full_name), tolower(president_title)) || stringr::str_detect(tolower(speaker_full_name), tolower("ident")) ) {
                speaker_full_name <- president_name
                speaker_type <- "President"
              } else {
                speaker_full_name <- trimws(stringr::str_remove(speaker_full_name, "\\|\\s"))
              }
              
              if ( stringr::str_detect(speaker_full_name, "\\((.*)\\)") ) {
                  speaker_polgroup <- stringr::str_match(speaker_full_name, "\\((.*)\\)")[2]
                  speaker_full_name <- stringr::str_replace(speaker_full_name, "\\((.*)\\)", "")
                  speaker_full_name <- stringr::str_replace(gsub("\\s+", " ", stringr::str_trim(speaker_full_name)), "B", "b")
              }
              
              speaker_full_name <- trimws(speaker_full_name)
              speaker_full_name <- gsub('^(\u00a0*)','', speaker_full_name)
              
              speaker_full_name <- stringr::str_to_title(speaker_full_name)
              
              speaker_full_name_native  <- speaker_full_name
              
              # Get the speaker data from hub 2.0.  If absent try to get it from the parliament.
              # If parliament successful and not in hub, then write in hub for next time
              if ( !is.null(df_people) ) {
                dfSpeaker <- df_people[which(df_people$data.fullName == speaker_full_name),]
                if (nrow(dfSpeaker) == 0) {
                  dfSpeaker <- df_people[which(df_people$data.fullNameNative == speaker_full_name_native),]
                  if (nrow(dfSpeaker) == 0) {
                    dfSpeaker <- df_people[which(tolower(df_people$data.fullNameNative) == tolower(speaker_full_name_native)),]
                    if (nrow(dfSpeaker) == 0) {
                      dfSpeaker <- df_people[which(tolower(df_people$data.fullName) == tolower(speaker_full_name_native)),]
                    }
                  }
                }
              } else {
                dfSpeaker <- data.frame()
              }

              #########################
              dfSpeaker <- data.frame()
              #########################

              if (nrow(dfSpeaker) == 0 && !stringr::str_detect(clessnverse::rm_accents(tolower(speaker_full_name)), "president")) {
                # We could not find the speaker in the hub based on his/her full name.
                # Get it from the europe parliament web site
                # And then store it in the hub for next time
                dfSpeaker <- clessnverse::get_europe_mep_data(speaker_full_name_native)

                if (!is.na(dfSpeaker$mepid)) {
                  # Found it on the europe parliament web site
                  speaker_full_name <- trimws(stringr::str_to_title(dfSpeaker$fullname))
                  speaker_first_name <- trimws(stringr::str_to_title(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]]))
                  speaker_last_name <- trimws(stringr::str_to_title(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2]))
                  speaker_mepid <- dfSpeaker$mepid
                  speaker_polgroup <- dfSpeaker$polgroup
                  speaker_party <- dfSpeaker$party
                  speaker_country <- dfSpeaker$country
                  skip_person_hub_write <- FALSE
                } else {
                  # Not found in hub NOR in parliament web site => translate
                  if (opt$translate && !is.null(speaker_full_name_native) && !is.na(speaker_full_name_native)) {                    
                    detected_lang <- clessnverse::detect_language("fastText", speaker_full_name_native)
                    if (detected_lang != "en") {
                      speaker_full_name_translated <- clessnverse::translate_text(
                                                        text=speaker_full_name_native, 
                                                        engine="deeptranslate",
                                                        source_lang=detected_lang, 
                                                        target_lang="en",
                                                        translate=opt$translate
                                                      )
                    } else {
                      speaker_full_name_translated <- speaker_full_name_native
                    }
                    if (!is.na(speaker_full_name_translated)) speaker_full_name <- speaker_full_name_translated
                  } 
                  
                  speaker_first_name <- trimws(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]])
                  speaker_last_name <- trimws(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2])
                  
                  if (tolower(clessnverse::rm_accents(speaker_first_name)) == "president") {
                    speaker_full_name <- president_name
                    speaker_first_name <- trimws(stringr::str_to_title(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]]))
                    speaker_last_name <- trimws(stringr::str_to_title(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2]))
                    skip_person_hub_write <- TRUE
                  } else {
                    skip_person_hub_write <- FALSE
                  }
                  
                }
                

                speaker_gender <- paste("", gender::gender(strsplit(speaker_first_name, "\\s")[[1]])$gender, sep = "")
                if ( speaker_gender == "" ) speaker_gender <- NA
                
                speaker_is_minister <- NA
                speaker_district <- NA
                speaker_media <- NA
                
                
                if (!skip_person_hub_write) {
                  
                  # person_row <- list("fullName"=speaker_full_name,
                  #                    "fullNameNative" = speaker_full_name_native,
                  #                    "isFemale"= if (!is.na(speaker_gender) && !is.null(speaker_gender) && speaker_gender=="female") as.character(1) else as.character(0),
                  #                    "lastName"=speaker_last_name,
                  #                    "firstName"=speaker_first_name,
                  #                    "twitterID"=NA_character_,
                  #                    "isMinister"="0",
                  #                    "twitterName"=NA_character_,
                  #                    "currentParty"=speaker_polgroup,
                  #                    "twitterHandle"=NA_character_,
                  #                    "currentMinister"=NA_character_,
                  #                    "currentPolGroup"=speaker_party,
                  #                    "twitterLocation"=NA_character_,
                  #                    "twitterPostsCount"=NA_character_,
                  #                    "twitterProfileURL"=NA_character_,
                  #                    "twitterListedCount"=NA_character_,
                  #                    "twitterFriendsCount"=NA_character_,
                  #                    "currentFunctionsList"=NA_character_,
                  #                    "twitterFollowersCount"=NA_character_,
                  #                    "currentProvinceOrState"=speaker_country,
                  #                    "twitterAccountVerified"=NA_character_,
                  #                    "twitterProfileImageURL"=NA_character_,
                  #                    "twitterAccountCreatedAt"=NA_character_,
                  #                    "twitterAccountCreatedOn"=NA_character_,
                  #                    "twitterAccountProtected"=NA_character_,
                  #                    "twitterProfileBannerURL"=NA_character_,
                  #                    "twitterUpdateDateStamps"=NA_character_,
                  #                    "twitterUpdateTimeStamps"=NA_character_
                  #                   )
                  
                  # if ( is.na(speaker_mepid) ) speaker_mepid <- digest::digest(speaker_full_name)
                  
                  # clessnverse::logit(scriptname=scriptname, message=paste("adding", speaker_full_name, "-", speaker_full_name_native, "to the hub"), logger = logger)
                  
                  # tryCatch(
                  #   {
                  #     clessnhub::create_item("persons", paste("EU-",speaker_mepid,sep=''), "mp", "v3", person_metadata_row, person_data_row)
                  #   },
                  #   error= function(e) {
                  #     clessnhub::create_item("persons", digest::digest(speaker_full_name), "mp", "v3", person_metadata_row, person_data_row)
                  #   },
                  #   finally={}
                  # )
                  
                  # person_metadata_dfrow <- as.data.frame(person_metadata_row)
                  # names(person_metadata_dfrow) <- paste("metadata.", names(person_metadata_dfrow),sep='')
                  # person_data_dfrow <- as.data.frame(person_data_row)
                  # names(person_data_dfrow) <- paste("data.", names(person_data_dfrow),sep='')
                  # dfRow <- tibble::tibble(key=paste("EU-",speaker_mepid,sep=''), type="mp", schema="v3", uuid="") %>% cbind(person_metadata_dfrow) %>% cbind(person_data_dfrow)
    
                  # if ( is.null(df_people) ) {
                  #   df_people <- dfRow
                  #   #df_people <- df_people %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " ")
                  # } else {
                  #   df_people <- df_people %>% rbind(dfRow)# %>% tidyr::separate(data.lastName, c("data.lastName1", "data.lastName2"), " "))
                  # }

                }    
              } else {
                # Found the speaker in the hub, use the one that has the most non NAs!
                #dfSpeaker <- dfSpeaker[which(rowSums(is.na(dfSpeaker)) == min(rowSums(is.na(dfSpeaker))))[1],]

                #speaker_mepid <- dfSpeaker$key
                #speaker_full_name <- dfSpeaker$data.fullName
                #speaker_first_name <- dfSpeaker$data.firstName
                #speaker_last_name <- dfSpeaker$data.lastName
                #speaker_polgroup <- dfSpeaker$data.currentParty
                #speaker_party <- dfSpeaker$data.currentPolGroup
                #speaker_country <- dfSpeaker$metadata.country
                #speaker_gender <- if (dfSpeaker$data.isFemale == 1) "female" else "male"
                #speaker_is_minister <- NA
                #speaker_district <- NA
                #speaker_media <- NA
              }

              clessnverse::logit(
                scriptname,
                paste(
                  "processing intervention from ", speaker_full_name,
                  " (", speaker_full_name_native, "), ", speaker_gender,
                  ", from country ", speaker_country, ", belonging to party ", speaker_party,
                  ", and political group ", speaker_polgroup, 
                  ", as speaker type ", speaker_type,
                  sep=""
                ),
                logger
              )
                            
              if (!is.na(hyphen_pos)) {
                first_parag <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),hyphen_pos+2,nchar(XML::xmlValue(content_node[[l]])))))
                first_parag <- gsub("\\.", "", first_parag)
                first_parag <- gsub('^(\u00a0*)','', first_parag)
              } else {
                first_parag <- stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)–(\\s*)(.*)$")))[3]
                if (is.na(first_parag)) first_parag <- stringi::stri_remove_empty(trimws(stringr::str_match(XML::xmlValue(content_node[[l]]),"^(.*)\\.(\\s*)−(\\s*)(.*)$")))[3]
              }
              
              
              if (length(first_parag) == 0) first_parag <- ""
              
              intervention_text <- ""              
            } else {
              if (!opt$refresh_data) {
                tryCatch(
                  {
                    clessnverse::logit(scriptname, paste("checking if ",event_id, "-", intervention_seqnum, "beta"," already exists", sep-""), logger)
                    item_check <<- clessnhub::get_item(
                      'agoraplus_interventions', 
                      paste(event_id, "-", intervention_seqnum, "beta", sep="")
                    )

                    if (!is.null(item_check)) {
                      clessnverse::logit(
                        scriptname, 
                        paste(
                          "item ", 
                          event_id, "-", intervention_seqnum, "beta",
                          " already exists, skipping...", 
                          sep=""),
                        logger)

                      intervention_seqnum <- intervention_seqnum + 1
                      next
                    }
                  },
                  error=function(e) {},
                  finally={}
                )
              }
            }# if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type )
            
            # Here we have an intervention - it is either  a new intervention (if first_parag is not null) or the continuation of the same intervention
            if ( is.null(content_type) || !is.null(first_parag) ) {

              if ( !is.null(first_parag) ) {
                #New
                intervention_text <- first_parag
              } else {
                #Continuation
                intervention_text <- paste(intervention_text, XML::xmlValue(content_node[[l]][[1]]), sep="\n\n")
              }
              
              if ( !is.na(intervention_text) && (intervention_text == "\n\n ") ) intervention_text <- ""
              
              first_parag <- NULL
            }
            
            if ( !is.null(intervention_type) && !is.na(intervention_type) ) {
              intervention_type <- gsub("\\(", "", intervention_type)
              intervention_type <- gsub("\\)", "", intervention_type)
              intervention_type <- gsub("\"", "", intervention_type)
              intervention_type <- gsub("\\\\u2012", "", intervention_type)
              intervention_type <- gsub("\\\\ U2012", "", intervention_type)
              intervention_type <- stringr::str_squish(intervention_type)
              intervention_type <- trimws(intervention_type)
              intervention_type <- stringr::str_to_title(intervention_type)
            }
            
            if ( !is.null(speaker_type) && !is.na(speaker_type) ) {
              speaker_type <- trimws(speaker_type)
            }
            
            
            # Look at the next paragraph to see if it is a new intervention
            if ( "/doceo/data/img/arrow_summary.gif" %in% next_content_type || l == length(content_node) && !is.na(intervention_text) && intervention_text != "" ) {
              # next is new              
              intervention_text <- trimws(intervention_text, "left")
              intervention_word_count <- nrow(tidytext::unnest_tokens(tibble::tibble(txt=intervention_text), word, txt, token="words",format="text"))
              intervention_sentence_count <- nrow(tidytext::unnest_tokens(tibble::tibble(txt=intervention_text), sentence, txt, token="sentences",format="text"))
              intervention_paragraph_count <- stringr::str_count(intervention_text, "\n\n") + 1
              

              # Translation
              if (grepl("\\\\", intervention_text)) intervention_text <- gsub("\\\\"," ", intervention_text)
              intervention_text <- gsub("^NA\n\n", "", intervention_text)
              intervention_text <- gsub("^\n\n", "", intervention_text)
              
              if (opt$translate && !is.null(intervention_text) && !is.na(intervention_text)) {
                detected_lang <- clessnverse::detect_language(engine="fastText",substring(intervention_text,1,500))
                intervention_lang <- detected_lang
                if (detected_lang != "en") {
                  intervention_translated_text <- clessnverse::translate_text(
                                                    text=intervention_text, 
                                                    enging="deeptranslate",
                                                    source_lang=detected_lang,
                                                    target_lang="en",
                                                    translate=TRUE)
                } else {
                  intervention_translated_text <- intervention_text
                }
                clessnverse::logit(
                  scriptname, 
                  paste(
                    "translated\n", 
                    substring(intervention_text, 1, 20), 
                    "\nto\n", 
                    substring(intervention_translated_text, 1, 20)
                  ), 
                  logger
                )
              } else {
                detected_lang <- "xx"
                intervention_translated_text <- "not translated because translation is turned off"
              }
              
              if ( is.na(intervention_lang) ) {
                intervention_lang <- clessnverse::detect_language("fastText", intervention_text)
                #intervention_lang <- df_country$two_letter[grep(tolower(intervention_lang), df_country$two_letter_locales)]
                #intervention_lang <- toupper(intervention_lang)
              }

              if (is.na(speaker_country)) speaker_country <- df_country$two_letter[grep(tolower(intervention_lang), df_country$two_letter_locales)]
              
              if (is.null(intervention_translated_text)) intervention_translated_text <- NA
              
              row_to_commit <- list(
                eventID = event_id, eventDate = event_date, eventStartTime = event_start_time, eventEndTime = event_end_time, 
                eventTitle = event_title, eventSubTitle = event_subtitle,
                interventionSeqNum = paste(intervention_seqnum, "beta", sep=""),
                objectOfBusinessID = NA, objectOfBusinessRubric = NA, objectOfBusinessTitle = NA, objectOfBusinessSeqNum = NA,
                subjectOfBusinessID = chapter_number, subjectOfBusinessTitle = chapter_title, subjectOfBusinessHeader = NA,
                subjectOfBusinessSeqNum = NA, subjectOfBusinessProceduralText = NA, subjectOfBusinessTabledDocID = chapter_tabled_docid,
                subjectOfBusinessTabledDocTitle = NA, subjectOfBusinessAdoptedDocID = chapter_adopted_docid,
                subjectOfBusinessAdoptedDocTitle = NA, speakerID = NA, speakerFirstName = speaker_first_name,
                speakerLastName = speaker_last_name, speakerFullName = speaker_full_name,
                speakerFullNameNative = speaker_full_name_native, speakerGender = speaker_gender,
                speakerType = speaker_type, speakerCountry = speaker_country,
                speakerIsMinister = speaker_is_minister, speakerPolGroup = speaker_polgroup,
                speakerParty = speaker_party, speakerDistrict = speaker_district,
                speakerMedia = speaker_media,
                interventionID = paste(gsub("dp", "", event_id),intervention_seqnum,sep=''),
                interventionDocID = NA, interventionDocTitle = NA,
                interventionType = intervention_type, interventionLang = intervention_lang,
                interventionWordCount = intervention_word_count, interventionSentenceCount = intervention_sentence_count,
                interventionParagraphCount = intervention_paragraph_count, interventionText = intervention_text,
                interventionTextFR = NA, interventionTextEN = intervention_translated_text)

              # commit intervention to the Hub
              clessnverse::logit(scriptname, "committing row", logger)

              clessnverse::commit_warehouse_row(
                table=wh_table,
                key=paste(event_id,"-", intervention_seqnum, "beta", sep=""),
                row=row_to_commit,
                refresh_data=TRUE,
                credentials=credentials 
              )

              clessnverse::logit(scriptname, "committing row done", logger)
              intervention_seqnum <- intervention_seqnum + 1

              previous_speaker_full_name <- speaker_full_name
              intervention_text <- ""
            } else {
              # Next is same speaker
            } #if ( "/doceo/data/img/arrow_summary.gif" %in% next_content_type || l == length(content_node) && !is.na(intervention_text) ) {
          } #for (l in 1:length(content_node))
        } #if (length(content_node) > 0)
      } # for (k in 1:length(chapter_nodes_list))
    } # if ("a" %in% chapter_nodes_list)
  } #for (j in 2:core_xml_nbchapters)
} #</function>



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
  } else {
    stop(paste("content-type", r$header$`Content-Type`, "not supported for lake item", lake_item$key, "in file", lake_item$file))
  }

} #</my_function>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    
  clessnverse::logit(scriptname, "starting main function", logger)
  clessnverse::logit(scriptname, paste("retrieving debates trasnscriptions from datelake with path = ", datalake_path, sep=''), logger)

  if (opt$method[[1]] == "date_range") {
    my_filter <- list(
      metadata__event_date__gte = opt$method[[2]],
      metadata__event_date__lte = opt$method[[3]]
    )
  }


  r <- hublot::filter_lake_items(
    credentials, 
    c( 
      list(path=datalake_path),
      my_filter
    )
  )

  for (lake_item in r$results) {
    strip_and_load_debate(lake_item)
  }

  clessnverse::logit(scriptname, "ending main function", logger)  
}



tryCatch( 
  withCallingHandlers(
  {
    library(dplyr)

    Sys.setlocale("LC_TIME", "fr_CA.utf8")

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

    opt <<- list(
      log_output = c("console"),
      method = c("date_range", "2016-11-30", "2016-11-30"),
      refresh_data = TRUE,
      translate = FALSE
    )

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

    metadata_filter <- list(institution="European Parliament")
    filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=metadata_filter)  
    df_people <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

    df_country <- clessnverse::get_warehouse_table(
      table_name = "countries",
      data_filter = list(),
      credentials = credentials,
      nbrows = 0
    )

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)
    
    main()
  },

  warning = function(w) {
    print(w)
    #final_message <<- if (final_message == "") w$message else paste(final_message, "\n", w$message, sep="")
    status <<- 2
  }),
    
  error = function(e) {
    print(e)
    #final_message <<- if (final_message == "") e$message else paste(final_message, "\n", e$message, sep="")
    status <<- 1
  },

  finally={
    # clessnverse::logit(scriptname, final_message, logger)

    # clessnverse::logit(scriptname, 
    #   paste(
    #       nb_lake_items, 
    #       "lake items were found",
    #       nb_debates,
    #       "debates were loaded in the data warehouse, totalling",
    #       nb_interventions
    #   ),
    #   logger
    # )

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::log_close(logger)
    if (exists("logger")) rm(logger)
    print(paste("exiting with status", status))
    #quit(status = status)
  }
)
