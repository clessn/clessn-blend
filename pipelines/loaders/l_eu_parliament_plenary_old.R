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
########################       Auxiliary functions       ######################
###############################################################################
`%vc%` <- clessnverse::`%vcontains%`

clntxt <- function(x) {
  x <- gsub("\\\"", "", x)
  return(x)
}

"%contains%" <- function(vec_y, x) {
   #checks that all the words in x are in a string in the vector of strings vec_y
   for (y in vec_y) {
      if (strsplit(y, " ") %vc% strsplit(x, " ")) return(TRUE)
   }
   return(FALSE)
}

"%contains_one_of%" <- function(vec_y, x) {
   #checks that at least one the words in x is in a string within the vector of strings vec_y
   for (y in vec_y) {
      if (strsplit(y, " ")[[1]] %in% strsplit(x, " ")[[1]]) return(TRUE)
   }
   return(FALSE)
}

"%contains_one_of2%" <- function(vec_y, x) {
   #checks that at least one the words in x is in a string within the vector of strings vec_y
   for (y in vec_y) {
      if (all(strsplit(y, " ")[[1]] %in% strsplit(x, " ")[[1]])) return(TRUE)
   }
   return(FALSE)
}

which_contains_one_of2 <- function(vec_y, x) {
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

compute_speaker_key <- function(text) {
  match <- stringr::str_match(text, "\\(.*")[1]
  match <- gsub("\\(", "\\\\(", match)
  match <- gsub("\\)", "\\\\)", match)
  match <- gsub("\\*", "\\\\*", match)

  if (!is.na(match)) {
    key <- gsub(match, "", text)
    key <- trimws(key)
  } else {
    key <- text
  }

  key <- gsub("'|’|\\.", "", key)

  key <- clessnverse::rm_accents(gsub(" ", "_", tolower(key)))

  return(key)
}


detect_president_change <- function(x) {
  return (  tolower(president) %contains_one_of2% tolower(x) ||
            tolower(vicepresident) %contains_one_of2% tolower(x) ||
            tolower(presidency) %contains_one_of2% tolower(x) ||
            tolower(presidency_of_the_hon) %contains_one_of2% tolower(x) )
}


extract_president_name <- function(x) {
  x <- gsub("\\.|\\:", "", x)
  x <- tolower(x)

  pattern <- which_contains_one_of2(tolower(presidency_of_the_hon), x)
  president_name <- gsub(tolower(pattern), "", x)

  pattern <- which_contains_one_of2(tolower(presidency), tolower(president_name))
  president_name <- gsub(tolower(pattern), "", president_name)

  pattern <- which_contains_one_of2(tolower(president), tolower(president_name))
  president_name <- gsub(tolower(pattern), "", president_name)

  pattern <- which_contains_one_of2(tolower(vicepresident), tolower(president_name))
  president_name <- gsub(tolower(pattern), "", president_name)

  president_name <- gsub(pattern, "", president_name)
  president_name <- stringr::str_to_title(trimws(president_name))

  return(president_name)
}






###############################################################################
###############################################################################
#####################           core functions          #######################
###############################################################################
###############################################################################

process_debate_xml <- function(lake_item, xml_core) {
}




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

  event_title <- XML::getNodeSet(xml_core, ".//td[@class='doc_title']")
  event_title <- if (!is.null(event_title) && length(event_title) > 0) event_title[[1]] else NULL
  event_title <- if (!is.null(event_title)) XML::xmlValue(event_title) else NA

  
  president_name  <- XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//span")[[1]])
  president_title <- tail(strsplit(XML::xmlValue(XML::getNodeSet(core_xml_chapters[[2]][["tr"]][["td"]], ".//span")[[2]]), split = " ")[[1]], 1)
  president_name  <- stringr::str_match(tolower(president_name), "(\\.|\\:|sidence de mme|puhetta johti)(.*)$")[3]
  president_name <- stringr::str_to_title(president_name)
  president_name <- trimws(president_name)
  
  
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
  speaker_type <- NA
  speaker_id <- NA
  speaker_polgroup <- NA
  speaker_party <- NA
  speaker_country <- NA
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

            if (!is.null(content_type) && content_type[[1]] == "bold") { 
              node_value <- XML::xmlValue(content_node[[l]])
              if ( detect_president_change(node_value) ) {
                president_name <- extract_president_name(node_value)
              }
            }
            
            first_parag <- NULL
            
            # We have a new speaker taking the stand - first thing : get his info and then his first paragraph
            if ( !is.null(content_type) && "/doceo/data/img/arrow_summary.gif" %in% content_type ) {
              speaker_first_name <- NA
              speaker_last_name <- NA
              speaker_full_name <- NA
              speaker_full_name_native <- NA
              speaker_gender <- NA
              speaker_type <- NA
              speaker_id <- NA
              speaker_polgroup <- NA
              speaker_party <- NA
              speaker_country <- NA
              intervention_type <- NA
              intervention_lang <- NA
              intervention_word_count <- NA
              intervention_sentence_count <- NA
              intervention_paragraph_count <- NA
              intervention_text <- ""
              intervention_translated_text <- NA
              
              speaker_text <- NA
              dfSpeaker <- data.frame()
              
              hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,75), " – | − | - | — | - | - | – | ‒ ")[1]            
              
              if (is.na(hyphen_pos)) {
                hyphen_pos <- stringr::str_locate(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,75), "\\)\\. ")[1] + 1
                speaker_text <- previous_speaker_full_name
              } else {
                speaker_text <- stringi::stri_remove_empty(trimws(stringr::str_sub(XML::xmlValue(content_node[[l]]),1,hyphen_pos)))
                speaker_text <- gsub("\\.", "", speaker_text)
                speaker_text <- gsub('^(\u00a0*)','', speaker_text)
              }
              
              if ( !is.na(speaker_text) && nchar(speaker_text) == 0 ) next
              
              if (!opt$refresh_data) {
                tryCatch(
                  {
                    clessnverse::logit(scriptname, paste("checking if ",event_id, "-", intervention_seqnum, "beta"," already exists", sep=""), logger)
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
              
              prez_check <- tolower(intervention_type)

              if ( !is.na(intervention_type) && intervention_type != "" && !(president %contains_one_of% prez_check) && !(tolower(df_people$data.full_name) %contains_one_of2% tolower(intervention_type)) ) {
                if (opt$translate && !is.null(intervention_type) && !is.na(intervention_type)) { 
                  clessnverse::logit(scriptname, paste("detecting language of string", intervention_type), logger)
                  detected_lang <- clessnverse::detect_language("fastText", clntxt(intervention_type))
                  if (detected_lang != "en") {
                    intervention_type_translated <- clessnverse::translate_text(
                      text=clntxt(intervention_type),
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
              } else  {
                if ( !is.null(intervention_type) && !is.na(intervention_type) && intervention_type != "" && president %contains_one_of% prez_check  && clessnverse::compute_nb_words(prez_check) < 4 ) {
                  intervention_type <- "Intervention from the president"
                  speaker_type <- "President"
                } else {
                  if ( !is.null(intervention_type) && !is.na(intervention_type) && intervention_type != "" && tolower(df_people$data.full_name) %contains_one_of2% tolower(intervention_type) ) {
                    intervention_type <- "Intervention from a member of parliament"
                    speaker_type <- "Member of Parliament"
                  }
                }
              }
                
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "rapporteur") || 
                                                stringr::str_detect(tolower(intervention_type), "reporter")) ) {
                intervention_type <- "Intervention from a rapporteur"
                speaker_type <- "Rapporteur"
              }
                
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "auteur") ||
                                                stringr::str_detect(tolower(intervention_type), "author")) ) {
                intervention_type <- "Intervention from the author"
                speaker_type <- "Author"
              }
                  
              if ( !is.na(intervention_type) && (stringr::str_detect(tolower(intervention_type), "membre") ||
                                                stringr::str_detect(tolower(intervention_type), "member")) ) {
                speaker_type <- intervention_type
                intervention_type <- "Intervention from a member of parliament"
              }

              if ( !is.na(intervention_type) && (tolower(president_of_the_committee) %contains_one_of2% tolower(intervention_type) ||
                                                 tolower(president_of_the_commission) %contains_one_of2% tolower(intervention_type) ) ) {
                speaker_type <- stringr::str_to_title(intervention_type)
                if (compute_speaker_key(speaker_full_name) %in% df_people$key) {
                  intervention_type <- "Intervention from a member of parliament"
                } else {
                  intervention_type <- paste("Intervention from the", speaker_type)
                }
              }
                
              if ( !is.na(intervention_type) && stringr::str_detect(tolower(intervention_type), "chancellor|president\\sof|minister|his\\sholiness|secretary|king") ) {
                speaker_type <- intervention_type
                intervention_type <- "Intervention from a dignitary"
              }

              speaker_full_name <- gsub(":", "", speaker_full_name)
              
              if ( stringr::str_detect(tolower(speaker_full_name), tolower(president_title)) || president %contains_one_of% tolower(speaker_full_name) ) {
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
                dfSpeaker <- df_people[which(tolower(df_people$data.full_name) == tolower(speaker_full_name)),]
                if (nrow(dfSpeaker) == 0) {
                  dfSpeaker <- df_people[which(grepl(tolower(speaker_full_name), tolower(df_people$data.other_names))),]
                  if (nrow(dfSpeaker) == 0) {
                    dfSpeaker <- df_people[which(grepl(tolower(speaker_full_name_native), tolower(df_people$data.other_names))),]
                    if (nrow(dfSpeaker) == 0) {
                      dfSpeaker <- df_people[which(tolower(clessnverse::rm_accents(df_people$data.full_name)) == tolower(clessnverse::rm_accents(speaker_full_name))),]
                      if (nrow(dfSpeaker) == 0) {
                        dfSpeaker <- df_people[which(grepl(tolower(clessnverse::rm_accents(speaker_full_name)), tolower(clessnverse::rm_accents(df_people$data.other_names)))),]
                        if (nrow(dfSpeaker) == 0) {
                          dfSpeaker <- df_people[which(grepl(tolower(clessnverse::rm_accents(speaker_full_name_native)), tolower(clessnverse::rm_accents(df_people$data.other_names)))),]
                        }
                      }
                    }
                  }
                }
              } else {
                dfSpeaker <- data.frame()
              }

              #########################
              #dfSpeaker <- data.frame()
              #########################

              if (nrow(dfSpeaker) == 0 && president %contains_one_of% tolower(speaker_full_name)) {
                # We could not find the speaker in the hub based on his/her full name.
                # Get it from the europe parliament web site
                # And then store it in the hub for next time
                clessnverse::logit(scriptname, "could not find speaker in the hub", logger)


                dfSpeaker <- clessnverse::get_europe_mep_data(speaker_full_name_native)

                if (!is.na(dfSpeaker$mepid)) {
                  # Found it on the europe parliament web site
                  speaker_full_name <- trimws(stringr::str_to_title(dfSpeaker$fullname))
                  speaker_first_name <- trimws(stringr::str_to_title(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]]))
                  speaker_last_name <- trimws(stringr::str_to_title(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2]))
                  speaker_polgroup <- dfSpeaker$polgroup
                  speaker_party <- dfSpeaker$party
                  speaker_country <- dfSpeaker$country
                  speaker_id <- compute_speaker_key(speaker_full_name)
                } else {
                  # Not found in hub NOR in parliament web site => put raw data in the row
                  
                }
                

                speaker_gender <- paste("", gender::gender(strsplit(speaker_first_name, "\\s")[[1]])$gender, sep = "")
                if ( speaker_gender == "" ) speaker_gender <- NA
                
              }  else {
                if (nrow(dfSpeaker) > 0) {
                  # found speaker in the hub
                  # take the richest row
                  clessnverse::logit(scriptname, "found speaker in the hub", logger)

                  dfSpeaker <- dfSpeaker[which.max(rowSums(!is.na(dfSpeaker))),]

                  speaker_full_name <- trimws(stringr::str_to_title(dfSpeaker$data.full_name))
                  speaker_first_name <- trimws(stringr::str_to_title(stringr::str_split(speaker_full_name, "\\s")[[1]][[1]]))
                  speaker_last_name <- trimws(stringr::str_to_title(stringr::str_match(speaker_full_name, paste("^",speaker_first_name,"(.*)$",sep=''))[2]))
                  speaker_gender <- dfSpeaker$data.gender
                  speaker_polgroup <- dfSpeaker$data.pol_group
                  speaker_party <- dfSpeaker$data.party
                  speaker_country <- dfSpeaker$data.country
                  speaker_id <- compute_speaker_key(speaker_full_name)
                }
              }

              if (is.na(speaker_type) && !is.na(speaker_polgroup)) {
                speaker_type <- "Member of Parliament"
              }

              clessnverse::logit(
                scriptname,
                paste(
                  "processing intervention from ", speaker_full_name,
                  " (", speaker_full_name_native, "), ", speaker_gender,
                  ", from country ", speaker_country, ", belonging to party ", speaker_party,
                  ", and political group ", speaker_polgroup, 
                  ", as speaker type ", speaker_type,
                  ", for intervention type ", intervention_type,
                  sep=""
                ),
                logger
              )

              # Now strip the intervention itself
                            
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
              # Same speaker
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
              
              if (opt$translate && !is.null(intervention_text) && !is.na(intervention_text) && nchar(intervention_text) > 0) {
                detected_lang <- clessnverse::detect_language(engine="fastText",substr(clntxt(intervention_text),1,500))
                intervention_lang <- detected_lang
                if (detected_lang != "en") {
                  intervention_translated_text <- clessnverse::translate_text(
                                                    text=clntxt(intervention_text),
                                                    engine="deeptranslate",
                                                    source_lang=detected_lang,
                                                    target_lang="en",
                                                    translate=TRUE)
                } else {
                  intervention_translated_text <- intervention_text
                }
                # clessnverse::logit(
                #   scriptname, 
                #   paste(
                #     "translated\n", 
                #     substring(intervention_text, 1, 20), 
                #     "\nto\n", 
                #     substring(intervention_translated_text, 1, 20)
                #   ), 
                #   logger
                # )
              } else {
                detected_lang <- "xx"
                intervention_lang <- "xx"
                intervention_translated_text <- "not translated because translation is turned off"
              }
              
              if ( is.na(intervention_lang) ) {
                intervention_lang <- clessnverse::detect_language("fastText", gsub("\\n", " ", intervention_text))
              }

              if (is.na(speaker_country)) {
                speaker_country <- df_country$two_letter[grep(tolower(intervention_lang), df_country$two_letter_locales)]

                if (length(speaker_country) == 0) {
                  speaker_country <- NA
                }
              }
              
              if (is.null(intervention_translated_text)) intervention_translated_text <- NA
              
              row_to_commit <- list(
                metadata_schema = "pipeline1_2023_03", metadata_event_format = "html",
                metadata_lake_item_path = lake_item$path, metadata_lake_item_key = lake_item$key,
                metadata_url = lake_item$metadata$source,
                event_id = event_id,  
                event_date = event_date, 
                event_start_time = event_start_time, event_end_time = event_end_time, 
                event_title = event_title, 
                subject_of_business_id = chapter_number, subject_of_business_title = chapter_title, 
                speaker_id = speaker_id, speaker_first_name = speaker_first_name,
                speaker_last_name = speaker_last_name, speaker_full_name = speaker_full_name,
                speaker_full_name_native = speaker_full_name_native, speaker_gender = speaker_gender,
                speaker_type = speaker_type, speaker_country = speaker_country,
                speaker_party = speaker_party, speaker_pol_group = speaker_polgroup,
                intervention_id = paste(gsub("dp", "", event_id),intervention_seqnum,sep=''),
                intervention_seq_num = paste(intervention_seqnum, "beta", sep=""),
                intervention_type = intervention_type, intervention_lang = intervention_lang,
                intervention_word_count = intervention_word_count, intervention_sentence_count = intervention_sentence_count,
                intervention_paragraph_count = intervention_paragraph_count, intervention_text = intervention_text,
                intervention_text_en = intervention_translated_text)

              # commit intervention to the Hub
              clessnverse::logit(scriptname, "committing row", logger)

              if (opt$backend == "hub") {
                nb_attempts <- 0
                write_success <- FALSE
                while (!write_success && nb_attempts < 20) {
                  tryCatch(
                    {
                      r <- clessnverse::commit_warehouse_row(
                        table=wh_table,
                        key=paste(event_id,"-", intervention_seqnum, "beta", sep=""),
                        row=row_to_commit,
                        refresh_data=opt$refresh_data,
                        credentials=credentials 
                      )

                      if (!is.null(r) && !is.na(r) && r) write_success <- TRUE 
                    },

                    error=function(e) {
                      clessnverse::logit(scriptname, paste("error rwiting to hub:", e$message, "on attempt", nb_attempts, ". sleeping 30 seconds"), logger)
                      sleep(30)
                    },

                    finally= {
                      nb_attempts <- nb_attempts + 1
                    }
                  )
                }
              } else {
                my_df <<- my_df %>% rbind(as.data.frame(row_to_commit))
              }
              clessnverse::logit(scriptname, "committing row done", logger)
              intervention_seqnum <- intervention_seqnum + 1

              previous_speaker_full_name <- speaker_full_name
              intervention_text <- ""

            } else {
              # Next is same speaker or a new president?
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
      if (r$header$`Content-Type` == "application/xml") {
      clessnverse::logit(scriptname, paste("extracting nodes from lake item xml file", lake_item$key), logger)
      doc_content <- httr::content(r, encoding = "UTF-8")
      parsed_xml <- XML::htmlParse(doc_content)
      xml_root <- XML::xmlRoot(parsed_xml)
      xml_head <- xml_root[[1]]
      xml_core <- xml_root[[2]]
      clessnverse::logit(scriptname, paste("properly extracted nodes from lake item", lake_item$key), logger)
      process_debate_xml(lake_item, xml_core)
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

  if (opt$method[[1]] == "date_range") {
    my_filter <- list(
      metadata__event_date__gte = opt$method[[2]],
      metadata__event_date__lte = opt$method[[3]]
    )
  }

  if (opt$backend != "hub") my_df <<- data.frame()

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

    president <<- tolower(unique(c(
      "president","президент","predsjednik","başkan","prezident","formand","presidentti","président",
      "präsident","Πρόεδρος","elnök","preside","uachtarán","Presidente","prezidents","prezidentas",
      "presidint","prezydent","presedinte","predsednik","presidentea","presidente","chairman","chair",
      "présidente","Präsident","President", "Preşedinte"
      )))

    vicepresident <<- tolower(unique(c(
      "vice-président","вицепрезидент","dopredsjednik","Başkan Vekili","víceprezident","vicepræsident",
      "asepresident","varapresidentti","Vizepräsident","αντιπρόεδρος","alelnök","Leasuachtarán",
      "vicepresidente","viceprezidents","viceprezidentas","Vizepresident","Viċi President","onderdirecteur",
      "fise-presidint","wiceprezydent","vice-presidente","vice-preşedinte","podpredsedníčka","podpredsednik",
      "lehendakariordea","vicepresident","vice President","vice-president","vice-présidente" 
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
      "προεδρία του Ον","elnöksége a Hon","uachtaránacht an Oinigh","presidenza dell'On","prezidentūra god",
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
      backend = "hub",
      log_output = c("console"),
      method = c("date_range", "2014-09-18", "2014-09-18"),
      refresh_data = TRUE,
      translate = TRUE
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
    filter <- clessnhub::create_filter(type="mp", schema="eu_mp_v1", metadata=metadata_filter)  
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
    if (final_message == "") {
      final_message <<- w$message
     } else {
      paste(final_message, "\n", w$message, sep="")
     }
     
    status <<- 2
  }),
    
  error = function(e) {
    print(e)
    if (final_message == "") {
      final_message <<- e$message
     } else {
      paste(final_message, "\n", e$message, sep="")
     }

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
