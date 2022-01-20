clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
#metadata_filter <- list(location="EU")
#filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=metadata_filter)  
#df <- clessnhub::get_items('agoraplus_interventions', filter=filter, download_data = TRUE)

# for (z in 1:nrow(df)) {
# 
#   if ( !is.null(df$data.interventionType[z]) && !is.na(df$data.interventionType[z]) && df$data.interventionType[z] != "" &&
#        ((textcat::textcat(df$data.interventionType[z]) != "english" && textcat::textcat(df$data.interventionType[z]) != "scots") || 
#         df$data.interventionType[z] == "moderation") ){
#     
#     if ( grepl("writing|written", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("blue-card", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("blue card", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("speech", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("for the", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("on behalf of", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("reporter", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("relator", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("on behalf of", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("mayor", tolower(df$data.interventionType[z])) ) next
#     if ( grepl("lecturer", tolower(df$data.interventionType[z])) ) next
#     
#     origInterventionType <- df$data.interventionType[z]
#     
#     if ( tolower(df$data.interventionType[z]) == "allocution" || tolower(df$data.interventionType[z]) == "moderation" ) {
#       tr <- "Speech"
#     } else if ( tolower(df$data.interventionType[z]) == "par écrit" || tolower(df$data.interventionType[z]) == "écrit" || tolower(df$data.interventionType[z]) == "písemnĕ" ) {
#       tr <- "In Writing"
#     } else if ( tolower(df$data.interventionType[z]) == "rapporteur" || tolower(df$data.interventionType[z]) == "protractor" ) {
#       tr <- "Reporter"
#     } else {
#       tr <- clessnverse::translateText(df$data.interventionType[z], engine="azure", target_lang="en",fake=F)[2]
#     }
# 
#     if ( !is.null(tr) && !is.na(tr) ) {
#       tr <- gsub("\\(", "", tr)
#       tr <- gsub("\\)", "", tr)
#       tr <- gsub("\"", "", tr)
#       tr <- gsub("\\\\u2012", "", tr)
#       tr <- gsub("\\\\ U2012", "", tr)
#       tr <- stringr::str_squish(tr)
#       tr <- trimws(tr)
#       tr <- stringr::str_to_title(tr)
#       
#       df$data.interventionType[z] <- tr
#     }
#     
#     df$data.interventionText[z] <- gsub("^NA\n\n", "", df$data.interventionText[z])
#     df$data.interventionText[z] <- gsub("^\n\n", "", df$data.interventionText[z])
#     df$data.interventionTextEN[z] <- gsub("^NA\n\n", "", df$data.interventionTextEN[z])
#     df$data.interventionTextEN[z] <- gsub("^\n\n", "", df$data.interventionTextEN[z])
#     
#     interv_metadata <- df[z,which(stringr::str_detect(colnames(df), "^metadata."))]
#     names(interv_metadata) <- gsub("metadata.", "", names(interv_metadata))
#     interv_data <- df[z,which(stringr::str_detect(colnames(df), "^data."))]
#     names(interv_data) <- gsub("data.", "", names(interv_data))
#     
#     
#     # cat(df$key[z],df$type[z],df$schema[z],"\n\n")
#     # print(as.list(interv_metadata))
#     # print(as.list(interv_data))
#     # cat("\n\n")
#     cat(z, df$key[z], origInterventionType, df$data.interventionType[z], "\n")
#     
#     clessnhub::edit_item('agoraplus_interventions', df$key[z], df$type[z], df$schema[z], as.list(interv_metadata), as.list(interv_data))
#     
#   }
# }

for (z in 1:nrow(df)) {

    origInterventionType <- df$data.interventionType[z]
    
    if (is.null(origInterventionType) || is.na(origInterventionType) || origInterventionType == "NA") next

    tr <- dfInterventions$data.interventionType[z]
      
    if ( !is.null(tr) && !is.na(tr) ) {
      tr <- gsub("\\(", "", tr)
      tr <- gsub("\\)", "", tr)
      tr <- gsub("\"", "", tr)
      tr <- gsub("\\\\u2012", "", tr)
      tr <- gsub("\\\\ U2012", "", tr)
      tr <- gsub("\\\\", "", tr)
      tr <- stringr::str_squish(tr)
      tr <- trimws(tr)
      tr <- stringr::str_to_title(tr)
      
      if ( grepl("in writing", tolower(tr)) || grepl("par écrit", tolower(tr)) ) tr <- "In Writing"

      df$data.interventionType[z] <- tr
    }

    df$data.interventionText[z] <- gsub("^NA\n\n", "", df$data.interventionText[z])
    df$data.interventionText[z] <- gsub("^\n\n", "", df$data.interventionText[z])
    df$data.interventionTextEN[z] <- gsub("^NA\n\n", "", df$data.interventionTextEN[z])
    df$data.interventionTextEN[z] <- gsub("^\n\n", "", df$data.interventionTextEN[z])

    interv_metadata <- df[z,which(stringr::str_detect(colnames(df), "^metadata."))]
    names(interv_metadata) <- gsub("metadata.", "", names(interv_metadata))
    interv_data <- df[z,which(stringr::str_detect(colnames(df), "^data."))]
    names(interv_data) <- gsub("data.", "", names(interv_data))


    # cat(df$key[z],df$type[z],df$schema[z],"\n\n")
    # print(as.list(interv_metadata))
    # print(as.list(interv_data))
    # cat("\n\n")
    cat(z, df$key[z], origInterventionType, df$data.interventionType[z], "\n")

    clessnhub::edit_item('agoraplus_interventions', df$key[z], df$type[z], df$schema[z], as.list(interv_metadata), as.list(interv_data))

  
}

