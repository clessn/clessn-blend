library(dplyr)

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

table <- table(df$data.interventionType)
freq <- as.data.frame(table)
badfreq <-  freq %>% filter(!stringr::str_detect(tolower(Var1), "behalf|namen|draftsman|chairman of|^chairman$|blue|^for ") & Freq <  10) 
goodfreq <- freq %>% filter(stringr::str_detect(tolower(Var1),  "behalf|namen|draftsman|chairman of|^chairman$|blue|^for ") | Freq >= 10) 
goodfreq <- goodfreq[-c(9,10,54,58,47,111),]


logger <- clessnverse::loginit("clean-debates-from-hub", backend = c("file", "console"), logpath = Sys.getenv("LOG_PATH"))

metadata_filter <- list(location="EU")

for (i in 1:nrow(goodfreq)) {
  
  clessnverse::logit("clean-debates-from-hub", paste("."), logger = logger)
  clessnverse::logit("clean-debates-from-hub", paste("================================"), logger = logger)
  clessnverse::logit("clean-debates-from-hub", paste("."), logger = logger)
  clessnverse::logit("clean-debates-from-hub", paste("retreiving interventions of type", goodfreq$Var1[i]), logger = logger)

  if ( grepl("^On Behalf Of", as.character(goodfreq$Var1[i]))  ) next
  
  #if (!grepl(',', as.character(goodfreq$Var1[i]))){
    data_filter <- list(interventionType = as.character(goodfreq$Var1[i]))
  #} else {
  #  data_filter <- list(interventionType = gsub(",","%2C%",as.character(goodfreq$Var1[i])))
  #}
  
  filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=metadata_filter, data=data_filter)  
  dfToClean <- clessnhub::get_items('agoraplus_interventions', filter=filter, download_data = TRUE)
  
  if (is.null(dfToClean)) next

  if ( grepl("question", tolower(as.character(goodfreq$Var1[i]))) && grepl("blue card", tolower(as.character(goodfreq$Var1[i]))) || 	
       grepl("raised with a blue card lift", tolower(as.character(goodfreq$Var1[i]))) ) {
    dfToClean$data.interventionType <- "Blue Card Question"
  }
  
  if ( grepl("answer|response", tolower(as.character(goodfreq$Var1[i])))  && grepl("blue card", tolower(as.character(goodfreq$Var1[i]))) ||
       grepl("set by raising a blue card", tolower(as.character(goodfreq$Var1[i]))) ) {
    dfToClean$data.interventionType <- "Blue Card Answer"
  }
  
  if ( tolower(as.character(goodfreq$Var1[i])) == "blue card" ) {
    dfToClean$data.interventionType <- "Blue Card Question"
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Au Nom Du Groupe The Left" ||  as.character(goodfreq$Var1[i]) == "Em Nome Do Grupo The Left" ||
       as.character(goodfreq$Var1[i]) == "Im Namen Der Fraktion The Left" || as.character(goodfreq$Var1[i]) == "Im Namen Der The Left-Fraktion" ||
       as.character(goodfreq$Var1[i]) == "Namens De Fractie The Left" || as.character(goodfreq$Var1[i]) == "Namens De The Left-Fractie" ) {
    dfToClean$data.interventionType <- "On Behalf Of The Left Group"
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Green Deal" ) {
    dfToClean$data.interventionType <- "On Behalf Of The Green Deal Group"
  }

  if ( as.character(goodfreq$Var1[i]) == "Vpc/Hr" ) {
    dfToClean$data.interventionType <- "On Behalf Of The Vpc/Hr Group"
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Brexit") {
    dfToClean$data.interventionType <- NA
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Chairman") {
    dfToClean$data.interventionType <- NA
    dfToClean$data.speakerType <- "President"
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Chairman Of The Agri Committee") {
    dfToClean$data.interventionType <- NA
    dfToClean$data.speakerType <- "Chairman Of The Agri Committee"
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Renew On Behalf Of The Group" || as.character(goodfreq$Var1[i]) == "Renew, On Behalf Of The Group") {
    dfToClean$data.interventionType <- NA
    dfToClean$data.speakerType <- "On Behalf Of The Group Renew"
  }
  
  if ( as.character(goodfreq$Var1[i]) == "Dimitrios Papadimoulis" || as.character(goodfreq$Var1[i]) == "Elena Kountoura" ||
       as.character(goodfreq$Var1[i]) == "For The Results Of The Vote And Other Details Of The Vote: See Minutes" ||
       as.character(goodfreq$Var1[i]) == "Konstantinos Arvanitis" || as.character(goodfreq$Var1[i]) == "Sophia In ‘T Veld") {
    dfToClean$data.interventionType <- NA
  }
  
  if ( grepl("draftsman|lecturer|mayor|ombudsman|author|rapporteur|referents", tolower(as.character(goodfreq$Var1[i]))) ) {
    dfToClean$data.speakerType <- dfToClean$data.interventionType
    dfToClean$data.interventionType <- NA
  }
  
  if ( grepl("^for\\s(.*)", tolower(as.character(goodfreq$Var1[i]))) ) {
    dfToClean$data.interventionType <- gsub("^For", "On Behalf Of The", dfToClean$data.interventionType)
    dfToClean$data.interventionType <- gsub("The The", "The", dfToClean$data.interventionType)
    dfToClean$data.interventionType <- gsub("Groups", "Group", dfToClean$data.interventionType)
    dfToClean$data.interventionType <- gsub("-Gruppen", " Group", dfToClean$data.interventionType)
    dfToClean$data.interventionType <- gsub("Gruppen", "Group", dfToClean$data.interventionType)
    
    if (!grepl("group", tolower(dfToClean$data.interventionType))) {
      dfToClean$data.interventionType <- paste(dfToClean$data.interventionType, "Group")
    }
  }

  for (j in 1:nrow(dfToClean)) {
      interv_metadata <- dfToClean[j,which(stringr::str_detect(colnames(dfToClean), "^metadata."))]
      names(interv_metadata) <- gsub("^metadata.", "", names(interv_metadata))
      interv_data <- dfToClean[j,which(stringr::str_detect(colnames(dfToClean), "^data."))]
      names(interv_data) <- gsub("^data.", "", names(interv_data))
  
      cat(j, dfToClean$key[j], "\n")
      
      
      interv_data_display <- interv_data
      interv_metadata_display <- interv_metadata
      interv_data_display$interventionText <- NULL
      interv_data_display$interventionTextEN <- NULL
      interv_data_display$interventionTextFR <- NULL
      #if (j==1) {
        clessnverse::logit("clean-debates-from-hub", )
        clessnverse::logit("clean-debates-from-hub", paste(interv_metadata_display, collapse=" * "), logger = logger)
        clessnverse::logit("clean-debates-from-hub", paste(interv_data_display, collapse=" * "), logger = logger)
      #}
  
      clessnhub::edit_item('agoraplus_interventions', dfToClean$key[j], dfToClean$type[j], dfToClean$schema[j], as.list(interv_metadata), as.list(interv_data))
  }
}

# for (z in 1:nrow(df)) {
#     interv_metadata <- df[z,which(stringr::str_detect(colnames(df), "^metadata."))]
#     names(interv_metadata) <- gsub("^metadata.", "", names(interv_metadata))
#     interv_data <- df[z,which(stringr::str_detect(colnames(df), "^data."))]
#     names(interv_data) <- gsub("^data.", "", names(interv_data))
# 
#     cat(z, df$key[z], "\n")
# 
#     clessnhub::edit_item('agoraplus_interventions', df$key[z], df$type[z], df$schema[z], as.list(interv_metadata), as.list(interv_data))
# }

# for (z in 1:nrow(df)) {
# 
#   if ( !is.null(df$data.interventionType[z]) && !is.na(df$data.interventionType[z]) && df$data.interventionType[z] != "" ){
#     
#     translate <- T
# 
#     if ( grepl("writing|written", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("blue-card", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("blue card", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("speech", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("for the", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("on behalf of", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("reporter", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("relator", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("on behalf of", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("mayor", tolower(df$data.interventionType[z])) ) translate <- F
#     if ( grepl("lecturer", tolower(df$data.interventionType[z])) ) translate <- F
#     
#     if ( is.na(textcat::textcat(df$data.interventionType[z])) || textcat::textcat(df$data.interventionType[z]) == "english" ) translate <- F
#     if ( is.na(textcat::textcat(df$data.interventionType[z])) || textcat::textcat(df$data.interventionType[z]) == "scots" ) translate <- F  
#     if ( df$data.interventionType[z] == "moderation" ) translate <- F  
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
#       if (translate) {
#         tr <- clessnverse::translateText(df$data.interventionType[z], engine="azure", target_lang="en",fake=F)[2]
#       } else {
#         tr <- df$data.interventionType[z]
#       }
#     }
# 
#     if ( !is.null(tr) && !is.na(tr) ) {
#       tr <- gsub("\\(", "", tr)
#       tr <- gsub("\\)", "", tr)
#       tr <- gsub("\"", "", tr)
#       tr <- gsub("\\\\u2012", "", tr)
#       tr <- gsub("\\\\ U2012", "", tr)
#       tr <- gsub("\\\\", "", tr)
#       tr <- gsub("Club", "Group", tr)
#       tr <- gsub("Gruppen", "Group", tr)
#       tr <- gsub("Groups", "Group", tr)
#       tr <- stringr::str_squish(tr)
#       tr <- trimws(tr)
#       tr <- stringr::str_to_title(tr)
#       
#       if ( grepl("writing|written", tolower(tr)) ) tr <- "In Writing"
#       
#       if ( grepl("blue card|blue-card", tolower(tr)) && grepl("answer|response|reply", tolower(tr)) ) tr <- "Blue Card Answer"
#       if ( grepl("blue card|blue-card", tolower(tr)) && grepl("question|ask", tolower(tr)) ) tr <- "Blue Card Question"
#       
#       if ( grepl("presiden|chief|king|chair|draftsman|drafter|commissioner|committee|candidate", tolower(tr)) ) {
#         df$data.speakerType[z] <- tr
#         tr <- NA
#       }
#       
#       tr <- gsub("Blue-Card", "Blue Card", tr)
#       tr <- gsub("0rdfører", "Mayor", tr)
#       
#       
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
#     names(interv_metadata) <- gsub("^metadata.", "", names(interv_metadata))
#     interv_data <- df[z,which(stringr::str_detect(colnames(df), "^data."))]
#     names(interv_data) <- gsub("^data.", "", names(interv_data))
# 
#     interv_data$interventionType <- tr
# 
#     cat(z, df$key[z], origInterventionType, tr, "\n")
# 
#     #clessnhub::edit_item('agoraplus_interventions', df$key[z], df$type[z], df$schema[z], as.list(interv_metadata), as.list(interv_data))
# 
#   }
# }

# for (z in 1:nrow(df)) {
# 
#     origInterventionType <- df$data.interventionType[z]
#     
#     if (is.null(origInterventionType) || is.na(origInterventionType) || origInterventionType == "NA") next
# 
#     tr <- dfInterventions$data.interventionType[z]
#       
#     if ( !is.null(tr) && !is.na(tr) ) {
#       tr <- gsub("\\(", "", tr)
#       tr <- gsub("\\)", "", tr)
#       tr <- gsub("\"", "", tr)
#       tr <- gsub("\\\\u2012", "", tr)
#       tr <- gsub("\\\\ U2012", "", tr)
#       tr <- gsub("\\\\", "", tr)
#       tr <- stringr::str_squish(tr)
#       tr <- trimws(tr)
#       tr <- stringr::str_to_title(tr)
#       
#       if ( grepl("in writing", tolower(tr)) || grepl("par écrit", tolower(tr)) ) tr <- "In Writing"
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
#   
# }

