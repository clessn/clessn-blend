library(dplyr)

nullToNA <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

df <- readRDS("dfInterventions_all2.rds")

# eu_parties_acronyms <- c("ECR", 
#                          "S&D", 
#                          "EPP", 
#                          "Renew", 
#                          "Greens/EFA", 
#                          "ID", 
#                          "The Left", 
#                          "NI",
#                          "EFDD")

eu_parties_acronyms <- c("S%26D", 
                         "GUE/NGL", 
                         "ALDE", 
                         "ECR", 
                         "EFDD", 
                         "EFD",
                         "Verts/ALE", 
                         "ID",
                         "Europe Écologie", 
                         "Renew",
                         "NI",
                         "ENF"
                         )

eu_parties_longname <- c("Group of the Progressive Alliance of Socialists and Democrats in the European Parliament",
                         "The Left group in the European Parliament - GUE/NGL",
                         "Alliance of Liberals and Democrats for Europe",
                         "European Conservatives and Reformists Group", 
                         "Europe of Freedom and Direct Democracy",
                         "Europe of Freedom and Direct Democracy",
                         "Group of the Greens/European Free Alliance",
                         "Identity and Democracy Group", 
                         "Europe Écologie",
                         "Group Renew Europe", 
                         "Non-attached Members",
                         "Europe of Nations and Freedom"
                         )


clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
logger <- clessnverse::loginit("clean-debates-from-hub2", backend = c("file", "console"), logpath = Sys.getenv("LOG_PATH"))

metadata_filter <- list(location="EU")

for (i in 1:length(eu_parties_acronyms)) {
  
  data_filter <- list(speakerParty = as.character(eu_parties_acronyms[i]))
  
  filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=metadata_filter, data=data_filter)  
  
  clessnverse::logit("clean-debates-from-hub2", paste(i, eu_parties_acronyms[i]), logger = logger)
  dfToClean <- clessnhub::get_items('agoraplus_interventions', filter=filter, download_data = TRUE)
  
  if (is.null(dfToClean)) next
  
  dfToClean$data.speakerPolGroup <- eu_parties_longname[i]
  dfToClean$data.speakerParty <- NA
  
  if (as.character(eu_parties_acronyms[i]) == "S%26D") {
    df$data.speakerPolGroup[which(df$data.speakerParty=="S&D")] <-  "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament"
    df$data.speakerParty[which(df$data.speakerParty=="S&D")] <- NA
  } else {
    df$data.speakerPolGroup[which(df$data.speakerParty==as.character(eu_parties_acronyms[i]))] <- eu_parties_longname[i]
    df$data.speakerParty[which(df$data.speakerParty==as.character(eu_parties_acronyms[i]))] <- NA
  }
  
  for (j in 1:nrow(dfToClean)) {
    interv_metadata <- dfToClean[j,which(stringr::str_detect(colnames(dfToClean), "^metadata."))]
    names(interv_metadata) <- gsub("^metadata.", "", names(interv_metadata))
    interv_data <- dfToClean[j,which(stringr::str_detect(colnames(dfToClean), "^data."))]
    names(interv_data) <- gsub("^data.", "", names(interv_data))
    
    #cat(j, dfToClean$key[j], "\n")
    
    
    interv_data_display <- interv_data
    interv_metadata_display <- interv_metadata
    interv_data_display$interventionText <- NULL
    interv_data_display$interventionTextEN <- NULL
    interv_data_display$interventionTextFR <- NULL

    #if (j==1) {
    clessnverse::logit("clean-debates-from-hub2", interv_data_display$key, logger = logger)
    clessnverse::logit("clean-debates-from-hub2", paste(interv_metadata_display, collapse=" * "), logger = logger)
    clessnverse::logit("clean-debates-from-hub2", paste(interv_data_display, collapse=" * "), logger = logger)
    #}
    
    clessnhub::edit_item('agoraplus_interventions', dfToClean$key[j], dfToClean$type[j], dfToClean$schema[j], as.list(interv_metadata), as.list(interv_data))
  }
}

