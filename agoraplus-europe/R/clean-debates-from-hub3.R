library(dplyr)

nullToNA <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

df <- readRDS("dfInterventions_all4.rds")


eu_polgroup_from <- c("European People's Party Group")

eu_polgroup_to <- c("Group of the European People's Party (Christian Democrats)")


clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
logger <- clessnverse::loginit("clean-debates-from-hub2", backend = c("file", "console"), logpath = Sys.getenv("LOG_PATH"))

metadata_filter <- list(location="EU")

for (i in 1:length(eu_parties_acronyms)) {
  
  data_filter <- list(speakerPolGroup = eu_polgroup_from[i])
  
  filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=metadata_filter, data=data_filter)  
  
  clessnverse::logit("clean-debates-from-hub2", paste(i, eu_polgroup_from[i]), logger = logger)
  dfToClean <- clessnhub::get_items('agoraplus_interventions', filter=filter, download_data = TRUE)
  
  if (is.null(dfToClean)) next
  
  dfToClean$data.speakerPolGroup <- eu_polgroup_to[i]

  df$data.speakerPolGroup[which(df$data.speakerPolGroup==eu_polgroup_from[i])] <- eu_polgroup_to[i]

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

