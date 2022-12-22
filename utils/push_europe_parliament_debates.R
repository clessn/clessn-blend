library(dplyr)

scriptname <- "push_europe_interventions"
opt <- list(dataframe_mode = "update", log_output = c("console"), hub_mode = "update", download_data = TRUE, translate=TRUE)
logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


for (a in 1:nrow(dfInterventions)) {

  m <- dfInterventions[a, which(grepl("^metadata.", names(dfInterventions)))]
  m <- as.list(m)
  names(m) <- gsub("^metadata.", "", names(m))

  d <- dfInterventions[a, which(grepl("^data.", names(dfInterventions)))]
  d <- as.list(d)
  names(d) <- gsub("^data.", "", names(d))

  clessnhub::create_item(
    'agoraplus_interventions',
    key = paste(dfInterventions$key[a], "-v3beta", sep=""),
    type = dfInterventions$type[a],
    schema = "v3beta",
    metadata = m,
    data = d
  )
}
