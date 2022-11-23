library(dplyr)

scriptname <- "get_canada_interventions"
opt <- list(dataframe_mode = "update", log_output = c("console"), hub_mode = "skip", download_data = TRUE, translate=TRUE)
logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))


dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(
  type = "parliament_debate", 
  schema = "v2", 
  location = "CA", 
  format = "xml",
  download_data = TRUE,
  token = Sys.getenv('HUB_TOKEN')
)

