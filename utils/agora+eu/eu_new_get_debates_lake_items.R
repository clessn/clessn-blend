library(dplyr)

scriptname <- "get_europe_lake_items"
logger <- clessnverse::log_init(scriptname, "console", Sys.getenv("LOG_PATH"))

credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD")
)

datalake_path <<- "agoraplus/european_parliament"

r <- hublot::filter_lake_items(
  filter = list(path=datalake_path, metadata__format="xml"),
  credentials = credentials
)


for (lake_item in r$results) {

  current_lake_item <- hublot::retrieve_lake_item(
    id = lake_item$id, 
    credentials = credentials
  )
 
}