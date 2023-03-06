library(dplyr)

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

scriptname <- "build_eu_mps"
logger <- clessnverse::log_init("push_eu_mps_hub3", "console", "~/logs")


clessnverse::dbxDownloadFile("/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-europe/df_people_eu_mps.csv", "./", Sys.getenv("DROPBOX_TOKEN"))
df_people <- read.csv("./df_people_eu_mps.csv")
if (file.exists("./df_people_eu_mps.csv")) file.remove(("./df_people_eu_mps.csv"))
df_people$X <- NULL

refresh <- TRUE



for (i in 1:nrow(df_people)) {
  row <- df_people[i,]
  row_data <- as.list(row)
  row_metadata <- list(
      source = "https://www.europarl.europa.eu/meps/fr/download/advanced/xml?name=",
      institution = "European Parliament",
      twitterAccountHasBeenScraped= "0"
  )

  match <- stringr::str_match(row$full_name, "\\(.*")[1]
  match <- gsub("\\(", "\\\\(", match)
  if (!is.na(match)) {
    key <- gsub(match, "", row$full_name)
    key <- trimws(key)
  } else {
    key <- row$full_name
  }

  key <- gsub("'|’|\\.", "", key)

  key <- clessnverse::rm_accents(gsub(" ", "_", tolower(key)))

  tryCatch(
    {
      clessnhub::create_item(
        table = 'persons',
        key = key,
        type = "mp",
        schema = "eu_mp_v1",
        metadata = row_metadata,
        data = row_data
      )
    },

    error = function(e) {
      if (refresh) {
        clessnverse::logit(scriptname, paste("refreshing", key), logger)

        clessnhub::edit_item(
          table = 'persons', 
          key = key,
          type = "mp", 
          schema = "eu_mp_v1",
          metadata = row_metadata,
          data = row_data
        )
      } else {
        clessnverse::logit(scriptname,  paste("skipping", key, "because already exists"), logger)
      }
    },

    finally = {
      clessnverse::logit(scriptname, paste("written", key), logger)
    }

  )  
}

