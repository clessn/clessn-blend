library(dplyr)


# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

scriptname <- "agora+_get_journalists"
logger <- clessnverse::log_init(scriptname, "console", "~/logs")

# create filter
filter <- clessnhub::create_filter(
   type = "journalist", 
   #schema = "journalist_v3" ou "v2". si non-spécifié, alors tous les schémas, 
   #metadata = list(twitterAccountHasBeenScraped = "1")
   #data = list(isFemale = 1)
)

# Now get the data
df_people <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
