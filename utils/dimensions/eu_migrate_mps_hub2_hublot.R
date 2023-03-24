library(dplyr)


# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

scriptname <- "build_eu_mps"
logger <- clessnverse::log_init("build_eu_mps", "console", "~/logs")


df_countries <- clessnverse::get_warehouse_table(
  table_name = 'countries',
  data_filter = list(), 
  credentials = credentials,
  nbrows = 0
)


# create filter
filter <- clessnhub::create_filter(type="mp", schema="eu_mp_v1", metadata=list(institution="European Parliament"), data=NULL)
# Now get the data
df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

names(df_persons)[which(names(df_persons) == "data.gender")] <- "female"
names(df_persons)[which(names(df_persons) == "data.country")] <- "country"
names(df_persons)[which(names(df_persons) == "data.full_name")] <- "full_name"
names(df_persons)[which(names(df_persons) == "data.pol_group")] <- "pol_group"
names(df_persons)[which(names(df_persons) == "data.party")] <- "party"
names(df_persons)[which(names(df_persons) == "metadata.institution")] <- "institution"
names(df_persons)[which(names(df_persons) == "metadata.twitterAccountHasBeenScraped")] <- "_tweetsacquired"
names(df_persons)[which(names(df_persons) == "key")] <- "_id"
names(df_persons)[which(names(df_persons) == "data.other_names")] <- "other_names"
df_persons$uuid <- NULL
df_persons$metadata.source <- NULL
df_persons$schema <- NULL

for (i in 1:nrow(df_persons)) {
   p <- df_persons[i,]

   p$full_name <- paste(p$data.last_name, p$data.first_name, sep = ', ')
   p$full_name <- trimws(gsub("NA,", "", p$full_name))
   p$full_name <- trimws(gsub(", NA", "", p$full_name))

   
   p$female <- if (p$female == "female") 1 else 0

   p$country <- unique(df_countries$short_name_3[df_countries$name == p$country])

   p$.tweetsacquired <- 0

   p$other_names <- paste(p$other_names, p$full_name, sep = " | ")

   p$type
   p$union <- "EU"
   p$province_or_state <- NA
   p$twitter_handle <- NA
   p$party
   p$pol_group
   p$media <- NA
   p$institution
   p$district <- NA
   p$committee <- NA
   p$history <- NA
   p$wikipedia_url <- paste("https://fr.wikipedia.org/wiki/", p$data.first_name, "_", p$data.last_name, sep = "")

   p$data.first_name <- NULL
   p$data.last_name <- NULL


    clessnverse::commit_warehouse_row(
    table="people",
    key=p$.id,
    row=list(
       country=p$country, full_name=p$full_name, female=p$female, other_names=p$other_names, party=p$party,
       pol_group=p$pol_group, .id=p$.id, institution=p$institution, .tweetsacquired=p$.tweetsacquired, type=p$type,
       union=p$union, province_or_state=p$province_or_state, twitter_handle=p$twitter_handle, media=p$media, district=p$district,
       committee=p$committee, history=p$history, wikipedia_url=p$wikipedia_url
    ),
    refresh_data=TRUE,
    credentials=credentials
  )
}
