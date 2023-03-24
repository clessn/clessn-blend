# connect to hublot
credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))


clessnverse::dbxDownloadFile("/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-europe/df_people_eu_mps_camille_flavie.csv", "./", Sys.getenv("DROPBOX_TOKEN"))
df <- read.csv2("./df_people_eu_mps_camille_flavie.csv")
if (file.exists("./df_people_eu_mps_camille_flavie.csv")) file.remove(("./df_people_eu_mps_camille_flavie.csv"))
df$X <- NULL


df_countries <- clessnverse::get_warehouse_table(
  table_name = 'countries',
  data_filter = list(), 
  credentials = credentials,
  nbrows = 0
)

for (i in 21:nrow(df)) {
  p <- df[i,]


  p$other_names <- if (!grepl(p$full_name, p$other_names)) paste(p$other_names, p$full_name, sep = " | ") else p$other_names


  match <- stringr::str_match(p$last_name, "^([A-Z])\\s.*$")[[2]]
  if ( !is.na(match) && nchar(match) == 1 ) p$last_name <- trimws(gsub(paste("^",match,sep=""), "", p$last_name))


  full_name_forward <- paste(p$first_name, p$last_name)
  match <- stringr::str_match(full_name_forward, "\\(.*")[1]
  match <- gsub("\\(", "\\\\(", match)
  if (!is.na(match)) {
    key <- gsub(match, "",full_name_forward)
    key <- trimws(key)
  } else {
    key <- full_name_forward
  }

  key <- gsub("'|’|\\.", "", key)
  key <- clessnverse::rm_accents(gsub(" ", "_", tolower(key)))


  p$full_name <- paste(p$last_name, p$first_name, sep = ', ')
  p$full_name <- trimws(gsub("NA,", "", p$full_name))
  p$full_name <- trimws(gsub(", NA", "", p$full_name))



  if (length(unique(df_countries$short_name_3[df_countries$name == p$country])) > 0) {
      p$country <- unique(df_countries$short_name_3[df_countries$name == p$country])
  } else {
    p$country
  }

  p$.tweetsacquired <- 0
  p$.id <- key

  p$other_names <- if (!grepl(p$full_name, p$other_names)) paste(p$other_names, p$full_name,  sep = " | ") else p$other_names
  p$other_names <- if (!grepl(paste(p$first_name, p$last_name), p$other_names)) paste(p$other_names, paste(p$first_name, p$last_name),  sep = " | ") else p$other_names
  

  p$type <- if (!is.na(p$pol_group) && !grepl("unknown", p$pol_group)) "mp" else NA
  p$union <- "EU"
  p$province_or_state <- NA
  p$twitter_handle <- NA
  p$party
  p$pol_group
  p$media <- NA
  p$institution <- "European Parliament"
  p$district <- NA
  p$committee <- NA
  p$history <- NA
  p$wikipedia_url <- paste("https://fr.wikipedia.org/wiki/", p$first_name, "_", p$last_name, sep = "")
  p$wikipedia_url <- gsub(" ", "_", p$wikipedia_url )

  p$data.first_name <- NULL
  p$data.last_name <- NULL

  
  row = list(
       full_name=p$full_name, gender=p$gender, other_names=p$other_names, country=p$country, party=p$party,
       pol_group=p$pol_group, .id=p$.id, institution=p$institution, .tweetsacquired=p$.tweetsacquired, type=p$type,
       union=p$union, province_or_state=p$province_or_state, twitter_handle=p$twitter_handle, media=p$media, district=p$district,
       committee=p$committee, history=p$history, wikipedia_url=p$wikipedia_url
  )

  print(paste("pushing", paste(names(row), row, sep="=", collapse=" ")))

  clessnverse::commit_warehouse_row(
    table = "people",
    key = key, 
    row = row,
    refresh_data = T,
    credentials = credentials
  )
}
