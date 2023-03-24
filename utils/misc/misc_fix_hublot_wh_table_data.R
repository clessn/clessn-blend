# connect to hublot
credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))



df_people <- clessnverse::get_warehouse_table(
  table_name = "people",
  data_filter = list(
    data__pol_group = "Progressive Alliance of Socialists And Democrats (S&D)"
  ),
  credentials = credentials,
  nbrows = 0
)

df_countries <- clessnverse::get_warehouse_table(
  table_name = 'countries',
  data_filter = list(
  ), 
  credentials = credentials,
  nbrows = 0
)


df <- clessnverse::get_warehouse_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(), 
  credentials = credentials,
  nbrows = 0
)

for (i in 29938:nrow(df)) {
  print (i)
  item <- df[i,]

  item$.schema <- "202303"
  # item$pol_group <- trimws(stringr::str_to_title(item$pol_group))
  # item$party <- trimws(stringr::str_to_title(item$party))

  # if (item$gender == "0") item$gender <- "male"
  # if (item$gender == "1") item$gender <- "female"
  # if (item$gender == "gender") item$gender <- "male"


  # if (tolower(item$country) %in% tolower(df_countries$name)) {
  #     item$country <- unique(df_countries$short_name_3[tolower(df_countries$name) == tolower(item$country)])
  # }
    
  # clessnverse::commit_warehouse_row(
  #   table = "agoraplus_european_parliament",
  #   key = gsub("test", "202303", item$hub.key), 
  #   row = as.list(item[1,c(which(!grepl("hub.",names(item))))]),
  #   refresh_data = T,
  #   credentials = credentials
  # )

  item$intervention_seq_num <- as.numeric(item$intervention_seq_num)

  hublot::update_table_item(
    table_name = "clhub_tables_warehouse_agoraplus_european_parliament",
    id = item$hub.id,
    body = list(
      key = gsub("test", "202303", item$hub.key), 
      timestamp = as.character(Sys.time()), 
      data = jsonlite::toJSON(
        as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
        auto_unbox = T
      )
    ),
    credentials = credentials
  )
}



###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################
###############################################################################

# df_interventions <- clessnverse::get_warehouse_table(
#   table_name = "agoraplus_european_parliament",
#   data_filter = list(),
#   credentials = credentials,
#   nbrows = 0
# )

# colnames(df_interventions)[colnames(df_interventions) == '_url'] <- '.url'
# colnames(df_interventions)[colnames(df_interventions) == 'metadata_lake_item_key'] <- '.lake_item_key'
# colnames(df_interventions)[colnames(df_interventions) == 'metadata_lake_item_path'] <- '.lake_item_path'
# colnames(df_interventions)[colnames(df_interventions) == 'metadata_event_format'] <- '.lake_item_format'
# colnames(df_interventions)[colnames(df_interventions) == 'metadata_schema'] <- '.schema'

# for (i in 1:nrow(df_interventions)) {
#   intervention <- df_interventions[i,]

#   intervention$intervention_seq_num <- as.numeric(gsub("beta", "", intervention$intervention_seq_num))

#   clessnverse::commit_warehouse_row(
#     table = "agoraplus_european_parliament",
#     key = intervention$hub.key, 
#     row = as.list(intervention[1,c(which(!grepl("hub.",names(intervention))))]),
#     refresh_data = T,
#     credentials = credentials
#   )
# }
