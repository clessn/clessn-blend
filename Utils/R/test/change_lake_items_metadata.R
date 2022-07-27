
# get credentials from hublot
credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"),
  Sys.getenv("HUB3_USERNAME"),
  Sys.getenv("HUB3_PASSWORD")
  )

table <- clessnverse::get_warehouse_table("globales_canada", credentials = credentials, nbrows = 50)

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

# filter for selecting the lakes items to be changed
filter <- list(
  path = "political_party_press_releases",
  metadata__province_or_state="QC", 
  metadata__country="CAN", 
  metadata__storage_class="lake"
)

data <- hublot::filter_lake_items(credentials, filter = filter)

if (length(data$results) > 0) {
    for (i in 1:length(data$results)) {
        row <- data$results[[i]]

        new_metadata <- row$metadata
        new_metadata$object_type <- "raw_data"
        new_metadata$source <- new_metadata$url
        new_metadata$url <- NULL
        new_metadata$source_type <- "website"

        clessnverse::commit_lake_item(
          data = list(
            key = row$key,
            path = row$path,
            file = row$file
          ),
          metadata = new_metadata,
          mode = "refresh",
          credentials = credentials
          )
    }
} else {
    warning("no lake item was retrived with this filter")
}


persons <- clessnverse::get_hub2_table('persons', data_filter = NULL, hub_conf = hub_config)

press_releases_freq <- clessnverse::get_mart_table("political_parties_press_releases_freq", data_filter = list(data__political_party = "CAQ"), credentials)

press_releases <- clessnverse::get_warehouse_table("political_parties_press_releases", data_filter = list(data__political_party = "QS"), credentials)



filter <- list(metadata__content_type = "ces_survey", metadata__storage_class = "lake")
hublot::filter_lake_items(credentials, filter)



  filter <- list(type="journalist")
  df_journalists <- clessnverse::get_hub2_table(
    'persons', 
    data_filter = filter,
    max_pages = -1,
    hub_conf = hub_config
  )


  df_interventions <- clessnverse::get_hub2_table(
    'agoraplus_interventions', 
    data_filter = NULL,
    max_pages = 2,
    hub_conf = hub_config
  )
