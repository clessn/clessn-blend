
# get credentials from hublot
credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"),
  Sys.getenv("HUB3_USERNAME"),
  Sys.getenv("HUB3_PASSWORD")
  )

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
