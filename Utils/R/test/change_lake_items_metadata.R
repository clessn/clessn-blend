commit_lake_item <- function(data, metadata, mode, credentials) {

  if (!is.null(data$item)) {
    # This is the object "item" that we will commit to the lake item
    handle_file <- TRUE
    if (grepl("file", metadata$format)) {
      metadata$format <- gsub("file", "", metadata$format)
      file.rename(data$item, paste("file.", metadata$format, sep=""))
    } else {
      write(data$item, paste("file.", metadata$format, sep=""))
    }
  } else {
    if (!is.null(data$file)){
      # This is an file_url to the file that we will commit to the lake item
      # There is not much to do here but update
      handle_file <- FALSE
    } else {
      stop("invalid item or file provided in data to the clessnverse::commit_lake_item function")
    }
  }


  # check if an item with this key already exists
  existing_item <- hublot::filter_lake_items(credentials, list(key = data$key))

  if (length(existing_item$results) == 0) {
    #clessnverse::log_activity(message = paste("creating new item", data$key, "in data lake", data$path), logger = logger)
    hublot::add_lake_item(
      body = list(
        key = data$key,
        path = data$path,
        file = httr::upload_file(paste("file.", metadata$format, sep=""))
        metadata = jsonlite::toJSON(metadata, auto_unbox = T)),
      credentials)
  } else {
    if (mode == "refresh") {
      # hublot::remove_lake_item(existing_item$results[[1]]$id, credentials)
      #
      # hublot::add_lake_item(
      #   body = list(
      #     key = data$key,
      #     path = data$path,
      #     file = httr::upload_file(paste("file.", metadata$format, sep="")),
      #     metadata = jsonlite::toJSON(metadata, auto_unbox = T)),
      #   credentials)
      hublot::update_lake_item(
        id = existing_item$results[[1]]$id,
        body = list(
          key = data$key,
          path = data$path,
          file = httr::upload_file(paste("file.", metadata$format, sep=""))
          metadata = jsonlite::toJSON(metadata, auto_unbox = T)),
        credentials)

    } else {
      warning(paste("not updating existing item", data$key, "in data lake", data$path, "because mode is", mode))
    }
  }

  if (file.exists(paste("file.", metadata$format, sep=""))) file.remove(paste("file.", metadata$format, sep=""))
}




# get credentials from hublot
credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"),
  Sys.getenv("HUB3_USERNAME"),
  Sys.getenv("HUB3_PASSWORD")
  )

# filter for selecting the lakes items to be changed
filter <- list(
  path = "political_party_press_releases",
  metadata__political_party = "CAQ",
  metadata__province_or_state="QC", 
  metadata__country="CAN", 
  metadata__storage_class="lake"
)

data <- hublot::filter_lake_items(credentials, filter = filter)

if (length(data$results) > 0) {
    for (i in 1:length(data$results)) {
        row <- data$results[[i]]
        row$metadata <- new_metadata

        clessnverse::commit_lake_item(
          list(
            key = row$key,
            path = row$path,
            file = row$file
          ),
          new_metadata,
          "refresh",
          credentials
          )
    }
} else {
    warning("no lake item was retrived with this filter")
}


row$key

