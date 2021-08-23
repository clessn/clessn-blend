clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

df <- clessnhub::get_items('agoraplus_press_releases')

indexes <- which(!is.na(df$metadata.date != ""))

for (i in 1:nrow(df)) {
  
  key <- df$key[i]
  type <- df$type[i]
  schema <- df$schema[i]
  
  data <- as.list(df[i,which(grepl("^data.",names(df[i,])))])
  names(data) <- gsub("^data.", "", names(data))
  metadata <- as.list(df[i,which(grepl("^metadata.",names(df[i,])))])
  names(metadata) <- gsub("^metadata.", "", names(metadata))
  
  
  if (!is.na(metadata$date)) metadata$lastUpdatedOn <- metadata$date
  metadata$date <- NULL
  data$Location <- NULL
  data$place <- NULL
  

  cat('\n\n\n',i," *** ",'\n')
  cat('=================================================================================\n')
  cat(df$key[i], df$type[i], df$schema[i],'\n')
  cat(paste(names(metadata), collapse = ' * '), '\n')
  cat(paste(metadata, collapse = ' * '), '\n')
  cat(paste(names(data), collapse = ' * '), '\n')
  cat(paste(data, collapse = ' * '),'\n')
  clessnhub::edit_item('agoraplus_press_releases', key = key, type = type, schema = schema, metadata = metadata, data = data)
}
