clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

myfilter <- clessnhub::create_filter(type="mp", schema="v2")#, metadata=list("metadata.institution"="House of Commons of Canada"))
dfPersons <- clessnhub::get_items('persons')


for (i in 1:nrow(dfPersons)) {
  
  key <- df_existing_persons$key[matching_existing_persons_row]
  type <- df_existing_persons$type[matching_existing_persons_row]
  schema <- df_existing_persons$schema[matching_existing_persons_row]
  data <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^data.",names(df_existing_persons[matching_existing_persons_row,])))])
  names(data) <- gsub("^data.", "", names(data))
  metadata <- as.list(df_existing_persons[matching_existing_persons_row,which(grepl("^metadata.",names(df_existing_persons[matching_existing_persons_row,])))])
  names(metadata) <- gsub("^metadata.", "", names(metadata))
  cat('updating mp twitter handle for', full_name, "key", key, "handle", twitter_handle, '\n', sep=' ')
  
  
}

