is.na.char <- function(x) {
  return (x=="NA")
}

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
filter <- clessnhub::create_filter(type="mp", schema="v3")  
df <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

n <- 1

for (i in 1:nrow(df)) {
  
  clessnhub::delete_item('persons', df$key[i])
  
  # if (!is.na(df$data.currentFunctionsList[i])) {
  #   cat(n, " found ", i, df$key[i], "\n\n")
  #   n <- n + 1
  #   
  #   data_to_commit <- as.list(df[i,which(grepl("^data.",names(df[i,])))])
  #   names(data_to_commit) <- gsub("^data.", "", names(data_to_commit))
  #   metadata_to_commit <- as.list(df[i,which(grepl("^metadata.",names(df[i,])))])
  #   names(metadata_to_commit) <- gsub("^metadata.", "", names(metadata_to_commit))
  #   
  #   data_to_commit$currentFunctionsList <- NA_character_
  #   
  #   data_to_commit[sapply(data_to_commit,is.null)] <- NA_character_ 
  #   metadata_to_commit[sapply(metadata_to_commit,is.null)] <- NA_character_
  #   data_to_commit[sapply(data_to_commit,is.na)] <- NA_character_ 
  #   metadata_to_commit[sapply(metadata_to_commit,is.na)] <- NA_character_
  #   data_to_commit[sapply(data_to_commit,is.na.char)] <- NA_character_ 
  #   metadata_to_commit[sapply(metadata_to_commit,is.na.char)] <- NA_character_
  #   
  #   clessnhub::edit_item('persons', key = df$key[i], type = df$type[i], schema = df$schema[i], metadata = metadata_to_commit, data = data_to_commit)
  #   
  # }
  
}
