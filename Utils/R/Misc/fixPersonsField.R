clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

myfilter <- clessnhub::create_filter(type="candidate", schema="v2")
dfPersons <- clessnhub::get_items('persons', myfilter)


cnt <- 0

for (i in 1:nrow(dfPersons)) {
  
  key <- dfPersons$key[i]
  type <- dfPersons$type[i]
  schema <- dfPersons$schema[i]
  
  if (!is.na(dfPersons$data.isFemale[i])) {
    if (dfPersons$data.isFemale[i] == "True" || dfPersons$data.isFemale[i] == "False" || dfPersons$data.isFemale[i] == TRUE || dfPersons$data.isFemale[i] == FALSE) {
      cnt <- cnt + 1
      
      if (dfPersons$data.isFemale[i] == TRUE) dfPersons$data.isFemale[i] <- 1
      if (dfPersons$data.isFemale[i] == FALSE) dfPersons$data.isFemale[i] <- 0
      if (dfPersons$data.isFemale[i] == "True") dfPersons$data.isFemale[i] <- 1
      if (dfPersons$data.isFemale[i] == "False") dfPersons$data.isFemale[i] <- 0
      
      data <- as.list(dfPersons[i,which(grepl("^data.",names(dfPersons[i,])))])
      names(data) <- gsub("^data.", "", names(data))
      metadata <- as.list(dfPersons[i,which(grepl("^metadata.",names(dfPersons[i,])))])
      names(metadata) <- gsub("^metadata.", "", names(metadata))
    
      cat('\n\n\n',i," *** ",cnt,'\n')
      cat('=================================================================================\n')
      cat(dfPersons$key[i], dfPersons$type[i], dfPersons$schema[i],'\n')
      cat(paste(names(metadata), collapse = ' * '), '\n')
      cat(paste(metadata, collapse = ' * '), '\n')
      cat(paste(names(data), collapse = ' * '), '\n')
      cat(paste(data, collapse = ' * '),'\n')
      #clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
    }
  }
}
