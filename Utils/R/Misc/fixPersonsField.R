clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

myfilter <- clessnhub::create_filter(type="candidate", schema="v2")
dfC <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="mp", schema="v2", metadata = list("institution"="House of Commons of Canada"))
dfM <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="mp", schema="v2", metadata = list("institution"="National Assembly of Quebec"))
dfQM <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="political_party", schema="v1")
dfP <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="journalist", schema="v2")
dfJ <- clessnhub::get_items('persons', myfilter)

df <- dfC %>% dplyr::full_join(dfM) %>% dplyr::full_join(dfQM) %>% dplyr::full_join(dfP)  %>% dplyr::full_join(dfJ)
cnt <- 0

for (i in 1:nrow(df)) {
  
  key <- df$key[i]
  type <- df$type[i]
  schema <- df$schema[i]
  
  if (grepl("Minister of", df$data.currentFunctionsList[i])) {
  #if (!is.na(df$data.isFemale[i])) {
  #  if (df$data.isFemale[i] == "True" || df$data.isFemale[i] == "False" || df$data.isFemale[i] == TRUE || df$data.isFemale[i] == FALSE) {
      cnt <- cnt + 1
      
  #    if (df$data.isFemale[i] == TRUE) df$data.isFemale[i] <- 1
  #    if (df$data.isFemale[i] == FALSE) df$data.isFemale[i] <- 0
  #    if (df$data.isFemale[i] == "True") df$data.isFemale[i] <- 1
  #    if (df$data.isFemale[i] == "False") df$data.isFemale[i] <- 0
      df$data.currentMinister <- 
      
      data <- as.list(df[i,which(grepl("^data.",names(df[i,])))])
      names(data) <- gsub("^data.", "", names(data))
      metadata <- as.list(df[i,which(grepl("^metadata.",names(df[i,])))])
      names(metadata) <- gsub("^metadata.", "", names(metadata))
    
      cat('\n\n\n',i," *** ",cnt,'\n')
      cat('=================================================================================\n')
      cat(df$key[i], df$type[i], df$schema[i],'\n')
      cat(paste(names(metadata), collapse = ' * '), '\n')
      cat(paste(metadata, collapse = ' * '), '\n')
      cat(paste(names(data), collapse = ' * '), '\n')
      cat(paste(data, collapse = ' * '),'\n')
      #clessnhub::edit_item('persons', key = key, type = type, schema = schema, metadata = metadata, data = data)
  #  }
  }
}
