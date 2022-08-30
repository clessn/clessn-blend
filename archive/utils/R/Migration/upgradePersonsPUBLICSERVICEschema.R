clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))


df <- clessnhub::get_items('persons', list("type"="public_service"))

for (i in 1:nrow(df)) {
  
  #if (df$metadata.source[i] == "http://www.assnat.qc.ca/fr/deputes/index.html#listeDeputes") {
  #  institution <- "National Assembly of Quebec"
  #}
  #if (df$metadata.source[i] == "manual") {
  #  institution <- "National Assembly of Quebec"
  #}
  #if (df$metadata.source[i] == "scrapeCanadaMPs.R" || df$metadata.source[i] == "https://www.ourcommons.ca/members/en/search#") {
  #  institution <- "House of Commons of Canada"
  #  df$metadata.source[i] <- "https://www.ourcommons.ca/members/en/search#"
  #}
  
  
  if (!grepl(",", df$data.fullName[i])) {
    df$data.fullName[i] <- paste(df$data.lastName[i], df$data.firstName[i], sep=", ")
  }
  
  #if (df$metadata.province_or_state[i] == "Ontario") { 
  #  df$metadata.province_or_state[i] <- "ON"
  #  df$data.currentProvinceOrState[i] <- "ON"
  #  df$metadata.province_or_state[i] <- "ON"
  #}
  
  # if (df$metadata.province_or_state[i] == "Alberta") { 
  #   df$metadata.province_or_state[i] <- "AB"
  #   df$data.currentProvinceOrState[i] <- "AB"
  #   df$metadata.province_or_state[i] <- "AB"
  # }
  
  # if (df$metadata.province_or_state[i] == "British Columbia") { 
  #   df$metadata.province_or_state[i] <- "BC"
  #   df$data.currentProvinceOrState[i] <- "BC"
  #   df$metadata.province_or_state[i] <- "BC"
  # }
  
  # if (df$metadata.province_or_state[i] == "Manitoba") { 
  #   df$metadata.province_or_state[i] <- "MB"
  #   df$data.currentProvinceOrState[i] <- "MB"
  #   df$metadata.province_or_state[i] <- "MB"
  # }
  
  # if (df$metadata.province_or_state[i] == "New Brunswick") { 
  #   df$metadata.province_or_state[i] <- "NB"
  #   df$data.currentProvinceOrState[i] <- "NB"
  #   df$metadata.province_or_state[i] <- "NB"
  # }
  
  # if (df$metadata.province_or_state[i] == "Newfoundland and Labrador") { 
  #   df$metadata.province_or_state[i] <- "NL"
  #   df$data.currentProvinceOrState[i] <- "NL"
  #   df$metadata.province_or_state[i] <- "NL"
  # }
  
  # if (df$metadata.province_or_state[i] == "Northwest Territories") { 
  #   df$metadata.province_or_state[i] <- "NT"
  #   df$data.currentProvinceOrState[i] <- "NT"
  #   df$metadata.province_or_state[i] <- "NT"
  # }
  
  # if (df$metadata.province_or_state[i] == "Nova Scotia") { 
  #   df$metadata.province_or_state[i] <- "NS"
  #   df$data.currentProvinceOrState[i] <- "NS"
  #   df$metadata.province_or_state[i] <- "NS"
  # }
  
  # if (df$metadata.province_or_state[i] == "Nunavut") { 
  #   df$metadata.province_or_state[i] <- "NU"
  #   df$data.currentProvinceOrState[i] <- "NU"
  #   df$metadata.province_or_state[i] <- "NU"
  # }
  
  # if (df$metadata.province_or_state[i] == "Prince Edward Island") { 
  #   df$metadata.province_or_state[i] <- "PE"
  #   df$data.currentProvinceOrState[i] <- "PE"
  #   df$metadata.province_or_state[i] <- "PE"
  # }
  
  # if (df$metadata.province_or_state[i] == "Quebec") { 
  #   df$metadata.province_or_state[i] <- "QC"
  #   df$data.currentProvinceOrState[i] <- "QC"
  #   df$metadata.province_or_state[i] <- "QC"
  # }
  
  # if (df$metadata.province_or_state[i] == "Saskatchewan") { 
  #   df$metadata.province_or_state[i] <- "SK"
  #   df$data.currentProvinceOrState[i] <- "SK"
  #   df$metadata.province_or_state[i] <- "SK"
  # }
  
  # if (df$metadata.province_or_state[i] == "Yukon") { 
  #   df$metadata.province_or_state[i] <- "YT"
  #   df$data.currentProvinceOrState[i] <- "YT"
  #   df$metadata.province_or_state[i] <- "YT"
  # }
  
  data_to_commit <- list(firstName = df$data.firstName[i], 
                         lastName = df$data.lastName[i],
                         fullName = df$data.fullName[i], 
                         isFemale = df$data.isFemale[i],
                         twitterID = df$data.twitterID[i], 
                         twitterHandle = gsub("@", "", df$data.twitterHandle[i]), 
                         currentFunctionsList = df$data.currentFunctionsList[i], 
                         twitterAccountProtected = df$data.twitterAccountProtected[i])
  
  #if (!is.na(df$metadata.location[i])) {
  metadata_to_commit <- list(source = df$metadata.source[i], 
                             country = "CA",
                             province_or_state = "QC")
  #} else {
  #  metadata_to_commit <- list(source = df$metadata.source[i], 
  #                             country = df$metadata.country[i],
  #                             province_or_state = df$metadata.province_or_state[i],
  #                             institution = institution)
  #}
  

  
  clessnhub::delete_item('persons', df$key[i])
  clessnhub::create_item('persons', key = df$key[i], type = df$type[i], schema = df$schema[i], metadata_to_commit, data_to_commit)
  cat("\n")
  cat("===========================================================================\n")
  cat(i, "\n")
  cat(paste(metadata_to_commit, collapse = "*"), "\n")
  cat(paste(data_to_commit, collapse = "*"), "\n")
}

