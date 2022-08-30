clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))


mps <- clessnhub::get_items('persons', list("type"="mp"))

for (i in 2:nrow(mps)) {
  
  if (mps$metadata.source[i] == "http://www.assnat.qc.ca/fr/deputes/index.html#listeDeputes") {
    institution <- "National Assembly of Quebec"
  }
  if (mps$metadata.source[i] == "manual") {
    institution <- "National Assembly of Quebec"
  }
  if (mps$metadata.source[i] == "scrapeCanadaMPs.R" || mps$metadata.source[i] == "https://www.ourcommons.ca/members/en/search#") {
    institution <- "House of Commons of Canada"
    mps$metadata.source[i] <- "https://www.ourcommons.ca/members/en/search#"
  }
  
  
  if (!grepl(",", mps$data.fullName[i])) {
    mps$data.fullName[i] <- paste(mps$data.lastName[i], mps$data.firstName[i], sep=", ")
  }
  
  if (mps$metadata.province_or_state[i] == "Ontario") { 
    mps$metadata.province_or_state[i] <- "ON"
    mps$data.currentProvinceOrState[i] <- "ON"
    mps$metadata.province_or_state[i] <- "ON"
  }
  
  if (mps$metadata.province_or_state[i] == "Alberta") { 
    mps$metadata.province_or_state[i] <- "AB"
    mps$data.currentProvinceOrState[i] <- "AB"
    mps$metadata.province_or_state[i] <- "AB"
  }
  
  if (mps$metadata.province_or_state[i] == "British Columbia") { 
    mps$metadata.province_or_state[i] <- "BC"
    mps$data.currentProvinceOrState[i] <- "BC"
    mps$metadata.province_or_state[i] <- "BC"
  }
  
  if (mps$metadata.province_or_state[i] == "Manitoba") { 
    mps$metadata.province_or_state[i] <- "MB"
    mps$data.currentProvinceOrState[i] <- "MB"
    mps$metadata.province_or_state[i] <- "MB"
  }
  
  if (mps$metadata.province_or_state[i] == "New Brunswick") { 
    mps$metadata.province_or_state[i] <- "NB"
    mps$data.currentProvinceOrState[i] <- "NB"
    mps$metadata.province_or_state[i] <- "NB"
  }
  
  if (mps$metadata.province_or_state[i] == "Newfoundland and Labrador") { 
    mps$metadata.province_or_state[i] <- "NL"
    mps$data.currentProvinceOrState[i] <- "NL"
    mps$metadata.province_or_state[i] <- "NL"
  }
  
  if (mps$metadata.province_or_state[i] == "Northwest Territories") { 
    mps$metadata.province_or_state[i] <- "NT"
    mps$data.currentProvinceOrState[i] <- "NT"
    mps$metadata.province_or_state[i] <- "NT"
  }
  
  if (mps$metadata.province_or_state[i] == "Nova Scotia") { 
    mps$metadata.province_or_state[i] <- "NS"
    mps$data.currentProvinceOrState[i] <- "NS"
    mps$metadata.province_or_state[i] <- "NS"
  }
  
  if (mps$metadata.province_or_state[i] == "Nunavut") { 
    mps$metadata.province_or_state[i] <- "NU"
    mps$data.currentProvinceOrState[i] <- "NU"
    mps$metadata.province_or_state[i] <- "NU"
  }
  
  if (mps$metadata.province_or_state[i] == "Prince Edward Island") { 
    mps$metadata.province_or_state[i] <- "PE"
    mps$data.currentProvinceOrState[i] <- "PE"
    mps$metadata.province_or_state[i] <- "PE"
  }
  
  if (mps$metadata.province_or_state[i] == "Quebec") { 
    mps$metadata.province_or_state[i] <- "QC"
    mps$data.currentProvinceOrState[i] <- "QC"
    mps$metadata.province_or_state[i] <- "QC"
  }
  
  if (mps$metadata.province_or_state[i] == "Saskatchewan") { 
    mps$metadata.province_or_state[i] <- "SK"
    mps$data.currentProvinceOrState[i] <- "SK"
    mps$metadata.province_or_state[i] <- "SK"
  }
  
  if (mps$metadata.province_or_state[i] == "Yukon") { 
    mps$metadata.province_or_state[i] <- "YT"
    mps$data.currentProvinceOrState[i] <- "YT"
    mps$metadata.province_or_state[i] <- "YT"
  }
  
  data_to_commit <- list(firstName = mps$data.firstName[i], 
                         lastName = mps$data.lastName[i],
                         fullName = mps$data.fullName[i], 
                         isFemale = mps$data.isFemale[i],
                         currentFunctionsList = mps$data.currentFunctionsList[i], 
                         currentParty = mps$data.currentParty[i],
                         currentDistrict = mps$data.currentDistrict[i], 
                         currentProvinceOrState = mps$data.currentProvinceOrState[i],
                         isMinister = mps$data.isMinister[i], 
                         currentMinister = mps$data.currentMinister[i],
                         twitterHandle = mps$data.twitterHandle[i], 
                         twitterID = mps$data.twitterID[i], 
                         twitterAccountProtected = mps$data.twitterAccountProtected[i])
  
  if (!is.na(mps$metadata.location[i])) {
    metadata_to_commit <- list(source = mps$metadata.source[i], 
                               country = mps$metadata.location[i],
                               province_or_state = mps$metadata.province_or_state[i],
                               institution = institution)
  } else {
    metadata_to_commit <- list(source = mps$metadata.source[i], 
                               country = mps$metadata.country[i],
                               province_or_state = mps$metadata.province_or_state[i],
                               institution = institution)
  }
  

  
  clessnhub::delete_item('persons', mps$key[i])
  clessnhub::create_item('persons', key = mps$key[i], type = mps$type[i], schema = mps$schema[i], metadata_to_commit, data_to_commit)
  cat("\n")
  cat("===========================================================================\n")
  cat(paste(metadata_to_commit, collapse = "*"), "\n")
  cat(paste(data_to_commit, collapse = "*"), "\n")
}

