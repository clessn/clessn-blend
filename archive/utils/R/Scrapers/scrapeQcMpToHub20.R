clessnhub::connect()

url <- "http://www.assnat.qc.ca/fr/deputes/index.html#listeDeputes"

doc_html <- RCurl::getURL(url)
parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
doc_urls <- XML::xpathSApply(parsed_html, "//a/@href")

doc_names <- XML::xpathSApply(parsed_html, "//a")
doc_names_index <- which(grepl("/fr/deputes/(.*)\\-[0-9]+/index.html", XML::xpathSApply(parsed_html, "//a", XML::xmlAttrs)))
doc_names <- XML::xmlValue(doc_names[doc_names_index])

list_urls <- doc_urls[grep("/fr/deputes/(.*)\\-[0-9]+/index.html", doc_urls)]

df <- data.frame(key=character(), type=character(), schema=character(),
                 source=character(), location=character(),
                 firstName=character(), lastName=character(),
                 fullName=character(), isFemale=character(),
                 currentFunctionsList=character(), currentParty=character(),
                 previousPartiesList=character(), previousPartiesDatesList=character(),
                 currentDistrict=character(), previousDistrictsList=character(),
                 previousDistrictsDatesList=character(), isMinister=character(), 
                 currentMinister=character(), previousMinistersList=character(), 
                 previousMinistersDatesList=character(), twitterHandle=character(), 
                 twitterID=character(), twitterAccountProtected=character())

for ( i_url in 1:length(list_urls)) {
  
  url <- paste("http://www.assnat.qc.ca", list_urls[i_url], sep='')
  doc_html <- RCurl::getURL(url)
  parsed_html <- XML::htmlParse(doc_html, asText = TRUE)
  doc <- XML::xpathApply(parsed_html, '//ul', XML::xmlValue)
  
  mp_full_name <- doc_names[i_url]
  mp_full_name <- stringr::str_trim(mp_full_name)
  mp_last_name <- strsplit(mp_full_name, "\\,\\s")[[1]][1]
  mp_first_name <- strsplit(mp_full_name, "\\,\\s")[[1]][2]

  mp_fiche <- doc[[34]]
  mp_fiche <- gsub("\r\n", "", mp_fiche)
  mp_fiche <- gsub("\t", "", mp_fiche)
  mp_fiche <- gsub("\\s\\s+", "\n", stringr::str_trim(mp_fiche))
  mp_fiche <- strsplit(mp_fiche, "\n")
  mp_fiche <- as.list(mp_fiche[[1]])
  
  if (grepl("jean", tolower(mp_first_name))) {
    mp_is_female <- FALSE
  } else {
    mp_is_female <- gender::gender(stringi::stri_trans_general(strsplit(mp_first_name, "\\s|\\-")[[1]][1], "Latin-ASCII"))$gender == "female"
  }
  
  if ( length(mp_is_female) == 0 ) { mp_is_female <- FALSE }
  if (mp_is_female) mp_is_female <- 1 else mp_is_female <- 0
  
  
  mp_id <- stringr::str_match(url, "http://www.assnat.qc.ca/fr/deputes/(.*)\\-(.*)/index.html")[3]
  
  depute_fiche_index <- grep ("Député", mp_fiche)
  mp_district <- gsub("Députée?\\s(de\\s|d\\'|d\\’|des\\s)", "", mp_fiche[[depute_fiche_index]])
  mp_party <- mp_fiche[[2]]
  mp_is_minister <- TRUE %in% grepl("Ministre", mp_fiche)
  if (mp_is_minister) mp_is_minister <- 1 else mp_is_minister <- 0
  
  mp_functions_list <- paste(mp_fiche, collapse = ' | ')
  
  row_to_commit <- list(firstName = mp_first_name, lastName = mp_last_name,
                        fullName = mp_full_name, isFemale = mp_is_female,
                        currentFunctionsList = mp_functions_list, 
                        currentParty = mp_party,
                        currentDistrict = mp_district, 
                        currentProvinceOrState = "QC",
                        isMinister = mp_is_minister, 
                        currentMinister = NA,
                        twitterHandle = NA, 
                        twitterID = NA, 
                        twitterAccountProtected = NA)
  
  metadata_to_commit <- list(source = "http://www.assnat.qc.ca/fr/deputes/index.html#listeDeputes", country = "CA", province_or_state = "QC")
  
  row <- cbind(data.frame(key=mp_id, type="mp", schema="v2"),
               as.data.frame(metadata_to_commit),
               as.data.frame(row_to_commit)
               )
  
  df <- df %>% rbind(row)
  
  
  tryCatch(
    {test_df <- clessnhub::get_item('persons', mp_id)},
    error = function(e) {test_df <<- NULL}
  )
  
  if (is.null(test_df)) {
    clessnhub::create_item('persons', key = mp_id, type = "mp", schema = "v2", metadata = metadata_to_commit, data = row_to_commit)
  } else {
    clessnhub::edit_item('persons', key = mp_id, type = "mp", schema = "v2", metadata = metadata_to_commit, data = row_to_commit)
  }
}
