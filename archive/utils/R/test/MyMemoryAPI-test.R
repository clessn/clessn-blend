library(httr)

url <- "https://translated-mymemory---translation-memory.p.rapidapi.com/api/get"

queryString <- list(
  langpair = "ru|en",
  q = "Андрей Слабаков",
  mt = "1",
  onlyprivate = "0",
  de = "patrick@infoscope.ca"
)

headers <- httr::add_headers('x-rapidapi-host' = "translated-mymemory---translation-memory.p.rapidapi.com", 
                             'x-rapidapi-key' = "21924b6e03msha14285d0411bf59p162e3ajsn902945780775")

response <- httr::VERB("GET", 
                       url, 
                       headers,
                       query = queryString, 
                       content_type("application/octet-stream"))
textcat::textcat(queryString$q)
result <- content(response, "text")
translatedText  <- jsonlite::parse_json(result)
translatedText$matches[[1]]$translation
