library(dplyr)

scriptname <- "get_europe_interventions"
opt <- list(dataframe_mode = "update", log_output = c("console"), hub_mode = "skip", download_data = TRUE, translate=TRUE)
logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


my_filter <- clessnhub::create_filter(
  type="parliament_debate", 
  #schema="v2", 
  metadata=list(
    location="EU", 
    format="xml"),
  data=list(
    eventID = "920221213EN"
  )
)

df <- clessnhub::get_items(
  table = 'agoraplus_interventions',
  filter = my_filter,
  download_data = TRUE,
  max_pages = -1
)


for (i in 1:nrow(df)) {
  clessnhub::delete_item('agoraplus_interventions', df$key[i])
}


#df <- data.frame(full_name=dfInterventions$data.speakerFullName, party = dfInterventions$data.speakerParty, pol_group = dfInterventions$data
#    .speakerPolGroup, country = dfInterventions$data.speakerCountry)

df <- dfInterventions %>% filter(is.na(data.speakerCountry) | is.na(data.speakerParty) | is.na(data.speakerPolGroup))

df1 <- unique(df)

which(is.na(df1$data.speakerPolGroup)) %>% length()
which(is.na(df1$data.speakerParty)) %>% length()
which(is.na(df1$data.speakerCountry)) %>% length()

write.csv2(df1, "missingvalues.csv")
getwd()
ncol(df1)


######################
# Translation APIs


#just-translated
library(httr)
url <- "https://just-translated.p.rapidapi.com/"
queryString <- list(
  lang = "en",
  text = dfInterventions$data.interventionText[135483]
)
response <- VERB(
    "GET", 
    url, 
    add_headers(
        'X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775',
        'X-RapidAPI-Host' = 'just-translated.p.rapidapi.com'),
        query = queryString,
        content_type("application/octet-stream")
    )


#just-translated
library(httr)
url <- "https://just-translated.p.rapidapi.com/"
queryString <- list(
  lang = "fr",
  text = "Hello, how are you?"
)
response <- VERB("GET", url, add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 'X-RapidAPI-Host' = 'just-translated.p.rapidapi.com'), query = queryString, content_type("application/octet-stream"))
content(response, "text")



#ibmwatsonlanguagetranslator
library(httr)
url <- "https://ibmwatsonlanguagetranslatordimasv1.p.rapidapi.com/translateByModelId"
payload <- "modelId=%3CREQUIRED%3E&username=%3CREQUIRED%3E&text=%3CREQUIRED%3E&password=%3CREQUIRED%3E"
encode <- "form"
response <- VERB("POST", url, body = payload, add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 'X-RapidAPI-Host' = 'IBMWatsonLanguageTranslatordimasV1.p.rapidapi.com'), content_type("application/x-www-form-urlencoded"), encode = encode)
content(response, "text")





#translef
library(httr)
url <- "https://translef-translator.p.rapidapi.com/translate/text"
payload <- paste("language_code=de&text=", dfInterventions$data.interventionText[135483])
encode <- "form"
response <- VERB("POST", url, body = payload, add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 'X-RapidAPI-Host' = 'translef-translator.p.rapidapi.com'), content_type("application/x-www-form-urlencoded"), encode = encode)
content(response, "text")



# Translo : excellent mais limité à 20Millions de chr par mois
library(httr)
url <- "https://translo.p.rapidapi.com/api/v3/detect"
queryString <- list(text = dfInterventions$data.interventionText[135483])
response <- VERB("GET", url, add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 'X-RapidAPI-Host' = 'translo.p.rapidapi.com'), query = queryString, content_type("application/octet-stream"))
content(response, "text")

url <- "https://translo.p.rapidapi.com/api/v3/translate"
payload <- paste("from=sk&to=en&text=",dfInterventions$data.interventionText[135483])
encode <- "form"
response <- VERB("POST", url, body = payload, add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 'X-RapidAPI-Host' = 'translo.p.rapidapi.com'), content_type("application/x-www-form-urlencoded"), encode = encode)
content(response, "text")



# Text Translator 
library(httr)
url <- "https://text-translator2.p.rapidapi.com/translate"
payload <- paste("source_language=en&target_language=fr&text=",gsub(" ", "%20", substring(dfInterventions$data.interventionText[99709],1, 2900)))
encode <- "form"
response <- VERB("POST", url, body = payload, add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 'X-RapidAPI-Host' = 'text-translator2.p.rapidapi.com'), content_type("application/x-www-form-urlencoded"), encode = encode)
content(response, "text")


#Deep Translate
library(httr)
url <- "https://deep-translate1.p.rapidapi.com/language/translate/v2/detect"
response <- VERB(
    "POST", 
    url, 
    body= "{\"q\":\"a utečencov. Včasným vyčlenením umožňujeme  Komisii pružnejšie reagovať a riešiť tieto problémy.\"}",
    add_headers(
        'X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775',
        'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'),
        content_type("application/octet-stream"))
content(response, "text")

library(httr)
url <- "https://deep-translate1.p.rapidapi.com/language/translate/v2"
payload <- paste(
    "{
    \"q\":\"", dfInterventions$data.interventionText[135483],
    "\",\"source\": \"sk\",
    \"target\": \"en\"
}")
encode <- "json"
response <- VERB(
    "POST", 
    url, 
    body = payload,
    add_headers('X-RapidAPI-Key' = '21924b6e03msha14285d0411bf59p162e3ajsn902945780775', 
    'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'), 
    content_type("application/json"), 
    encode = encode)
content(response, "text")
