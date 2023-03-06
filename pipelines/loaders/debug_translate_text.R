text <- readRDS("intervention_text.rds")
engine = "deeptranslate"
source_lang = "el"
target_lang = "en"
translate = TRUE

text <- stringr::str_flatten(text)

text <- clntxt(substr(test1,1,3141))


    key <- Sys.getenv("DEEP_TRANSLATE_KEY")

    # if source_lang = NA let's detect the language first
    if (is.na(source_lang)) source_lang <- clessnverse::detect_language(engine = "deeptranslate", text)

    # translate next
    url <- "https://deep-translate1.p.rapidapi.com/language/translate/v2"

    if (nchar(text) > 3000) {
      # more than 5000 characters
      df <- tidytext::unnest_tokens(
        data.frame(txt=text), 
        input = txt, 
        output = "Sentence", 
        token = "regex",
        pattern = "(?<!\\b\\p{L}r)\\.|\\n\\n", to_lower=F)

      result <- ""
      payload_txt <- ""

      for (i in 1:nrow(df)) {
        if (is.null(df$Sentence[i])) next 
        if (is.na(df$Sentence[i])) next 
        if (nchar(trimws(df$Sentence[i])) == 0) next

        if (payload_txt == "") {
          payload_txt <- trimws(df$Sentence[i])
        } else {
          if (nchar(payload_txt) + nchar(df$Sentence[i]) < 3000 && i < nrow(df)) {
            payload_txt <- paste(payload_txt, trimws(df$Sentence[i]), sep = ".  ") 
          } else {
            payload_txt <- paste(payload_txt, ".", sep='')
            payload <- paste("{\"q\":\"", payload_txt,"\",\"source\": \"",source_lang,"\",\"target\": \"",target_lang,"\"}", sep='')
            encode <- "json"

            #clessnverse::logit(scriptname, paste("translating language - pass", i), logger)

            response <- httr::VERB(
              "POST", 
              url, 
              body = payload,
              httr::add_headers('X-RapidAPI-Key' = key, 
              'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'), 
              httr::content_type("application/json"), 
              encode = encode)
        
            #clessnverse::logit(scriptname, paste("translating language done - pass", i), logger)

            r <- jsonlite::fromJSON(httr::content(response, "text"))

            result <- paste(result,r$data$translations$translatedText, sep=" ")

            payload_txt <- ""
          }
        }
      }
    } else {
      # less than 5000 characters
      payload <- paste("{\"q\":\"", text,"\",\"source\": \"",source_lang,"\",\"target\": \"",target_lang,"\"}", sep='')
      encode <- "json"

      #clessnverse::logit(scriptname, "translating language", logger)

      response <- httr::VERB(
        "POST", 
        url, 
        body = payload,
        httr::add_headers('X-RapidAPI-Key' = key, 
        'X-RapidAPI-Host' = 'deep-translate1.p.rapidapi.com'), 
        httr::content_type("application/json"), 
        encode = encode)

      #clessnverse::logit(scriptname, "translating language done", logger)
    
      r <- jsonlite::fromJSON(httr::content(response, "text"))

      result <- r$data$translations$translatedText
    } #if (nchar(text) > 5000)

    print(trimws(result))
    
