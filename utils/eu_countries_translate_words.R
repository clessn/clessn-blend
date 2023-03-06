credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))

scriptname <- "translate_eu_countries_words"

logger <- clessnverse::log_init(scriptname, "console")

df_countries <- clessnverse::get_warehouse_table(
  table_name = 'countries',
  data_filter = list(),
  credentials = credentials,
  nbrows = 0
)


word <- "presidency"
translated <- c()

for (i in df_countries$two_letter_locales) {
  for (j in strsplit(i,",")[[1]]) {
    tryCatch(
      {
        translation <- clessnverse::translate_text(
          text = word,
          engine = "deeptranslate",
          source_lang = "en", 
          target_lang = substr(j,1,2),
          translate = TRUE
        )
        clessnverse::logit(scriptname, paste("transated", word, "into", translation), logger)
        translated <- c(translated,translation)
      },
      error = function(e) {
        clessnverse::logit(scriptname, paste("unsupported language", substr(j,1,2)), logger)
      },
      finally = {        
      }
    )
  }
}
