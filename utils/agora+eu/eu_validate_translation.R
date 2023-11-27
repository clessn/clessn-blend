library(magrittr)
library(dplyr)

# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

table_name <- "agoraplus_european_parliament"


df <- clessnverse::get_mart_table(
  table_name = table_name,
  data_filter = list(
     data__.schema="202303"
  #   data__event_date__gte="2014-01-01", 
  #   data__event_date__lte="2023-03-31"
  ),
  credentials = credentials,
  nbrows = 0
)

ratio <- nchar(df$intervention_text)/nchar(df$intervention_text_en)
summary(ratio)

list = which(ratio>=0.5 & ratio<0.6)
list 

checkdf <- data.frame()
for (i in list) {
  checkdf <- checkdf %>% rbind(df[i,])
}
View(data.frame(checkdf$intervention_text_en, checkdf$intervention_text))

i = list[1]
df$intervention_text[i]
df$intervention_text_en[i]
df$intervention_text_en[i] <- "According to the Audit Report for the financial year 2014, there were no irregularities found in the financial documentation. Based on the Audit Report, I hereby confirm that the Director of the European Institute of Innovation and Technology has properly executed the Institute's financial reports for the year 2014."

for (i in list) {
  lang <- clessnverse::detect_language("fastText", df$intervention_text[i])
  df$intervention_text_en[i] <- clessnverse::translate_text(df$intervention_text[i], engine = "azure"
    , source_lang = lang, target_lang = "en", translate = TRUE)
}


for (i in list) {
    success <- FALSE
    attempt <- 1

    item <- df[i,]

    while (!success && attempt < 20) {
      tryCatch(
        {
          hublot::update_table_item(
            table_name = paste("clhub_tables_mart_", table_name, sep=""),
            id = item$hub.id,
            body = list(
              key = item$hub.key, 
              timestamp = as.character(Sys.time()), 
              data = jsonlite::toJSON(
                as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
                auto_unbox = T
              )
            ),
            credentials = credentials
          )
          success <- TRUE
        },
        error = function(e) {
          print(e)
          Sys.sleep(30)
        },
        finally={
          attempt <- attempt + 1
        }
      )
    }
}


