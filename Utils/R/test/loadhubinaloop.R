clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

library(dplyr)

start_date <- format(Sys.time(), "%Y-%m-%d")
start_date <- as.Date(start_date)

little_df <- data.frame()
big_df <- data.frame()

for (d in 0:5) {
  date_point <- start_date - 365 + d 
  
  filter <- clessnhub::create_filter(data = list(eventDate = as.character(date_point)))
  
  little_df <- clessnhub::get_items('agoraplus_interventions', filter = filter, download_data = T) 
  
  if (is.null(little_df)) next
  
  if (nrow(big_df) > 0) {  
    big_df <- big_df %>% rbind(little_df %>%
      select(data.eventID, data.interventionSeqNum, data.speakerIsMinister, data.eventDate, data.speakerType, 
             data.speakerParty, data.speakerDistrict, data.interventionLang, data.interventionText, 
             data.speakerFirstName, data.speakerLastName, data.speakerFullName, key) %>% 
      mutate(slug = little_df$key))
  } else {
    big_df <- little_df %>%
      select(data.eventID, data.interventionSeqNum, data.speakerIsMinister, data.eventDate, data.speakerType, 
             data.speakerParty, data.speakerDistrict, data.interventionLang, data.interventionText, 
             data.speakerFirstName, data.speakerLastName, data.speakerFullName, key) %>% 
      mutate(slug = little_df$key)  
  }
}
