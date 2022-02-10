clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(location="EU")


start_date <- "2014-01-01"
start_date <- as.Date(start_date)

end_date <- "2019-12-31"
end_date <- as.Date(end_date)

nb_days <- as.integer(difftime(end_date, start_date, units = 'days'))

little_df <- data.frame()
big_df <- data.frame()

for (d in 0:nb_days) {
  day <- start_date + d
  
  metadata_filter <- list(location="EU")
  data_filter <- list(eventDate=as.character(day))
  
  filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=metadata_filter, data=data_filter)  
  little_df <- clessnhub::get_items('agoraplus_interventions', filter=filter, download_data = TRUE)
  
  if (is.null(little_df)) next
  
  if (nrow(big_df) > 0) {  
    big_df <- big_df %>% rbind(little_df)
  } else {
    big_df <- little_df 
  }
  
}