clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
metadata_filter <- list(location="EU")
filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=metadata_filter)  
df <- clessnhub::get_items('agoraplus_interventions', filter=filter, download_data = TRUE)

for (i in 1:nrow(df)) {
  clessnhub::delete_item('agoraplus_interventions', df$key[i])
}
