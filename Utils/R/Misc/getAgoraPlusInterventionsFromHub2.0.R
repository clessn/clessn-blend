clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

library(dplyr)

df <- clessnhub::get_items('agoraplus_interventions', download_data = T)

filter <- clessnhub::create_filter(metadata = list("location"="CA-QC"))
dataset <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T)

