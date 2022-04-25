clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

library(dplyr)

#df <- clessnhub::get_items('agoraplus_interventions', download_data = T)

filter <- clessnhub::create_filter(data=list("eventID"="920220404EN"))
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T)
filter <- clessnhub::create_filter(data=list("eventID"="920220405EN"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))
filter <- clessnhub::create_filter(data=list("eventID"="920220406EN"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))



for (i in 1:nrow(df)) {
    clessnhub::delete_item(table='agoraplus_interventions', key = df$key[i])
}

