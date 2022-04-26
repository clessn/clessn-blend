clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

library(dplyr)

#df <- clessnhub::get_items('agoraplus_interventions', download_data = T)

filter <- clessnhub::create_filter(data=list("eventID"="920220404EN"))
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T)
filter <- clessnhub::create_filter(data=list("eventID"="920220405EN"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))
filter <- clessnhub::create_filter(data=list("eventID"="920220406EN"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))


filter <- clessnhub::create_filter(data=list("eventID"="441057HAN057"))
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T)
filter <- clessnhub::create_filter(data=list("eventID"="441056HAN056"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))
filter <- clessnhub::create_filter(data=list("eventID"="441055HAN055"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))
filter <- clessnhub::create_filter(data=list("eventID"="441055HAN054"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))
filter <- clessnhub::create_filter(data=list("eventID"="441055HAN053"))
df <- df %>% bind_rows(clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T))


filter <- clessnhub::create_filter(metadata=list("location"="EU"), data=list(eventDate__gte="2022-04-01"))
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T)


filter <- clessnhub::create_filter(metadata=list("institution"="House of Commons of Canada"), data=list(eventDate__gte="2022-04-01"))
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = T)
