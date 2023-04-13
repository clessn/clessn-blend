library(dplyr)

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

filter <- clessnhub::create_filter(
   metadata=NULL, 
   data=list(
      twitterHandle__gte = ""
   )
)

df_persons <- clessnhub::get_items(
   table = 'persons', 
   filter = filter, 
   download_data = TRUE, 
   max_pages = -1
)
