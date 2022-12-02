# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))


filter <- clessnhub::create_filter(metadata=list(twitterHandle = "RobinHood1776"), data=NULL)

df_tweets <- clessnhub::get_items(table = 'tweets', filter = filter, download_data = TRUE)
