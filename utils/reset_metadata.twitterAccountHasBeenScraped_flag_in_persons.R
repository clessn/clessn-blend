# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))


filter <- clessnhub::create_filter(type="journalist", schema="v2", metadata=NULL, data=NULL)

df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)


for (i in 1:nrow(df_persons)) {

    item <- clessnhub::get_item('persons', df_persons$key[i])

    data <- item$data
    metadata <- item$metadata

    metadata$twitterAccountHasBeenScraped <- 0

    clessnhub::edit_item('persons', item$key, item$type, item$schema, metadata, data)
}
