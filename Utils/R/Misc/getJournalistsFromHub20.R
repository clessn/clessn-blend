clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

my_filter  <- clessnhub::create_filter(type = "journalist")

df <- clessnhub::get_items("persons", filter = my_filter)

