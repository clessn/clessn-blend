clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

my_filter <- clessnhub::create_filter()

df <- clessnhub::get_items(
  table = 'agoraplus_press_releases',
  filter = my_filter,
  download_data = TRUE,
  max_pages = -1
) 

table(df$type)