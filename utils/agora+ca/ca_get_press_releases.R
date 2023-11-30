clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


my_filter <- clessnhub::create_filter(
  type="parliament_debate", 
  schema="v2", 
  metadata=list(
    institution="House of Commons of Canada"), 
    #format="xml"),
  data=list(
    #eventID="432123HAN123"
    #eventDate__gte="2023-01-01"
    eventDate__lte="2007-01-29"
  )
)

my_filter <- clessnhub::create_filter()

df <- clessnhub::get_items(
  table = 'agoraplus_press_releases',
  filter = my_filter,
  download_data = TRUE,
  max_pages = -1
) 

table(df$type)