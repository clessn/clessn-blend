# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


df <- clessnverse::get_warehouse_table(
  table_name = 'factiva_press_articles',
  data_filter = list(
  ),
  credentials = credentials,
  nbrows = 0
)
