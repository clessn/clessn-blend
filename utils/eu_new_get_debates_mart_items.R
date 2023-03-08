# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

scriptname <- "eu_get_new_debates"
logger <- clessnverse::log_init(scriptname, "console", "./logs")

df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__event_date = "2014-09-18"
  ), 
  credentials = credentials,
  nbrows = 0
)

# Uncomment below to purge the hub from the collected records
# for (i in df$hub.id) {
#   print(i)
#   hublot::remove_table_item('clhub_tables_warehouse_agoraplus_european_parliament', i, credentials)
# }
