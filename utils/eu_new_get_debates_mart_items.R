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
    data__event_date__gte="2014-01-01", 
    data__event_date__lte="2014-12-31"
  ),
  credentials = credentials,
  nbrows = 0
)

table(df$event_date)


table(df$event_date)


# Uncomment below to purge the hub from the collected records
#for (i in df$hub.id) {
#   cat("processing #", count, "key", i)
#   hublot::remove_table_item('clhub_tables_warehouse_agoraplus_european_parliament', i, credentials)
#   count <- count + 1
#}
