# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


df <- clessnverse::get_warehouse_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__.schema = "202303",
    #data__event_date__gte="2014-01-01", 
    #data__event_date__lte="2019-06-30"
    data__president_name__isnull=TRUE 
  ),
  credentials = credentials,
  nbrows = 0
)
table(df$president_name)
length(table(df$event_date))
# Uncomment below to purge the hub from the collected records
#for (i in df$hub.id) {
#  print(i)
#  hublot::remove_table_item('clhub_tables_warehouse_agoraplus_european_parliament', i, credentials)
#}
