# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

df <- clessnverse::get_warehouse_table(
  table_name = 'people',
  data_filter = list(data__institution = "European Parliament"), #list(key__contains = "820161201EN"),
  credentials = credentials,
  nbrows = 0
)

table(df$pol_group)
  

# Uncomment below to purge the hub from the collected records
# for (i in df$hub.id) {
#   print(i)
#   hublot::remove_table_item('clhub_tables_warehouse_agoraplus_european_parliament', i, credentials)
# }
