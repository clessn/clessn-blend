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
    data__.schema="202303",
    data__event_date__gte="2014-01-01", 
    data__event_date__lte="2014-12-31"
  ),
  credentials = credentials,
  nbrows = 0
)

table(df$speaker_full_name)
table(df$speaker_polgroup)
which(grepl("Modifier", df$speaker_full_name))
df$hub.key[which(grepl("Modifier", df$speaker_full_name))]

# Uncomment below to PURGE the hub from the collected records
# This will actually PERMANENTLY DELETE the collected records
#for (i in df$hub.id) {
#   cat("processing #", count, "key", i)
#   hublot::remove_table_item('clhub_tables_warehouse_agoraplus_european_parliament', i, credentials)
#   count <- count + 1
#}
