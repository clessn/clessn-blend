# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__.schema="test",
    data__.lake_item_format="xml",
    data__event_date__gte="2019-07-01", 
    data__event_date__lte="2019-07-31"
  ),
  credentials = credentials,
  nbrows = 0
)

table(df$speaker_full_name)
table(df$speaker_polgroup, useNA = 'ifany')
which(grepl("Modifier", df$speaker_full_name))
df$hub.key[which(grepl("Modifier", df$speaker_full_name))]

Clean <- df %>%
  select(speaker_full_name, speaker_type) %>%
  filter(is.na(speaker_type)) %>%
  distinct(speaker_full_name)

# Uncomment below to PURGE the hub from the collected records
# This will actually PERMANENTLY DELETE the collected records
#for (i in df$hub.id) {
#   cat("processing #", count, "key", i)
#   hublot::remove_table_item('clhub_tables_warehouse_agoraplus_european_parliament', i, credentials)
#   count <- count + 1
#}
