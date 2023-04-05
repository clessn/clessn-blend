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
    data__event_date__lte="2019-12-31"
  ),
  credentials = credentials,
  nbrows = 0
)

table(df$speaker_full_name, useNA = 'ifany')
table(df$speaker_polgroup, useNA = 'ifany')
table(df$speaker_party, useNA = 'ifany')
table(df$president_name, useNA = 'ifany')
table(df$speaker_type[is.na(df$speaker_polgroup)], useNA = 'ifany')
length(df$intervention_id[is.na(df$speaker_polgroup)])
table(df$speaker_full_name[is.na(df$speaker_polgroup)], useNA = 'ifany')
df$intervention_header[is.na(df$speaker_polgroup)]
df$intervention_id[is.na(df$speaker_polgroup)]

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
