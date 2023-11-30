# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


df <- clessnverse::get_warehouse_table(
  table_name = 'political_parties_press_releases',
  data_filter = list(
    data__province_or_state="QC"
  ),
  credentials = credentials,
  nbrows = 0
)

names(df)
table(df$political_party)
table(df$date)
