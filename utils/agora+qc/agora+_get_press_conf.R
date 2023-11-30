# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


df <- clessnverse::get_warehouse_table(
  table_name = 'agoraplus_qc_press_conferences',
  data_filter = list(
    data__event_id = "ytzbwmyt4c4ck"
  ),
  credentials = credentials,
  nbrows = 0
)

names(df)
table(df$political_party)
table(df$date)
