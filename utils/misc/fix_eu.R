
# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))



df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__president_name="Die In – Als Erster Punkt Der Tagesordnung Folgt Die Aussprache Uber Den Bericht Uber Den Vorschlag Fur Eine Verordnung Des Europaischen Parlaments Und Des Rates Zur Anderung Der Verordnung (Eg) Nr 1059/2003 In Bezug Auf Die Territorialen Typologien (Tercet) Von Iskra Mihaylova Im Namen Des Ausschusses Fur Regionale Entwicklung (Com(2016)0788 – C8-0516/2016 – 2016/0393(Cod)) (A8-0231/2017)"
  ),
  credentials = credentials,
  nbrows = 0
)

for (i in 1:nrow(df)) {
  item <- df[i,]

  item$president_name <-  "Evelyne Gebhardt"

  hublot::update_table_item(
    table_name = "clhub_tables_mart_agoraplus_european_parliament",
    id = item$hub.id,
    body = list(
      key = item$hub.key, 
      timestamp = as.character(Sys.time()), 
      data = jsonlite::toJSON(
        as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
        auto_unbox = T
      )
    ),
    credentials = credentials
  )
}
