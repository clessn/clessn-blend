# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


df <- clessnverse::get_warehouse_table(
  table_name = 'people',
  data_filter = list(
    data__institution="European Parliament"
  ),
  credentials = credentials,
  nbrows = 0
)

table(df$pol_group)

# df$pol_group[which(df$pol_group == "Renew Europe Group")] <- "Group Renew Europe"

# df$pol_group[which(df$pol_group == "Confederal Group Of The European United Left - Nordic Green Left")] <- "The Left Group In The European Parliament - GUE/NGL"

# df$pol_group[which(df$pol_group == "Europe Of Nations And Freedom Group")] <- "Europe Of Freedom And Direct Democracy"

# df$pol_group[which(df$pol_group == "Europe Of Freedom And Direct Democracy Group")] <- "Europe Of Freedom And Direct Democracy"

# df$pol_group[which(df$pol_group == "Progressive Alliance Of Socialists And Democrats (S&D)")] <- "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament"
# df$pol_group[which(df$pol_group == " Progressive Alliance Of Socialists And Democrats (S&D)")] <- "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament"
df$pol_group[which(df$pol_group == "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament")] <- "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"

# df$pol_group[which(df$pol_group == "Group of the European People's Party (Christian Democrats)")] <- "Group Of The European People's Party (Christian Democrats)"

# df$pol_group[which(df$pol_group == "Europe Of Freedom And Democracy Group")] <- "Europe Of Freedom And Direct Democracy"
# df$pol_group[which(df$pol_group == "Europe Of Freedom And Direct Democracy Group ")] <- "Europe Of Freedom And Direct Democracy"

df_to_process <- df[which(df$pol_group == "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"),]


for (i in 1:nrow(df_to_process)) {
  item <- df_to_process[i,]

  cat("processing #", i, "key", item$hub.key, "\n")

  hublot::update_table_item(
    table_name = "clhub_tables_warehouse_people",
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
