# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

scriptname <- "eu_get_new_debates"
logger <- clessnverse::log_init(scriptname, "console", "./logs")


###############################################################################
# fix NA in party, polgroup and country
df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__.schema = "202303",
    data__speaker_polgroup=NA, 
    data__speaker_party=NA,
    data__speaker_country=NA
  ),
  credentials = credentials,
  nbrows = 0
)


for (i in 1:nrow(df)) {
  item <- df[i,]

  cat("processing #", i, "key", item$hub.key, "\n")

  item$intervention_header_en

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


###############################################################################
# fix speaker_fullname == L’étoile, Modifier
# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__speaker_full_name="L’étoile, Modifier"
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

# for (i in 1:nrow(df)) {
#   item <- df[i,]

#   cat("processing #", i, "key", item$hub.key, "\n")

#   item$speaker_full_name<- "Estrela, Edite"

#   hublot::update_table_item(
#     table_name = "clhub_tables_mart_agoraplus_european_parliament",
#     id = item$hub.id,
#     body = list(
#       key = item$hub.key, 
#       timestamp = as.character(Sys.time()), 
#       data = jsonlite::toJSON(
#         as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
#         auto_unbox = T
#       )
#     ),
#     credentials = credentials
#   )
# }


###############################################################################
# fix speaker_fullname == Herczog, Modifier
df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__.schema="202303",
    data__speaker_full_name="Herczog, Modifier"
  ),
  credentials = credentials,
  nbrows = 0
)

for (i in 1:nrow(df)) {
  item <- df[i,]

  cat("processing #", i, "key", item$hub.key, "\n")

  item$speaker_full_name<- item$president_name

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



###############################################################################
# fix President | Chairman in speaker_full_name
# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__speaker_full_name__in=c("President","Chairman","The President, Ms. President")
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

# for (i in 1:nrow(df)) {
#   item <- df[i,]

#   cat("processing #", i, "key", item$hub.key, "\n")

#   item$speaker_full_name<- item$president_name

#   hublot::update_table_item(
#     table_name = "clhub_tables_mart_agoraplus_european_parliament",
#     id = item$hub.id,
#     body = list(
#       key = item$hub.key, 
#       timestamp = as.character(Sys.time()), 
#       data = jsonlite::toJSON(
#         as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
#         auto_unbox = T
#       )
#     ),
#     credentials = credentials
#   )
# }


###############################################################################
# fix polgroup
# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__speaker_polgroup__in=c("Group Of The Alliance Of Liberals And Democrats for Europe","Group Of The Alliance Of Liberals And Democrats For Europe")
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

# for (i in 1:nrow(df)) {
#   item <- df[i,]

#   cat("processing #", i, "key", item$hub.key, "\n")

#   item$speaker_polgroup <- "Renew Europe Group"

#   hublot::update_table_item(
#     table_name = "clhub_tables_mart_agoraplus_european_parliament",
#     id = item$hub.id,
#     body = list(
#       key = item$hub.key, 
#       timestamp = as.character(Sys.time()), 
#       data = jsonlite::toJSON(
#         as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
#         auto_unbox = T
#       )
#     ),
#     credentials = credentials
#   )
# }


###############################################################################
# fix polgroup
# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__speaker_polgroup="Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

# for (i in 1:nrow(df)) {
#   item <- df[i,]

#   cat("processing #", i, "key", item$hub.key, "\n")

#   item$speaker_polgroup <-  "Progressive Alliance Of Socialists And Democrats (S&D)"

#   hublot::update_table_item(
#     table_name = "clhub_tables_mart_agoraplus_european_parliament",
#     id = item$hub.id,
#     body = list(
#       key = item$hub.key, 
#       timestamp = as.character(Sys.time()), 
#       data = jsonlite::toJSON(
#         as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
#         auto_unbox = T
#       )
#     ),
#     credentials = credentials
#   )
# }


###############################################################################
# fix polgroup
df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__.schema="202303",
    data__speaker_polgroup="Confederal Group Of The European United Left - Nordic Green Left"
  ),
  credentials = credentials,
  nbrows = 0
)

for (i in 1:nrow(df)) {
  item <- df[i,]

  cat("processing #", i, "key", item$hub.key, "\n")

  item$speaker_polgroup <-  "The Left Group In The European Parliament - GUE/NGL"

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


###############################################################################
# fix polgroup
# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__speaker_polgroup="Europe Of Nations And Freedom Group"
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

# for (i in 1:nrow(df)) {
#   item <- df[i,]

#   cat("processing #", i, "key", item$hub.key, "\n")

#   item$speaker_polgroup <-  "Europe Of Freedom And Direct Democracy Group"

#   hublot::update_table_item(
#     table_name = "clhub_tables_mart_agoraplus_european_parliament",
#     id = item$hub.id,
#     body = list(
#       key = item$hub.key, 
#       timestamp = as.character(Sys.time()), 
#       data = jsonlite::toJSON(
#         as.list(item[1,c(which(!grepl("hub.",names(item))))]), 
#         auto_unbox = T
#       )
#     ),
#     credentials = credentials
#   )
# }

