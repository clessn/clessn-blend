# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


fix_list <- list(

  list(
    filter = list(
      data__.schema = "202303",
      data__speaker_polgroup=NA, 
      data__speaker_party=NA,
      data__speaker_country=NA
    ),
    fix = list(
      blah = "toto"
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_full_name="L’étoile, Modifier"
    ),
    fix = list(
      fix = 'item$speaker_full_name<- "Estrela, Edite"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_full_name="Herczog, Modifier"
    ),
    fix = list(
      blah = "toto"
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_full_name__in=c("President","Chairman","The President, Ms. President")
    ),
    fix = list(
      fix = 'item$speaker_full_name <- df_people$full_name[grepl(item$president_name, paste(df_people$full_name, df_people$other_names, sep = " | "))]'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup__in=c("Group Of The Alliance Of Liberals And Democrats for Europe","Group Of The Alliance Of Liberals And Democrats For Europe")
    ),
    fix = list(
      fix = 'item$speaker_polgroup <- "Renew Europe Group"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Progressive Alliance Of Socialists And Democrats (S&D)"
    ),
    fix = list(
      fix = 'item$speaker_polgroup <- "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Confederal Group Of The European United Left - Nordic Green Left"
    ),
    fix = list(
      blah = "toto"
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Europe Of Nations And Freedom Group"
    ),
    fix = list(
      blah = "toto"
    )
  )
)

df_people <- clessnverse::get_warehouse_table(
  table_name = 'people', 
  data_filter=list(
    data__union = "EU",
    data__institution = "European Parliament"
  ),
  credentials = credentials,
  nbrows = 0
)

for (i in 1:length(fix_list)) {
  df <- clessnverse::get_mart_table(
    table_name = 'agoraplus_european_parliament',
    data_filter = fix_list[[i]]$filter,
    credentials = credentials,
    nbrows = 0
  )

  for (j in 1:nrow(df)) {
    item <- df[j,]

    cat("processing #", j, "key", item$hub.key, "\n")

    eval(parse(text = fix_list[[i]]$fix$fix))

    success <- FALSE
    attempt <- 1

    while (!success && attempt < 20) {
      tryCatch(
        {
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
          success <- TRUE
        },
        error = function(e) {
          print(e)
          Sys.sleep(30)
        },
        finally={
          attempt <- attempt + 1
        }
      )
    }
  }
}




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
# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__speaker_polgroup="Confederal Group Of The European United Left - Nordic Green Left"
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

# for (i in 1:nrow(df)) {
#   item <- df[i,]

#   cat("processing #", i, "key", item$hub.key, "\n")

#   item$speaker_polgroup <-  "The Left Group In The European Parliament - GUE/NGL"

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
    data__speaker_polgroup="Europe Of Nations And Freedom Group"
  ),
  credentials = credentials,
  nbrows = 0
)

for (i in 1:nrow(df)) {
  item <- df[i,]

  cat("processing #", i, "key", item$hub.key, "\n")

  item$speaker_polgroup <-  "Europe Of Freedom And Direct Democracy Group"

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


#Confederal Group Of The European United Left - Nordic Green Left est à merger avec The Left Group In The European Parliament - GUE/NGL


#Europe Of Nations And Freedom Group à merger avec Europe Of Freedom And Direct Democracy



#Europe Of Freedom And Direct Democracy Group pour Europe Of Freedom And Direct Democracy


#Progressive Alliance Of Socialists And Democrats (S&D) pour Group of the Progressive Alliance of Socialists and Democrats in the European Parliament


#Renew Europe Group pour Group Renew Europe





df <- clessnverse::get_warehouse_table(
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
    table_name = "clhub_tables_warehouse_agoraplus_european_parliament",
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
