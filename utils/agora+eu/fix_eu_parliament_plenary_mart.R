# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


table_name <- "clhub_tables_mart_agoraplus_european_parliament"


df_people <- clessnverse::get_warehouse_table(
  table_name = 'people',
  data_filter = list(data__institution = "European Parliament"),
  credentials = credentials,
  nbrows = 0
)

df_countries <- clessnverse::get_warehouse_table(
  table_name = 'countries',
  data_filter = list(),
  credentials = credentials,
  nbrows = 0
)

df <- clessnverse::get_mart_table(
  table_name = 'agoraplus_european_parliament',
  data_filter = list(
    data__.schema="202303",
    data__event_date__gte="2014-01-01", 
    data__event_date__lte="2019-06-30"
  ),
  credentials = credentials,
  nbrows = 0
)

# df <- clessnverse::get_mart_table(
#   table_name = 'agoraplus_european_parliament',
#   data_filter = list(
#     data__.schema="202303",
#     data__event_date__gte="2014-01-01", 
#     data__event_date__lte="2019-06-30",
#     data__speaker_full_name = "Mélenchon, Jean-Luc"
#   ),
#   credentials = credentials,
#   nbrows = 0
# )

#####
# Fix polgroups

fix_list <- list(

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup=" Progressive Alliance Of Socialists And Democrats (S&D)"
    ),
    fix = list(
      fix = 'item$speaker_polgroup<- "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup__in=c("Europe Of Freedom And Direct Democracy Group ", "Europe Of Freedom And Direct Democracy")
    ),
    fix = list(
      fix = 'item$speaker_polgroup<- "Europe Of Freedom And Direct Democracy Group"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Group of the European People\'s Party (Christian Democrats)"
    ),
    fix = list(
      fix = 'item$speaker_polgroup<- "Group Of The European People\'s Party (Christian Democrats)"'
    )
  ),

   list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Renew Europe Group"
    ),
    fix = list(
      fix = 'item$speaker_polgroup<- "Group Renew Europe"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Confederal Group Of The European United Left - Nordic Green Left"
    ),
    fix = list(
      fix = 'item$speaker_polgroup <- "The Left Group In The European Parliament - GUE/NGL"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Europe Of Nations And Freedom Group"
    ),
    fix = list(
      fix = 'item$speaker_polgroup <- "Identity And Democracy Group"'
    )
  ),

  list(
    filter = list(
      data__.schema="202303",
      data__speaker_polgroup="Group of the Progressive Alliance of Socialists and Democrats in the European Parliament"
    ),
    fix = list(
      fix = 'item$speaker_polgroup <- "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"'
    )
  ),

  
)


for (i in 1:length(fix_list)) {

  supported_table <- FALSE

  if (grepl("mart", table_name)) {
    df <- clessnverse::get_mart_table(
      table_name = 'agoraplus_european_parliament',
      data_filter = fix_list[[i]]$filter,
      credentials = credentials,
      nbrows = 0
    )
    supported_table <- TRUE
  }

  if (grepl("warehouse", table_name)) {
    df <- clessnverse::get_warehouse_table(
      table_name = 'agoraplus_european_parliament',
      data_filter = fix_list[[i]]$filter,
      credentials = credentials,
      nbrows = 0
    )
    supported_table <- TRUE
  }

  if (!supported_table) stop(paste("unsupported table", table_name))

  for (j in 1:nrow(df)) {
    item <- df[j,]

    cat("processing #", j, "/", nrow(df), "key", item$hub.key, "\n")

    eval(parse(text = fix_list[[i]]$fix$fix))

    success <- FALSE
    attempt <- 1

    while (!success && attempt < 20) {
      tryCatch(
        {
          hublot::update_table_item(
            table_name = table_name,
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



#####
# Fix speaker_full_name


#######
# Fix1
#df_to_fix <- df[which(nchar(df$speaker_full_name) > 30),]

# Fix2
#df_to_fix <- df[which(grepl("in writing", df$speaker_full_name)),]

# Fix3
df_to_fix <- df[which(grepl("chairman", df$speaker_full_name)),]
#df_to_fix <- df[which(grepl("The president", df$speaker_full_name)),]
#df_to_fix$president_name[df_to_fix$president_name == "Rainer Wieland Catch-The-Eye-Verfahren"] <- "Rainer Wieland"
#df_to_fix$president_name <- trimws(df_to_fix$president_name)
#df_to_fix$president_name[grepl("Als Nachster Punkt Folgt Die Abstimmungsstunde", df_to_fix$president_name)] <- "Martin Schulz"
#df_to_fix$president_name[df_to_fix$president_name == "Προεδρια Αννυ Ποδηματα"] <- "Anni Podimata"
#df_to_fix$president_name[df_to_fix$president_name == "Προεδρια Αννυ Ποδηματα"] <- "Anni Podimata"
#df_to_fix <- df[df$speaker_full_name == "Προεδρια Αννυ Ποδηματα",] 


# Fix4
#df_to_fix <- df[which(grepl("Προεδρια Δημητριοσ Παπαδημουλη", df$speaker_full_name)),]
#df_to_fix$speaker_full_name <- "Dimitrios Papadimoulis"

# Fix5
#df_to_fix <- df[which(df$speaker_full_name == "Written by Marju Lauristin (S&D)."),]

# Fix6
#df_to_fix <- df[which(df$speaker_full_name == "Véronique Mathieu Houillon (PPE), blue card response"),]

# Fix7
#df_to_fix <- df[which(df$speaker_full_name == "Schulz, Martin"),]

# Fix8
#df_to_fix <- df[which(grepl("Tibor Navracsics, Member of the Commission", df$speaker_full_name)),]

# Fix8
#df_to_fix <- df[which(grepl("^.*(\\(.*\\))$", df$speaker_full_name)),]

for (i in 1:nrow(df_to_fix)) {
  df_speaker <- data.frame()
  
  item <- df_to_fix[i,]

  if (is.na(item$speaker_full_name)) next


  full_name <- NA
  speaker_type <- NA

  cat("processing #", i, "/", nrow(df_to_fix), "key", item$hub.key, "\n")

  change <- FALSE

  if (grepl("in writing", tolower(item$speaker_full_name))) {
    cat("found in writing ")
    full_name <- gsub("in writing", "", tolower(item$speaker_full_name))
    full_name <- gsub(",", "", full_name)
    full_name <- gsub("\\(.*\\)", "", full_name)
    full_name <- trimws(full_name)
    full_name <- stringr::str_to_title(full_name)

    df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]

    start <- stringr::str_locate(tolower(item$speaker_full_name), "in writing")[1]
    item$speaker_type <- paste("MEP,", stringr::str_sub(item$speaker_full_name, start, nchar(item$speaker_full_name)))
    change <- TRUE
  } else {
    if (grepl("on behalf", tolower(item$speaker_full_name)) && !grepl("tibor navracsics", tolower(item$speaker_full_name))) {
      cat("found on behalf ")
      start <- stringr::str_locate(tolower(item$speaker_full_name), "on behalf")[1]
      full_name <- stringr::str_sub(item$speaker_full_name, 1, start-2)
      full_name <- gsub(",", "", full_name)
      full_name <- gsub("\\(.*\\)", "", full_name)
      full_name <- trimws(full_name)
      full_name <- stringr::str_to_title(full_name)

      df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]

      start <- stringr::str_locate(tolower(item$speaker_full_name), "on behalf")[1]
      item$speaker_type <- paste("MEP,", stringr::str_sub(item$speaker_full_name, start, nchar(item$speaker_full_name)))
      change <- TRUE
    } else {
      if (grepl("member of", tolower(item$speaker_full_name))) {
        cat("found member of ")
        start <- stringr::str_locate(tolower(item$speaker_full_name), "member of")[1]
        full_name <- stringr::str_sub(item$speaker_full_name, 1, start-2)
        full_name <- gsub(",", "", full_name)
        full_name <- gsub("\\(.*\\)", "", full_name)
        full_name <- trimws(full_name)
        full_name <- stringr::str_to_title(full_name)

        df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]

        start <- stringr::str_locate(tolower(item$speaker_full_name), "member of")[1]
        item$speaker_type <- paste(stringr::str_sub(item$speaker_full_name, start, nchar(item$speaker_full_name)))
        change <- TRUE
      } else {
        if (grepl("the president", tolower(item$speaker_full_name))) {
          cat("found The president ")
          full_name <- item$president_name

          df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]

          item$speaker_type <- "President"
          change <- TRUE
        } else {
          if (grepl("dimitrios papadimoulis", tolower(item$speaker_full_name))) {
            cat("found Dimitrios Papadimoulis ")
            full_name <- "Dimitrios Papadimoulis"

            if (item$speaker_type == "Chairman" || item$speaker_type == "President") item$president_name <- full_name

            df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
            change <- TRUE
          } else {
            if (item$speaker_full_name == "Written by Marju Lauristin (S&D).") {
              cat("found Written by Marju Lauristin (S&D). ")
              full_name <- "Marju Lauristin"

              df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
              change <- TRUE
            } else {
              if (item$speaker_full_name == "Véronique Mathieu Houillon (PPE), blue card response") {
                cat("found Véronique Mathieu Houillon (PPE), blue card response ")
                full_name <- "Véronique Mathieu Houillon"

                df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
                change <- TRUE
              } else {
                if (item$speaker_full_name == "Schulz, Martin") {
                  cat("found Schulz, Martin ")
                  full_name <- "Schulz, Martin"

                  df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
                  change <- TRUE
                } else {
                  if (grepl("tibor navracsics member of the commission", tolower(item$speaker_full_name)) || grepl("tibor navracsics, member of the commission", tolower(item$speaker_full_name))) {
                    cat("found Tibor Navracsics, Member of the Commission ")
                    full_name <- "Tibor Navracsics"

                    df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))) ,]
                    change <- TRUE
                  } else {
                    if (grepl("^.*(\\(.*\\))$", item$speaker_full_name)) {
                      pattern <- stringr::str_match(item$speaker_full_name, "^.*(\\(.*\\))$")[2]

                      if (nchar(pattern) <= 6) {
                        pattern <- gsub("\\(", "\\\\(", pattern)
                        pattern <- gsub("\\)", "\\\\)", pattern)
                        full_name <- trimws(gsub(pattern, "", item$speaker_full_name))
                        df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))) ,]
                        change <- TRUE
                      } 
                    } else {
                      if (item$speaker_full_name == "Προεδρια Αννυ Ποδηματα") {
                          full_name <- "Anni Podimata" 
                          df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))) ,]
                          change <- TRUE
                        }
                      } 
                  }
                } 
              } 
            }
          } 
        }
      }
    }
  }

  if (nrow(df_speaker) > 0) {
    item$speaker_full_name <- df_speaker$full_name[1]
    if (is.na(item$speaker_polgroup)) item$speaker_polgroup <- df_speaker$pol_group[1]
    item$speaker_party <- df_speaker$party[1]
    item$speaker_gender <- df_speaker$gender[1]
    item$speaker_country <- df_countries$name[df_countries$short_name_3 == df_speaker$country[1]]
  } else {
    if (!is.na(full_name)) item$speaker_full_name <- full_name
  }



  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
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

#####
# Fix speaker_country
df_to_fix <- df[which(nchar(df$speaker_country)==3),]

for (i in 1:nrow(df_to_fix)) {

  item <- df_to_fix[i,]

  item$speaker_country <- df_countries$name[df_countries$short_name_3 == item$speaker_country]

  success <- FALSE
  attempt <- 1

  while (!success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
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


#####
# Fix speaker_gender
df_to_fix <- df[which(is.na(df$speaker_gender)),]

for (i in 1:nrow(df_to_fix)) {

  item <- df_to_fix[i,]

  df_gender <- gender::gender(strsplit(item$speaker_full_name, " ")[[1]][1])
  if (nrow(df_gender) > 0 ) {
    item$speaker_gender <- df_gender$gender
    change <- TRUE
  }

  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
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


#####
# Fix president_name 

#1
#president_patterns <- " Vicepresedinta|vicepresedinta|Catch-The-Eye Procedure|Ol |Pan |Presedintia |Przewodniczy |Przewodniczy |Catch-The-Eye-Verfahren| Wiceprzewodniczacy|Προεδρια "
#df_to_fix <- df[which(grepl(president_patterns, df$president_name)),]

#2
#df_to_fix <- df[which(grepl("Dezbaterea A Fost Inchisavotul Va Avea Loc Joi, 18 Mai", df$president_name)),]

#3
#df_to_fix <- df[which(grepl("Siamo Chiamati Ad Esprimerci Anche Sull’accordo", df$president_name)),]

#4
df_to_fix <- df[which(grepl("Als Nachster Punkt Folgt Die Abstimmungsstunde", df$president_name)),]


for (i in 1:nrow(df_to_fix)) {

  item <- df_to_fix[i,]

  cat("processing #", i, "/", nrow(df_to_fix), "key", item$hub.key, "\n")

  #1
  #full_name <- item$president_name
  #2 
  #full_name <- "IOAN MIRCEA PAŞCU"
  #3
  #fill_name <- "ANTONIO TAJANI"
  #4
  fill_name <- "MARTIN SCHULZ"

  full_name <- gsub(president_patterns, "", full_name)

  df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
  if (nrow(df_speaker) == 0) df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$full_name))),]
 
  if (nrow(df_speaker) > 0) {
    item$president_name <- paste(strsplit(df_speaker$full_name[1], ", ")[[1]][2], strsplit(df_speaker$full_name[1], ", ")[[1]][1])
    change <- TRUE
  } 

  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
    tryCatch(
      {

        df[df$hub.key == item$hub.key,] <- item   

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


#####
# Fix speaker_full_name and speaker_full_name_native when spraker is president

#1
# connect to hublot
speaker_type_patterns <- "^Chairman$|^Chairman \\:$|^Chairman\\:$|^President$|^vice-president$|^Speaker$|^Vice-Presiden$"
df_to_fix <- df[which(stringr::str_detect(df$speaker_full_name, speaker_type_patterns) & is.na(df$speaker_polgroup)),]
nrow(df_to_fix)
table(df_to_fix$speaker_type)
table(df_to_fix$speaker_full_name)

for (i in 1:nrow(df_to_fix)) {

  item <- df_to_fix[i,]

  cat("processing #", i, "/", nrow(df_to_fix), "key", item$hub.key, "\n")

  
  full_name <- item$president_name
  full_name <- gsub(president_patterns, "", full_name)

  df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
  if (nrow(df_speaker) == 0) df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$full_name))),]
 
  if (nrow(df_speaker) > 0) {
    item$president_name <- paste(strsplit(df_speaker$full_name[1], ", ")[[1]][2], strsplit(df_speaker$full_name[1], ", ")[[1]][1])
    change <- TRUE
  } 

  if (nrow(df_speaker) == 0) warning(paste("could not find", full_name))


  if (nrow(df_speaker) > 0) {
    item$speaker_full_name <- df_speaker$full_name[1]
    if (is.na(item$speaker_polgroup)) item$speaker_polgroup <- df_speaker$pol_group[1]
    item$speaker_party <- df_speaker$party[1]
    item$speaker_gender <- df_speaker$gender[1]
    item$speaker_country <- df_countries$name[df_countries$short_name_3 == df_speaker$country[1]]
  } else {
    if (!is.na(full_name)) item$speaker_full_name <- full_name
  }


  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
    tryCatch(
      {

        df[df$hub.key == item$hub.key,] <- item   

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


#######
# Fix speaker_full_name
#1
#patterns <- ", on behalf of|, rapporteur for|, representative of|, answer to|, Vice-President of|, former President|, Wife of|, President of|, Author of|, Winner of |, president of|, Prime Minister of"
#df_to_fix <- df[which(grepl(patterns, df$speaker_full_name) & is.na(df$speaker_polgroup)),]
#2
patterns <- "president|президент|predsjednik|başkan|prezident|formand|presidentti|président|präsident|Πρόεδρος|elnök|preside|uachtarán|Presidente|prezidents|prezidentas|presidint|prezydent|presedinte|predsednik|presidentea|presidente|chairman|chair|présidente|Präsident|President|Preşedinte|Preşedintele|Presedintele|in the chair|Mistopredseda|Präsidentin|Presedintia|Speaker"
df_to_fix <- df[which(grepl(tolower(patterns), tolower(df$speaker_full_name))),]


nrow(df_to_fix)
table(df_to_fix$speaker_type)
table(df_to_fix$speaker_full_name)

for (i in 1:nrow(df_to_fix)) {

  item <- df_to_fix[i,]

  cat("processing #", i, "/", nrow(df_to_fix), "key", item$hub.key, "\n")

  #1
  # full_name <- trimws(strsplit(item$speaker_full_name, ",")[[1]][1])
  # full_name <- gsub(president_patterns, "", full_name)
  # speaker_type <- trimws(strsplit(item$speaker_full_name, ",")[[1]][2])

  # df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
  # if (nrow(df_speaker) == 0) df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$full_name))),]
 
  # if (nrow(df_speaker) > 0) {
  #   item$speaker_full_name <- df_speaker$full_name[1]
  #   if (is.na(item$speaker_polgroup)) item$speaker_polgroup <- df_speaker$pol_group[1]
  #   item$speaker_party <- df_speaker$party[1]
  #   item$speaker_gender <- df_speaker$gender[1]
  #   item$speaker_country <- if (!is.na(item$speaker_country)) df_countries$name[df_countries$short_name_3 == df_speaker$country[1]] else NA
  # } else {
  #   if (!is.na(full_name)) item$speaker_full_name <- full_name
  # }

  # item$speaker_type <- speaker_type

  #2
  full_name <- item$president_name
  df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
  if (nrow(df_speaker) == 0) df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$full_name))),]
 
  if (nrow(df_speaker) > 0) {
    item$speaker_full_name <- df_speaker$full_name[1]
    if (is.na(item$speaker_polgroup)) item$speaker_polgroup <- df_speaker$pol_group[1]
    item$speaker_party <- df_speaker$party[1]
    item$speaker_gender <- df_speaker$gender[1]
    item$speaker_country <- if (!is.na(df_speaker$country[1])) df_countries$name[df_countries$short_name_3 == df_speaker$country[1]] else NA
  } else {
    if (!is.na(full_name)) item$speaker_full_name <- full_name
  }


  change <- TRUE
  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
    tryCatch(
      {

        df[df$hub.key == item$hub.key,] <- item   

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

########
#fix polgroup
#1
#df_to_fix <- df[is.na(df$speaker_polgroup),]
#2
#df_to_fix <- df[df$speaker_full_name == "Mélenchon, Jean-Luc",]
#3
#df_to_fix <- df[df$speaker_full_name == "Laid, Diogo",]
#4
#df_to_fix <- df[df$speaker_full_name == "Epitideos, George",]
#df_to_fix <- df[df$speaker_full_name == "Epitideos, Georgios",]
#5
#df_to_fix <- df[df$speaker_full_name == "Mogherini, Federica",]
#6
#df_to_fix <- df[df$speaker_full_name == "Jourová, Vĕra",]
#7
#df_to_fix <- df[df$speaker_full_name == "Hahn, Johannes",]
#8
#df_to_fix <- df[df$speaker_full_name == "Assise, François",]
#9
#df_to_fix <- df[df$speaker_full_name == "Zdzislaw Krasnodebski",]
#10
#df_to_fix <- df[df$speaker_full_name == "Timmermans, Frans",]
#11
#df_to_fix <- df[df$speaker_full_name == "Assis, Francisco",]
#12
#df_to_fix <- df[df$speaker_full_name == "Mauridis, Kostas",]
#13
df_to_fix <- df[df$speaker_full_name == "Avramopoulos, Dimitris",]



nrow(df_to_fix)


for (i in 1:nrow(df_to_fix)) {
  change <- FALSE

  item <- df_to_fix[i,]

  if (is.na(unique(as.list(item))[[1]])) next

  cat("processing #", i, "/", nrow(df_to_fix), "key", item$hub.key, "\n")

  full_name <- item$speaker_full_name
  full_name <- gsub("\\(","\\\\(",full_name)
  full_name <- gsub("\\)","\\\\)",full_name)
  
  df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
  if (nrow(df_speaker) == 0) df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$full_name))),]
 
  if (nrow(df_speaker) > 0) {
    df_speaker <- df_speaker[which.max(rowSums(!is.na(df_speaker))),]

    item$speaker_full_name <- df_speaker$full_name
    if (is.na(item$speaker_polgroup)) item$speaker_polgroup <- df_speaker$pol_group
    item$speaker_party <- df_speaker$party
    item$speaker_gender <- df_speaker$gender
    item$speaker_country <- if (!is.na(df_speaker$country) && nchar(df_speaker$country) == 3) df_countries$name[df_countries$short_name_3 == df_speaker$country] else NA
    item$speaker_country <- if (!is.na(df_speaker$country) && nchar(df_speaker$country) > 3)  df_speaker$country else item$speaker_country
    change <- TRUE
  } 

  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
    tryCatch(
      {

        df[df$hub.key == item$hub.key,] <- item   

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


########
#fix speaker_type
#1
#df_to_fix <- df[df$speaker_full_name == "Vella, Karmenu",]
#2
#df_to_fix <- df[df$speaker_full_name == "Bulc, Violeta",]
#3
#df_to_fix <- df[df$speaker_full_name == "Povilas Andriukaitis, Vytenis",]
#4
#df_to_fix <- df[df$speaker_full_name == "Mimica, Neven",]
#5
df_to_fix <- df[df$speaker_full_name == "Bieńkowska, Elżbieta",]

nrow(df_to_fix)


for (i in 1:nrow(df_to_fix)) {
  change <- FALSE

  item <- df_to_fix[i,]

  if (is.na(unique(as.list(item))[[1]])) next

  cat("processing #", i, "/", nrow(df_to_fix), "key", item$hub.key, "\n")

  full_name <- item$speaker_full_name
  full_name <- gsub("\\(","\\\\(",full_name)
  full_name <- gsub("\\)","\\\\)",full_name)
  
  df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$other_names))),]
  if (nrow(df_speaker) == 0) df_speaker <- df_people[which(grepl(tolower(full_name), tolower(df_people$full_name))),]
 
  if (nrow(df_speaker) > 0) {
    df_speaker <- df_speaker[which.max(rowSums(!is.na(df_speaker))),]

    item$speaker_full_name <- df_speaker$full_name
    if (is.na(item$speaker_polgroup)) item$speaker_polgroup <- df_speaker$pol_group
    item$speaker_party <- df_speaker$party
    item$speaker_gender <- df_speaker$gender
    item$speaker_country <- if (!is.na(df_speaker$country) && nchar(df_speaker$country) == 3) df_countries$name[df_countries$short_name_3 == df_speaker$country] else NA
    item$speaker_country <- if (!is.na(df_speaker$country) && nchar(df_speaker$country) > 3)  df_speaker$country else item$speaker_country
    if (item$speaker_type == "Member Of The Commission") item$speaker_type <- df_speaker$type
    change <- TRUE
  } 

  success <- FALSE
  attempt <- 1

  while (change && !success && attempt < 20) {
    cat("writing ", item$hub.key, "\n")
    tryCatch(
      {

        df[df$hub.key == item$hub.key,] <- item   

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
