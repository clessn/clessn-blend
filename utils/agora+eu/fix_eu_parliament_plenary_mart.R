# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


table_name <- "clhub_tables_mart_agoraplus_european_parliament"



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
  )

  
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
    data__event_date__lte="2014-12-30"
  ),
  credentials = credentials,
  nbrows = 0
)

# Fix1
#df_to_fix <- df[which(nchar(df$speaker_full_name) > 30),]

# Fix2
#df_to_fix <- df[which(grepl("in writing", df$speaker_full_name)),]

# Fix3
#df_to_fix <- df[which(grepl("The president", df$speaker_full_name)),]
#df_to_fix$president_name[df_to_fix$president_name == "Rainer Wieland Catch-The-Eye-Verfahren"] <- "Rainer Wieland"
#df_to_fix$president_name <- trimws(df_to_fix$president_name)
#df_to_fix$president_name[grepl("Als Nachster Punkt Folgt Die Abstimmungsstunde", df_to_fix$president_name)] <- "Martin Schulz"
#df_to_fix$president_name[df_to_fix$president_name == "Προεδρια Αννυ Ποδηματα"] <- "Anni Podimata"

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
df_to_fix <- df[which(grepl("^.*(\\(.*\\))$", df$speaker_full_name)),]

for (i in 1:nrow(df_to_fix)) {
  df_speaker <- data.frame()
  
  item <- df_to_fix[i,]
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

  item$country <- df_countries$name[df_countries$short_name_3 == item$speaker_country]

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
# Fix speaker_country
df_to_fix <- df[which(is.na(df$speaker_gender)),]

for (i in 1:nrow(df_to_fix)) {

  item <- df_to_fix[i,]

  df_gender <- gender::gender(item$speaker_full_name)
  if (nrow(df_gender) > 0 ) item$speaker_gender <- df_gender$gender

  item$country <- df_countries$name[df_countries$short_name_3 == item$speaker_country]

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
