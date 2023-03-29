# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))


table_name <- "clhub_tables_mart_agoraplus_european_parliament"



#####
Fix polgroups

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

    cat("processing #", j, "key", item$hub.key, "\n")

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
Fix speaker_full_name




#####
Fix speaker_gender