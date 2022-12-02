###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                         l_datagotchi_elxn_qc2022
#                                                                             #
# Script pour créer la table d'entrepôt contenant les données de datagotchi
# de l'élection québécoise 2022.
# - Télécharge les données de aws
# - Modifie les valeurs des variables catégorielles
# - Retire les variables non utiles aux analyses
#
# Les variables d'environnement suivantes doivent être configurées
#
# DGAPP_URL <- "https://api.datagotchi.com/"
# DGAPP_USERNAME
# DGAPP_PASSWORD
#                                                                             #
###############################################################################

# Until it is automated - set time interval to upload
# KEEP LAST TIME INTERVAL UPLOADED
#
 interval_min <- ISOdatetime(2022,9,8,14,15,0)
 interval_max <- ISOdatetime(2022,10,04,00,00,0)

# interval_min <- ISOdatetime(2022,10,3,00,00,0)
# interval_max <- ISOdatetime(2022,10,3,07,00,0)
# interval_min <- ISOdatetime(2022,10,3,07,00,0)
# interval_max <- ISOdatetime(2022,10,3,09,00,0)
# interval_min <- ISOdatetime(2022,10,3,09,00,0)
# interval_max <- ISOdatetime(2022,10,3,12,00,0)
# interval_min <- ISOdatetime(2022,10,3,12,00,0)
# interval_max <- ISOdatetime(2022,10,3,15,00,0)
# interval_min <- ISOdatetime(2022,10,3,15,00,0)
# interval_max <- ISOdatetime(2022,10,3,17,00,0)
# interval_min <- ISOdatetime(2022,10,3,17,00,0)
# interval_max <- ISOdatetime(2022,10,3,18,00,0)
# interval_min <- ISOdatetime(2022,10,3,18,00,0)
# interval_max <- ISOdatetime(2022,10,3,19,00,0)
# interval_min <- ISOdatetime(2022,10,3,19,00,0)
# interval_max <- ISOdatetime(2022,10,3,21,30,0)
# interval_min <- ISOdatetime(2022,10,3,21,30,0)
# interval_max <- ISOdatetime(2022,10,4,00,00,0)


###############################################################################
########################           Functions            ######################
###############################################################################


#' fetch JWT token to log in to api
fetch_token <- function(username, password, url)
{
  clessnverse::logit(
    scriptname,
    "fetch_token",
    logger
  )
  response <- httr::POST(url=paste0(url, 'api/token/'), body = list(username=username, password=password), verify=F)
  if (response$status_code != 200)
  {
    stop(paste0("Erreur lors de la connexion : ", response$status_code))
  }
  access_token <- httr::content(response)$access
  return(access_token)
}

#' fetch one set of data between two datetimes
fetch_data <- function(date_gte, date_lte, username, password, token, url)
{
  response <- httr::GET(url=paste0(url, "visitors_complete/", "filter/", "?", "gte=", date_gte, "&lte=", date_lte),
                        config=httr::add_headers(Authorization=paste("Bearer", token)), verify=F)
  if (response$status_code != 200)
  {
    if (response$status_code == 403)
    {
      token = fetch_token(username, password, url)
      response <- httr::GET(url=paste0(url, "visitors_complete/", "filter/", "?", "gte=", date_gte, "&lte=", date_lte),
                            config=httr::add_headers(Authorization=paste("Bearer", token)), verify=F)
      if (response$status_code != 200)
      {
        stop(paste0("Erreur du téléchargement : ", response$status_code))
      }
    }
    stop(paste0("Erreur du téléchargement : ", response$status_code))
  }

  data <- httr::content(response)
  df <- tidyjson::spread_all(data)
#  df <- jsonlite::fromJSON(httr::content(response, "text"), flatten = TRUE)
  return(df)
}

#' fetch the current survey and extract all choice uuids and their text in the specified language
fetch_survey_choices <- function(url, lang="fr")
{
  clessnverse::logit(
    scriptname,
    "fetch_survey_choices",
    logger
  )
  response <- httr::GET(paste0(url, "surveys/"), httr::add_headers("Accept-Language" = lang))
  questions <- httr::content(response)$results[[1]]$category_set[[1]]$question_set
  choice_uuids <- list()
  choice_texts <- list()

  for (question in questions)
  {
    if ("choice_set" %in% names(question))
    {
      for (choice in question$choice_set)
      {
        choice_uuids <- append(choice_uuids, choice$id)
        choice_texts <- append(choice_texts, choice$text)
      }
    }
  }
  choices <- data.frame(uuid = unlist(choice_uuids), text = unlist(choice_texts), stringsAsFactors = FALSE)
  return(choices)
}

#' replace answer choices uuid with their text equivalent
replace_choices_with_text <- function(answers_df, choices_df)
{
  clessnverse::logit(
    scriptname,
    "replace_choices_with_text",
    logger
  )
  for (i in 1:nrow(choices_df))
  {
    uuid <- choices_df$uuid[[i]]
    text <- choices_df$text[[i]]
#    clessnverse::logit(
#      scriptname,
#      paste(i, "/", nrow(choices_df), ":", text),
#      logger
#    )
    answers_df[answers_df == uuid] <- text
  }
  return(answers_df)
}

#' execute fetch_data for a list of datetime intervals
fetch_all_data <- function(interval, username, password, url)
{
  clessnverse::logit(
    scriptname,
    "fetch_all_data",
    logger
  )
  token <- fetch_token(username, password, url)
  interval[[length(interval)+1]] <- "null"
  all_data <- NULL

  for (i in 1:length(interval))
  {
    gte <- interval[[i]]
    lte <- interval[[i+1]]
    if (interval[[i+1]] == "null")
    {
      return(all_data)
    }

    clessnverse::logit(
        scriptname,
        paste("between", gte, "and", lte),
        logger
    )
    data <- fetch_data(gte, lte, username, password, token, url)


    if (is.null(all_data))
    {
      all_data <- data
    }
    else
    {
      all_data <- dplyr::bind_rows(all_data, data)
    }
    clessnverse::logit(
        scriptname,
        paste("period:", nrow(data), "total:", nrow(all_data)),
        logger
    )
  }
}

generate_interval <- function(gte, lte, by)
{
  interval <- seq(gte, lte, by=by)
  interval <- lapply(interval, function(x) strftime(x, format="%Y-%m-%d %H:%M:%S"))
  return(interval)

}

###############################################################################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################


###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################


###############################################################################
######################   Get Data Sources from Dropbox   ######################
###############################################################################


###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {

    clessnverse::logit(scriptname,  "starting main function...", logger)
    clessnverse::logit(scriptname, paste("Getting datagotchi data from", interval_min, "to", interval_max, "."), logger)

    # Put your code here
    # Create functions above and call theme here to make your code more readable
    # Use global variables scriptname, logger, opt and status
    # Log lots of activity
    # Exit your script with stop(...) when there is a fatal error
    # Call warning() to issue warnings and alert the clessn that some non fatal problem occured during execution

    # generate a set of dates at 15 minute intervals
    interval <- generate_interval(interval_min, interval_max, by=(60*15))

    data <- fetch_all_data(interval, username, password, url)
    clessnverse::logit(
        scriptname,
        paste("total:", nrow(data), "rows uploaded."),
        logger
    )
    choices <- fetch_survey_choices(url, lang = "fr")
    data <- data.table::as.data.table(data)

    # clean data
    data$"..JSON" <- NULL
    data$"document.id" <- NULL
    data$"survey" <- NULL

    if ("data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.data.answers.music.pixelated_artwork_url" %in% colnames(data)) {
      data$`data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.data.answers.music.pixelated_artwork_url` <- NULL
    }

    if ("data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.data.answers.cinema.pixelated_artwork_url" %in% colnames(data)) {
      data$`data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.data.answers.cinema.pixelated_artwork_url` <- NULL
    }
    #data$`data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.data.answers.birthplace.pixelated_artwork_url` <- NULL
    data$`data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.data.visitor_id` <- NULL
#    data$`data.answers.87b164af-2e19-4453-ab80-22b2ef380d95.prediction.data` <- NULL

    # Replace uuid categories by human readable categories
    data_text <- replace_choices_with_text(data, choices)

    # Upload data
    clessnverse::logit(
    scriptname,
    "Data row upload begin",
    logger
    )
    for (i in seq_len(nrow(data_text))) {
        row <- as.list(data_text[i, ])

        clessnverse::commit_warehouse_row(
            table = warehouse_table_name,
            key = data_text$"id"[i], ### Clé des données datagotchi
            row,
            refresh_data = TRUE,
            credentials
        )

        if (i %% 50 == 0) {
            clessnverse::logit(
            scriptname,
            paste0(
                "Commiting ",
                i,
                "th entry."
            ),
            logger
            )
        }
    }

    clessnverse::logit(
      scriptname,
      paste0(nrow(data_text),
            " lines uploaded in the datagotchi qc2022 warehouse table : ",
            warehouse_table_name
      ),
      logger
    )

    clessnverse::logit(scriptname, "Datagotchi data loaded.", logger)
    clessnverse::logit(scriptname, "...ending main function.", logger)

}


tryCatch(
    withCallingHandlers(
    {
        # Package
        library(dplyr)

        Sys.setlocale("LC_TIME", "fr_CA.UTF-8")

        # Globals : scriptname, opt, logger, credentials
        warehouse_table_name <- "dg_elxn-qc2022"

        if (!exists("scriptname")) scriptname <<- "l_datagotchi_elxn_qc2022"

        opt <- list(
            dataframe_mode = "refresh",
            log_output = c("file"),
            hub_mode = "refresh",
            download_data = FALSE,
            translate=FALSE,
            refresh_data=TRUE
        )

        if (!exists("opt")) {
            opt <- clessnverse::process_command_line_options()
        }

        if (!exists("logger") || is.null(logger) || logger == 0) {
            logger <<-
                clessnverse::log_init(
                    scriptname,
                    opt$log_output,
                    Sys.getenv("LOG_PATH")
                )
        }

        # datagotchi app credentials
        url      <- Sys.getenv("DGAPP_URL")
        username <- Sys.getenv("DGAPP_USERNAME")
        password <- Sys.getenv("DGAPP_PASSWORD")

        # login to hublot
        clessnverse::logit(scriptname, "Connecting to hub3", logger)
        credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"),
            Sys.getenv("HUB3_USERNAME"),
            Sys.getenv("HUB3_PASSWORD"))

        clessnverse::logit(
            scriptname,
            paste("Execution of", scriptname, "starting"),
            logger
        )

        status <<- 0

        # Call main script
        main()
    },

    warning = function(w) {
        clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
        print(w)
        status <<- 2
    }),

    # Handle an error or a call to stop function in the code
    error = function(e) {
        clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
        print(e)
        status <<- 1
    },

    # Terminate gracefully whether error or not
    finally={
        clessnverse::logit(
            scriptname,
            paste("Execution of", scriptname, "program terminated. Status :", status),
        logger)
        clessnverse::log_close(logger)

        # Cleanup
        closeAllConnections()
        rm(logger)
        cat("exit status: ", status, "\n")
        quit(status = status)
    }
)
