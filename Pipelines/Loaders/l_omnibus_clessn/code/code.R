###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        l_omnibus_clessn                                     #
#                                                                             #
#  This loader does the following:                                            #
#     1. Loads all the omnibus surveys                                        #
#     2. Loads all additional data related to them                            #
#        (open questions and postal codes) and links them to the surveys      #
#     3. Merges all the surveys together, leaving NAs when a question was not #
#         asked                                                               #
#     4. Adds the provincial riding of the respondent                         #
#     5. Adds the provincial weight and the riding weight of the respondent   #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################

# login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"),
        Sys.getenv("HUB3_USERNAME"),
        Sys.getenv("HUB3_PASSWORD"))

###############################################################################
########################           Functions            ######################
###############################################################################

# Declare and define all the general-purpose functions specific to this script.
# If there are functions that are shared between many scripts, you should ask
# yourself whether they should go to in the clessnverse:: Package
#
# Ex:
#      get_lake_press_releases <- functions(parties_list) {
#         your function code goes here
#      }

    # Function to join postal code data to each omnibus survey
    # Takes a `monthyear` id, a survey_list_tables and a
    # postalcodes_list_tables as arguments
    # Returns the survey data with the postalcode column
    join_survey_postalcodes <- function(monthyear, survey_list_tables, postalcodes_list_tables){
      #monthyear <- "january2022"
      #survey_list_tables <- get_omnibus_survey()
      #postalcodes_list_tables <- get_omnibus_postalcodes()
      survey <- survey_list_tables[[monthyear]]
      postalcodes <- postalcodes_list_tables[[monthyear]]
      names(postalcodes) <- c("id", "postal_code")
      names(survey)[1] <- "id"
      output <- dplyr::left_join(survey, postalcodes, by = "id")
      return(output)
    }

# Cleaning functions ####
    # These functions are used to clean CLESSN variables.

    # Function to get the month from a monthyear_id of an omnibus survey
    get_omnibus_month <- function(monthyear_id){
      month <- gsub("[[:digit:]]", "", monthyear_id)
      return(month)
    }

    # Functions to clean the age variable of an omnibus
    clean_age24m <- function(raw_data_vector){
      ses_age24m <- rep(NA, length(raw_data_vector))
      ses_age24m[raw_data_vector==2] <- 1
      ses_age24m[raw_data_vector!=2] <- 0
      return(ses_age24m)
    }
    clean_age25p34 <- function(raw_data_vector){
      ses_age25p34 <- rep(NA, length(raw_data_vector))
      ses_age25p34[raw_data_vector==3] <- 1
      ses_age25p34[raw_data_vector!=3] <- 0
      return(ses_age25p34)
    }
    clean_age35p44 <- function(raw_data_vector){
      ses_age35p44 <- rep(NA, length(raw_data_vector))
      ses_age35p44[raw_data_vector==4] <- 1
      ses_age35p44[raw_data_vector!=4] <- 0
      return(ses_age35p44)
    }
    clean_age45p54 <- function(raw_data_vector){
      ses_age45p54 <- rep(NA, length(raw_data_vector))
      ses_age45p54[raw_data_vector==5] <- 1
      ses_age45p54[raw_data_vector!=5] <- 0
      return(ses_age45p54)
    }
    clean_age55p <- function(raw_data_vector){
      ses_age55p <- rep(NA, length(raw_data_vector))
      ses_age55p[raw_data_vector>=6] <- 1
      ses_age55p[raw_data_vector<=6] <- 0
      return(ses_age55p)
    }
    clean_ageAll <- function(raw_data_vector){
      ses_ageAll <- rep(NA, length(raw_data_vector))
      ses_ageAll[raw_data_vector==2] <- "24m"
      ses_ageAll[raw_data_vector==3] <- "25p34"
      ses_ageAll[raw_data_vector==4] <- "35p44"
      ses_ageAll[raw_data_vector==5] <- "45p54"
      ses_ageAll[raw_data_vector==6] <- "55p64"
      ses_ageAll[raw_data_vector==7] <- "65p74"
      ses_ageAll[raw_data_vector==8] <- "75p"
      return(ses_ageAll)
    }

    # Function to clean gender question
    clean_female <- function(raw_data_vector){
      ses_female <- rep(NA, length(raw_data_vector))
      ses_female[raw_data_vector==1]<- 0
      ses_female[raw_data_vector==2]<- 1
      return(ses_female)
    }

    # Functions to clean regions
    clean_mtlIsle <- function(raw_data_vector){
      ses_mtlIsle <- rep(NA, length(raw_data_vector))
      ses_mtlIsle[raw_data_vector==1] <- 1
      ses_mtlIsle[raw_data_vector!=1&!is.na(raw_data_vector)] <- 0
      return(ses_mtlIsle)
    }

    clean_mtlRegion <- function(raw_data_vector){
      ses_mtlRegion <- rep(NA, length(raw_data_vector))
      ses_mtlRegion[raw_data_vector==2] <- 1
      ses_mtlRegion[raw_data_vector!=2&!is.na(raw_data_vector)] <- 0
      return(ses_mtlRegion)
    }

    clean_qcCity <- function(raw_data_vector){
      ses_qcCity <- rep(NA, length(raw_data_vector))
      ses_qcCity[raw_data_vector==3] <- 1
      ses_qcCity[raw_data_vector!=3&!is.na(raw_data_vector)] <- 0
      return(ses_qcCity)
    }

    clean_noMtlQc <- function(raw_data_vector){
      ses_noMtlQc <- rep(NA, length(raw_data_vector))
      ses_noMtlQc[raw_data_vector==4] <- 1
      ses_noMtlQc[raw_data_vector!=4&!is.na(raw_data_vector)] <- 0
      return(ses_noMtlQc)
    }

    # Functions to clean language questions
    clean_langFr <- function(raw_data_vector){
      ses_langFr <- rep(NA, length(raw_data_vector))
      ses_langFr[raw_data_vector==1] <- 1
      ses_langFr[raw_data_vector!=1&!is.na(raw_data_vector)] <- 0
      return(ses_langFr)
    }

    clean_langEn <- function(raw_data_vector){
      ses_langEn <- rep(NA, length(raw_data_vector))
      ses_langEn[raw_data_vector==2] <- 1
      ses_langEn[raw_data_vector!=2&!is.na(raw_data_vector)] <- 0
      return(ses_langEn)
    }

    clean_langOthr <- function(raw_data_vector){
      ses_langOthr <- rep(NA, length(raw_data_vector))
      ses_langOthr[raw_data_vector==3] <- 1
      ses_langOthr[raw_data_vector!=3&!is.na(raw_data_vector)] <- 0
      return(ses_langOthr)
    }

    # Functions to clean school questions
    clean_educHsOrBelow <- function(raw_data_vector){
      ses_educHsOrBelow <- rep(NA, length(raw_data_vector))
      ses_educHsOrBelow[raw_data_vector==1] <- 1
      ses_educHsOrBelow[raw_data_vector!=1&!is.na(raw_data_vector)] <- 0
      return(ses_educHsOrBelow)
    }

    clean_educColl <- function(raw_data_vector){
      ses_educColl <- rep(NA, length(raw_data_vector))
      ses_educColl[raw_data_vector==2] <- 1
      ses_educColl[raw_data_vector!=2&!is.na(raw_data_vector)] <- 0
      return(ses_educColl)
    }

    clean_educUniv <- function(raw_data_vector){
      ses_educUniv <- rep(NA, length(raw_data_vector))
      ses_educUniv[raw_data_vector==3] <- 1
      ses_educUniv[raw_data_vector!=3&!is.na(raw_data_vector)] <- 0
      return(ses_educUniv)
    }

    # Functions to clean employment questions
    clean_educUniv <- function(raw_data_vector){
      ses_educUniv <- rep(NA, length(raw_data_vector))
      ses_educUniv[raw_data_vector==3] <- 1
      ses_educUniv[raw_data_vector!=3&!is.na(raw_data_vector)] <- 0
      return(ses_educUniv)
    }









###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################

    # Function to get survey data from all omnibus
    get_omnibus_survey <- function() {
      # Note pour l'implémentation :
      # log for each file processed
      lake_items_selection_metadata <-
        list(
          metadata__content_type = "omnibus_survey",
          metadata__storage_class = "lake"
        )
      hublot_list <- hublot::filter_lake_items(credentials, lake_items_selection_metadata)
      list_tables <- list()
      for (i in seq_len(length(hublot_list[[1]]))){
        df <- haven::read_sav(hublot_list[[1]][[i]]$file)
        list_tables[[i]] <- df
        names(list_tables)[i] <- hublot_list[["results"]][[i]][["key"]]
        clessnverse::logit(
          scriptname,
          paste0("      ", hublot_list[[1]][[i]]$key, " added to list_tables"),
          logger
        )
      }
      return(list_tables)
    }

    # Function to get postal codes data from all omnibus
    get_omnibus_postalcodes <- function() {
      # Note pour l'implémentation :
      # log for each file processed
      lake_items_selection_metadata <-
        list(
          metadata__content_type = "respondents_postal_codes",
          metadata__storage_class = "lake"
        )
      hublot_list <- hublot::filter_lake_items(credentials, lake_items_selection_metadata)
      list_tables <- list()
      for (i in seq_len(length(hublot_list[[1]]))){
        #i <- 3
        file <- hublot_list[[1]][i]
        df <- utils::read.csv(file[[1]][["file"]], sep = ";")
        list_tables[[i]] <- df
        names(list_tables)[i] <- gsub("postalcodes_", "", hublot_list[["results"]][[i]][["key"]])
        clessnverse::logit(
          scriptname,
          paste0("      ", hublot_list[[1]][[i]]$key, " added to list_tables"),
          logger
        )
      }
      return(list_tables)
    }


###############################################################################
######################            Functions to           ######################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################


###############################################################################
######################            Functions to          ######################
######################   Get Data Sources from Dropbox   ######################
###############################################################################


###############################################################################
########################               Main              ######################
## This function is the core of your script. It can use global R objects and ##
## variables that you can define in the tryCatch section of the script below ##
###############################################################################

main <- function() {
    ###########################################################################
    # Define local objects of your core algorithm here
    # Ex: parties_list <- list("CAQ", "PLQ", "QS", "PCQ", "PQ")


    ###########################################################################
    # Start your main script here using the best practices in activity logging
    #
    # Ex: warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0)
    #     clessnverse::logit(scriptname, "Getting political parties press releases from the datalake", logger)
    #     lakes_items_list <- get_lake_press_releases(parties_list)
    #
  # Initial logging
  # Get lake items
  clessnverse::logit(
    scriptname,
    "Survey data items downloading beginning...",
    logger
  )
  survey_list_tables <- get_omnibus_survey()
  clessnverse::logit(
    scriptname,
    paste(length(survey_list_tables),
          "survey data tables downloaded from the lake. End of downloading."
    ),
    logger
  )

  clessnverse::logit(
    scriptname,
    "Postal codes data items downloading beginning...",
    logger
  )
  postalcodes_list_tables <- get_omnibus_postalcodes()
  clessnverse::logit(
    scriptname,
    paste(length(postalcodes_list_tables),
          "postal codes data tables downloaded from the lake. End of downloading."
    ),
    logger
  )

  clessnverse::logit(
    scriptname,
    "Joining postal codes on survey data",
    logger
  )

  list_of_dfs <- list()
  for (i in seq_len(length(survey_list_tables))){
    df <- join_survey_postalcodes(names(survey_list_tables)[i],
                                  survey_list_tables,
                                  postalcodes_list_tables)
    list_of_dfs[[i]] <- df
    names(list_of_dfs)[i] <- names(survey_list_tables)[i]
  }


  # Process lake items (return input_df)
  clessnverse::logit(
    scriptname,
    "Data processing beginning...",
    logger
  )




}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch(
  withCallingHandlers(
  {
    # Package dplyr for the %>%
    # All other packages must be invoked by specifying its name
    # in front ot the function to be called
    library(dplyr)

    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #



    #########################################################################
    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"




    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally")
    if (!exists("scriptname")) scriptname <<- "l_omnibus_clessn"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    # opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::process_command_line_options()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

    # if your script uses hub 2.0 uncomment the line below
    # clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    # or
    # use this one
    # clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

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
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::log_close(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)

    quit(status = status)
  }
)

