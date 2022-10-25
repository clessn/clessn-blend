###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        l_qc125                                              #
#                                                                             #
#     This script merges the csv located in the lake/qc125 path together      #
#     in the qc125_predictions warehouse_table                                #
#                                                                             #
###############################################################################

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

df_to_batch_list <- function(df){
  final_list <- list()
  for (i in 1:nrow(df)){
    data_list <- c(df[i,])
    listi <- list(
      key = i,
      data = data_list
    )
    final_list[[i]] <- listi
    if (i %% 100 == 0){
      print(paste0(i, "/", nrow(df)))
    }
  }
  return(final_list)
}

###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################
    get_lake_items_qc125 <- function() {
      # Note pour l'implémentation :
      # log for each file processed
      lake_items_selection_metadata <-
        list(
          metadata__content_type = "qc125_preds",
          metadata__storage_class = "lake"
        )
      hublot_list <- hublot::filter_lake_items(credentials, lake_items_selection_metadata)
      list_tables <- list()
      for (i in seq_len(length(hublot_list[[1]]))){
        df <- utils::read.csv2(hublot_list[[1]][[i]]$file)
        list_tables[[i]] <- df
        clessnverse::logit(
          scriptname,
          paste0("      ", hublot_list[[1]][[i]]$key, " added to list_tables"),
          logger
        )
      }
      return(list_tables)
    }

    upload_warehouse_qc125 <- function(input_df) {
      n <- nrow(input_df)
      for (i in seq_len(n)) {
        row <- as.list(input_df[i, ])
        clessnverse::commit_warehouse_row(
          table = "qc125",
          key = input_df$unique_id[i], ### Clé des données GlobalES
          row,
          mode = "refresh",
          credentials
        )
        if (i%%50==0){
          print(paste0(i, "/", n, "-", round(i/n*100), "%"))
        }
      }
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
  riding_infos <- ridings <- read.csv2(hublot::retrieve_file("prov_ridings_ids", credentials)[["file"]]) %>%
    mutate(riding_id = as.character(riding_id)) %>%
    select(riding_name, riding_id)
  list_tables <- get_lake_items_qc125()
  df <- dplyr::bind_rows(list_tables) %>%
    mutate(unique_id = 1:nrow(.)) %>%
    select(unique_id, names(.), - riding_id) %>%
    left_join(., riding_infos, by = "riding_name")
  clessnverse::logit(
    scriptname,
    paste0("   Qc125 data from all dates are now binded together"),
    logger
  )
<<<<<<< HEAD
  clessnverse::logit(
    scriptname,
    paste0("Checking to see if the qc125 warehouse table already exists"),
    logger
  )
  current_warehouse <- clessnverse::get_warehouse_table("qc125", credentials)
  ids_to_delete <- current_warehouse$hub.id
  hublot::batch_delete_table_items("clhub_tables_warehouse_qc125", ids_to_delete, credentials)
=======
>>>>>>> 4a39f0df1ffedacaa0440946a97294d6360475f4
  batch_object <- df_to_batch_list(df)
  hublot::batch_create_table_items("clhub_tables_warehouse_qc125", batch_object, credentials)
  #upload_warehouse_qc125(df)
  return(df)
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
    if (!exists("scriptname")) scriptname <<- "l_qc125"

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

    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    credentials <- hublot::get_credentials(
      Sys.getenv("HUB3_URL"),
      Sys.getenv("HUB3_USERNAME"),
      Sys.getenv("HUB3_PASSWORD"))

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

