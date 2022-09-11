###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        <e|l|r_name_of_the_etl_script>                       #
#                                                                             #
#  Describe what the script does.  You can copy/paste part of the README.md   #
#  of the folder which this script resides in                                 #
#                                                                             #
###############################################################################



###############################################################################
########################           Functions            ######################
###############################################################################

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

load_polimeters <- function(lake_item) {
  
  xlsx_doc <- NULL
  
  lake_file_url <- lake_item$file
  r <- openxlsx::read.xlsx(lake_file_url, 2)
  
  if (r$status_code == 200) {
    xlsx_doc <- httr::content(r, as="raw")
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to fetch the", lake_item$metadata$country, "Polimeter document at URL", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
  
  if (!is.null(xlsx_doc)) {
    names(r) <- # rename duplicate column names
      make.unique(names(r))
    HistoricalPolimeters <- dplyr::slice( # remove mandates with incomplete info
      r, -c(1:2, 19:22)) |>
      dplyr::select(!c( # remove columns with duplicate info
        Réalisée:Santé.et.services.sociaux, X25, X29, X33, X37, X41, X45,
        X49, X53, X57, X61, X65, X69))
    HistoricalPolimetersArranged <- tidyr::pivot_longer( # transform into a data
      HistoricalPolimeters, # frame with one row per fulfillment category
      cols = # per public policy domain per mandate
        Affaires.internationales.et.Défense.1:X68)
    names(HistoricalPolimetersArranged) <- c(
      "province_or_state", "legislature", "first_minister",
      "government_type", "policy_domain", "number_pledges")
    HistoricalPolimetersArranged$verdict <- rep(c("kept", "partially_kept",
                                                  "broken"), 192)
    HistoricalPolimetersArranged$policy_domain <- rep(c(
      rep("Affaires internationales et défense", 3),
      rep("Arts et culture", 3),
      rep("Économie et employabilité", 3),
      rep("Éducation et recherche", 3),
      rep("Environnement", 3),
      rep("Familles", 3),
      rep("Gouvernement et démocratie", 3),
      rep("Identité et nationalisme", 3),
      rep("Loi et ordre", 3),
      rep("Minorités", 3),
      rep("Régions et agriculture", 3),
      rep("Santé et services sociaux", 3)), 16)
    HistoricalPolimetersArranged$country <- "CAN"
    
    batch_object <- df_to_batch_list(HistoricalPolimetersArranged)
    hublot::batch_create_table_items("clhub_tables_warehouse_polimeter_historical",
                                     batch_object, credentials = credentials)
    
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to parse the", lake_item$metadata$country, "Polimeter document at", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
}



###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################


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
  
  filter <- list(path = "polimeter_historical")
  data <- hublot::filter_lake_items(credentials, filter = filter)
  
  for (i in 1:length(data$results)) {
    if (data$results[[i]]$metadata$country == "CAN") load_polimeters(data$results[[i]])
  }
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
      if (!exists("scriptname")) scriptname <<- "l_polimeter_historical"
      
      # Uncomment the line below to hardcode the command line option passed to this script when it runs
      # This is particularly useful while developping your script but it's wiser to use real command-
      # line options when puting your script in production in an automated container.
      # opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)
      
      if (!exists("opt")) {
        opt <- clessnverse::process_command_line_options()
      }
      
      if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
      
      
      # login to hublot
      clessnverse::logit(scriptname, "connecting to hub", logger)
      
      credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))
      
      
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
