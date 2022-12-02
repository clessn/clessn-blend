###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        <l_party_manifestos_can2021>                       #
#                                                                             #
#  This script loads Canadian party manifesto data from the 2021 election
#  and transforms them into a data warehouse
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

load_plcen_manifesto <- function(lake_item) {
  
  pdf_doc <- NULL
  
  lake_file_url <- lake_item$file
  r <- httr::GET(lake_file_url)
  
  if (r$status_code == 200) {
    pdf_doc <- httr::content(r, as="raw")
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to fetch the", lake_item$metadata$political_party, "manifesto at URL", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
  # for tests: pdf_doc <- httr::content(httr::GET(data$results[[13]]$file), as="raw")
  if (!is.null(pdf_doc)) {
    PLCENPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\\.\\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_replace_all("\n", " ") |> # replace line breaks by spaces
      stringr::str_remove_all("Forward. For Everyone. \\d+ ") |>
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(PLCENPlatform) <- "paragraph"
    PLCENPlatform[PLCENPlatform == ""] <- NA
    PLCENPlatform <- na.omit(PLCENPlatform)
    PLCENPlatform$uppercase_first <- stringr::str_detect( # identify paragraphs
      PLCENPlatform$paragraph, "^[a-z]") == FALSE # starting with a lowercase
    PLCENPlatform$group <- NA
    PLCENPlatform$group[PLCENPlatform$uppercase_first == T] <- 1:length(
      PLCENPlatform$group[PLCENPlatform$uppercase_first == T])
    PLCENPlatform <- PLCENPlatform |>
      tidyr::fill(group, .direction = "down") |> # group paragraphs with the
      dplyr::group_by(group) |> # previous paragraph starting with uppercase
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "PLC",
                    election_year = 2021,
                    original_doc_type = "pdf",
                    release_date = as.Date("2021-09-01"),
                    title = "Forward. For Everyone.",
                    country = "CAN",
                    language = "EN",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    
    index_column <- rep(1:nrow(PLCENPlatform))
    PLCENPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_can2021", 
                                        df = PLCENPlatform, 
                                        key_columns = "paragraph+political_party+index", 
                                        key_encoding = "digest",
                                        refresh_data = TRUE, 
                                        credentials = credentials)
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to parse the", lake_item$metadata$political_party, "manifesto document at", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
}


load_plcfr_manifesto <- function(lake_item) {
  
  pdf_doc <- NULL
  
  lake_file_url <- lake_item$file
  r <- httr::GET(lake_file_url)
  
  if (r$status_code == 200) {
    pdf_doc <- httr::content(r, as="raw")
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to fetch the", lake_item$metadata$political_party, "manifesto at URL", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
  # for tests: pdf_doc <- httr::content(httr::GET(data$results[[12]]$file), as="raw")
  if (!is.null(pdf_doc)) {
    PLCFRPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      unlist() |> # transform list into one big vector
      stringr::str_split("\\.\\n") |> # unmerge separate paragraphs
      stringr::str_replace_all("\n", " ") |> # replace line breaks by spaces
      stringr::str_remove_all("Avançons ensemble \\d+ ") |>
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(PLCFRPlatform) <- "paragraph"
    PLCFRPlatform[PLCFRPlatform == ""] <- NA
    PLCFRPlatform <- na.omit(PLCFRPlatform)
    PLCFRPlatform$uppercase_first <- stringr::str_detect( # identify paragraphs
      PLCFRPlatform$paragraph, "^[a-z]") == FALSE # starting with a lowercase
    PLCFRPlatform$group <- NA
    PLCFRPlatform$group[PLCFRPlatform$uppercase_first == T] <- 1:length(
      PLCFRPlatform$group[PLCFRPlatform$uppercase_first == T])
    PLCFRPlatform <- PLCFRPlatform |>
      tidyr::fill(group, .direction = "down") |> # group paragraphs with the
      dplyr::group_by(group) |> # previous paragraph starting with uppercase
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "PLC",
                    election_year = 2021,
                    original_doc_type = "pdf",
                    release_date = as.Date("2021-09-01"),
                    title = "Avançons ensemble",
                    country = "CAN",
                    language = "FR",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    
    index_column <- rep(1:nrow(PLCFRPlatform))
    PLCFRPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_can2021", 
                                        df = PLCFRPlatform, 
                                        key_columns = "paragraph+political_party+index", 
                                        key_encoding = "digest",
                                        refresh_data = TRUE, 
                                        credentials = credentials)
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to parse the", lake_item$metadata$political_party, "manifesto document at", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
}

load_pccen_manifesto <- function(lake_item) {
  
  pdf_doc <- NULL
  
  lake_file_url <- lake_item$file
  r <- httr::GET(lake_file_url)
  
  if (r$status_code == 200) {
    pdf_doc <- httr::content(r, as="raw")
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to fetch the", lake_item$metadata$political_party, "manifesto at URL", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
  # for tests: pdf_doc <- httr::content(httr::GET(data$results[[14]]$file), as="raw")
  if (!is.null(pdf_doc)) {
    PCCENPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\\.\\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_replace_all("\n", " ") |> # replace line breaks by spaces
      stringr::str_remove_all("\\d+ ([A-Z]+ )+") |>
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(PCCENPlatform) <- "paragraph"
    PCCENPlatform[PCCENPlatform == ""] <- NA
    PCCENPlatform <- na.omit(PCCENPlatform)
    PCCENPlatform$uppercase_first <- stringr::str_detect( # identify paragraphs
      PCCENPlatform$paragraph, "^[a-z]") == FALSE # starting with a lowercase
    PCCENPlatform$group <- NA
    PCCENPlatform$group[PCCENPlatform$uppercase_first == T] <- 1:length(
      PCCENPlatform$group[PCCENPlatform$uppercase_first == T])
    PCCENPlatform <- PCCENPlatform |>
      tidyr::fill(group, .direction = "down") |> # group paragraphs with the
      dplyr::group_by(group) |> # previous paragraph starting with uppercase
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "PCC",
                    election_year = 2021,
                    original_doc_type = "pdf",
                    release_date = as.Date("2021-08-16"),
                    title = "Canada's Recovery Plan",
                    country = "CAN",
                    language = "EN",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    
    index_column <- rep(1:nrow(PCCENPlatform))
    PCCENPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_can2021", 
                                        df = PCCENPlatform, 
                                        key_columns = "paragraph+political_party+index", 
                                        key_encoding = "digest",
                                        refresh_data = TRUE, 
                                        credentials = credentials)
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to parse the", lake_item$metadata$political_party, "manifesto document at", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
}


load_pccfr_manifesto <- function(lake_item) {
  
  pdf_doc <- NULL
  
  lake_file_url <- lake_item$file
  r <- httr::GET(lake_file_url)
  
  if (r$status_code == 200) {
    pdf_doc <- httr::content(r, as="raw")
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to fetch the", lake_item$metadata$political_party, "manifesto at URL", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
  # for tests: pdf_doc <- httr::content(httr::GET(data$results[[15]]$file), as="raw")
  if (!is.null(pdf_doc)) {
    PCCFRPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\\.\\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_replace_all("\n", " ") |> # replace line breaks by spaces
      stringr::str_remove_all("\\d+ ([A-Z]+ )+") |>
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(PCCFRPlatform) <- "paragraph"
    PCCFRPlatform[PCCFRPlatform == ""] <- NA
    PCCFRPlatform <- na.omit(PCCFRPlatform)
    PCCFRPlatform$uppercase_first <- stringr::str_detect( # identify paragraphs
      PCCFRPlatform$paragraph, "^[a-z]") == FALSE # starting with a lowercase
    PCCFRPlatform$group <- NA
    PCCFRPlatform$group[PCCFRPlatform$uppercase_first == T] <- 1:length(
      PCCFRPlatform$group[PCCFRPlatform$uppercase_first == T])
    PCCFRPlatform <- PCCFRPlatform |>
      tidyr::fill(group, .direction = "down") |> # group paragraphs with the
      dplyr::group_by(group) |> # previous paragraph starting with uppercase
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "PCC",
                    election_year = 2021,
                    original_doc_type = "pdf",
                    release_date = as.Date("2021-08-16"),
                    title = "Plan de rétablissement du Canada",
                    country = "CAN",
                    language = "FR",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    
    index_column <- rep(1:nrow(PCCFRPlatform))
    PCCFRPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_can2021", 
                                        df = PCCFRPlatform, 
                                        key_columns = "paragraph+political_party+index", 
                                        key_encoding = "digest",
                                        refresh_data = TRUE, 
                                        credentials = credentials)
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to parse the", lake_item$metadata$political_party, "manifesto document at", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
}


load_bq_manifesto <- function(lake_item) {
  
  pdf_doc <- NULL
  
  lake_file_url <- lake_item$file
  r <- httr::GET(lake_file_url)
  
  if (r$status_code == 200) {
    pdf_doc <- httr::content(r, as="raw")
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to fetch the", lake_item$metadata$political_party, "manifesto at URL", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
  # for tests: pdf_doc <- httr::content(httr::GET(data$results[[16]]$file), as="raw")
  if (!is.null(pdf_doc)) {
    BQPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\\.\\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_replace_all("\n", " ") |> # replace line breaks by spaces
      stringr::str_remove_all("\\d+ ([A-Z]+ )+") |>
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(BQPlatform) <- "paragraph"
    BQPlatform[BQPlatform == ""] <- NA
    BQPlatform <- na.omit(BQPlatform)
    BQPlatform$uppercase_first <- stringr::str_detect( # identify paragraphs
      BQPlatform$paragraph, "^[a-z]") == FALSE # starting with a lowercase
    BQPlatform$group <- NA
    BQPlatform$group[BQPlatform$uppercase_first == T] <- 1:length(
      BQPlatform$group[BQPlatform$uppercase_first == T])
    BQPlatform <- BQPlatform |>
      tidyr::fill(group, .direction = "down") |> # group paragraphs with the
      dplyr::group_by(group) |> # previous paragraph starting with uppercase
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "PCC",
                    election_year = 2021,
                    original_doc_type = "pdf",
                    release_date = as.Date("2021-08-16"),
                    title = "Canada's Recovery Plan",
                    country = "CAN",
                    language = "EN",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    
    index_column <- rep(1:nrow(BQPlatform))
    BQPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_can2021", 
                                        df = BQPlatform, 
                                        key_columns = "paragraph+political_party+index", 
                                        key_encoding = "digest",
                                        refresh_data = TRUE, 
                                        credentials = credentials)
  } else {
    clessnverse::logit(scriptname, 
                       paste("There was an error trying to parse the", lake_item$metadata$political_party, "manifesto document at", lake_file_url),
                       logger)
    status <<- 1
    return()
  }
}



###############################################################################
########################               Main              ######################
## This function is the core of your script. It can use global R objects and ##
## variables that you can define in the tryCatch section of the script below ##
###############################################################################

main <- function() {
  
  filter <- list(path = "party_manifestos/can2021")
  data <- hublot::filter_lake_items(credentials, filter = filter)
  
  for (i in 1:length(data$results)) {
    if (data$results[[i]]$metadata$political_party == "PLC" &
        data$results[[i]]$metadata$language == "EN" &
        data$results[[i]]$metadata$clean == "yes") load_plcen_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "PLC" &
        data$results[[i]]$metadata$language == "FR" &
        data$results[[i]]$metadata$clean == "yes") load_plcfr_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "PCC" &
        data$results[[i]]$metadata$language == "EN" &
        data$results[[i]]$metadata$clean == "yes") load_pccen_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "PCC" &
        data$results[[i]]$metadata$language == "FR" &
        data$results[[i]]$metadata$clean == "yes") load_pccfr_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "BQ" &
        data$results[[i]]$metadata$clean == "yes") load_bq_manifesto(data$results[[i]])
    #if (data$results[[i]]$metadata$political_party == "NPD" &
    #    data$results[[i]]$metadata$language == "EN" &
    #    data$results[[i]]$metadata$clean == "yes") load_npden_manifesto(data$results[[i]])
    #if (data$results[[i]]$metadata$political_party == "NPD" &
    #    data$results[[i]]$metadata$language == "FR" &
    #    data$results[[i]]$metadata$clean == "yes") load_npdfr_manifesto(data$results[[i]])
    #if (data$results[[i]]$metadata$political_party == "PVC" &
    #    data$results[[i]]$metadata$language == "EN" &
    #    data$results[[i]]$metadata$clean == "yes") load_pvcen_manifesto(data$results[[i]])
    #if (data$results[[i]]$metadata$political_party == "PVC" &
    #    data$results[[i]]$metadata$language == "FR" &
    #    data$results[[i]]$metadata$clean == "yes") load_pvcfr_manifesto(data$results[[i]])
    #if (data$results[[i]]$metadata$political_party == "PPC" &
    #    data$results[[i]]$metadata$language == "EN" &
    #    data$results[[i]]$metadata$clean == "yes") load_ppcen_manifesto(data$results[[i]])
    #if (data$results[[i]]$metadata$political_party == "PPC" &
    #    data$results[[i]]$metadata$language == "FR" &
    #    data$results[[i]]$metadata$clean == "yes") load_ppcfr_manifesto(data$results[[i]])
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
      if (!exists("scriptname")) scriptname <<- "l_party_manifestos_can2021"
      
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
