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

load_qs_manifesto <- function(lake_item) {
  
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
  
  if (!is.null(pdf_doc)) {
    QSPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\n\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(QSPlatform) <- "paragraph"
    QSPlatform[QSPlatform == ""] <- NA
    QSPlatform <- na.omit(QSPlatform)
    QSPlatform <- data.frame(paragraph = QSPlatform$paragraph[ # remove
      stringr::str_detect(QSPlatform$paragraph, # footer
                          "PLATEFORME ÉLECTORALE 2022") == F])
    QSPlatform <- data.frame(paragraph = QSPlatform$paragraph[
      stringr::str_detect(QSPlatform$paragraph, "^\\d+$") == F])
    QSPlatform <- data.frame(paragraph = QSPlatform$paragraph[
      stringr::str_detect(QSPlatform$paragraph, "^GABRIEL") == F])
    QSPlatform <- data.frame(paragraph = QSPlatform$paragraph[
      stringr::str_detect(QSPlatform$paragraph, "^MANON") == F])
    QSPlatform$dot_end <- stringr::str_detect( # identify paragraphs ending with a
      QSPlatform$paragraph, "\\.$") # period
    QSPlatform$group <- NA
    QSPlatform$group[QSPlatform$dot_end == T] <- 1:length(
      QSPlatform$group[QSPlatform$dot_end == T])
    QSPlatform <- QSPlatform |>
      tidyr::fill(group, .direction = "up") |> # group paragraphs with the next
      dplyr::group_by(group) |> # paragraph ending with a period
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "qs",
                    election_year = 2022,
                    original_doc_type = "pdf",
                    release_date = as.Date("2022-08-19"),
                    title = "Changer d'ère",
                    country = "CAN",
                    province_or_state = "QC",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    
    index_column <- rep(1:nrow(QSPlatform))
    QSPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_qc2022", 
                                        df = QSPlatform, 
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


load_caq_manifesto <- function(lake_item) {
  clessnverse::logit(scriptname, 
                     "Function load_caq_manifesto not implemented yet!",
                     logger)
  status <<- 2
  return()
}


load_pcq_manifesto <- function(lake_item) {
  
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
  
  if (!is.null(pdf_doc)) {
    PCQPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\n\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    colnames(PCQPlatform) <- "paragraph"
    PCQPlatform[PCQPlatform == ""] <- NA
    PCQPlatform <- na.omit(PCQPlatform)
    PCQPlatform <- data.frame(paragraph = PCQPlatform$paragraph[ # remove
      stringr::str_detect(PCQPlatform$paragraph, # footer
                          "PLATEFORME DU PARTI CONSERVATEUR DU QUÉBEC") == F])
    PCQPlatform$dot_end <- stringr::str_detect( # identify paragraphs ending with a
      PCQPlatform$paragraph, "\\.$") # period
    PCQPlatform$group <- NA
    PCQPlatform$group[PCQPlatform$dot_end == T] <- 1:length(
      PCQPlatform$group[PCQPlatform$dot_end == T])
    PCQPlatform <- PCQPlatform |>
      tidyr::fill(group, .direction = "up") |> # group paragraphs with the next
      dplyr::group_by(group) |> # paragraph ending with a period
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "pcq",
                    election_year = 2022,
                    original_doc_type = "pdf",
                    release_date = as.Date("2022-08-14"),
                    title = "Liberté 22",
                    country = "CAN",
                    province_or_state = "QC",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    index_column <- rep(1:nrow(PCQPlatform))
    PCQPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_qc2022", 
                                        df = PCQPlatform, 
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


load_plq_manifesto <- function(lake_item) {
  
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
  
  if (!is.null(pdf_doc)) {
    PLQPlatform <-
      pdf_doc |>
      pdftools::pdf_text() |> # transform PDF into text
      stringr::str_split("\n\n") |> # unmerge separate paragraphs
      unlist() |> # transform list into one big vector
      stringr::str_squish() |> # remove white spaces
      data.frame() # transform into data frame
    
    colnames(PLQPlatform) <- "paragraph"
    
    PLQPlatform[PLQPlatform == ""] <- NA
    PLQPlatform <- na.omit(PLQPlatform)
    PLQPlatform <- data.frame(paragraph = PLQPlatform$paragraph[ # remove
      stringr::str_detect(PLQPlatform$paragraph, "LE LIVRE LIBÉRAL") == F]) # footer
    
    PLQPlatform$dot_end <- stringr::str_detect( # identify paragraphs ending with a
      PLQPlatform$paragraph, "\\.$") # period
    
    PLQPlatform$group <- NA
    PLQPlatform$group[PLQPlatform$dot_end == T] <- 1:length(
      PLQPlatform$group[PLQPlatform$dot_end == T])
    
    PLQPlatform <- PLQPlatform |>
      tidyr::fill(group, .direction = "up") |> # group paragraphs with the next
      dplyr::group_by(group) |> # paragraph ending with a period
      dplyr::summarise(paragraph = paste(paragraph, collapse = " ")) |>
      dplyr::select(-group) |>
      dplyr::mutate(political_party = "plq",
                    election_year = 2022,
                    original_doc_type = "pdf",
                    release_date = as.Date("2022-06-11"),
                    title = "Le livre libéral",
                    country = "CAN",
                    province_or_state = "QC",
                    n_words = stringr::str_count(paragraph, "\\S+"),
                    n_sentences = 1 + stringr::str_count(paragraph, "\\.\\s"))
    
    index_column <- rep(1:nrow(PLQPlatform))
    PLQPlatform$index <- index_column
    
    clessnverse::commit_warehouse_table(table_name = "political_parties_manifestos_qc2022", 
                                        df = PLQPlatform, 
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


load_pq_manifesto <- function(lake_item) {
  clessnverse::logit(scriptname, 
                     "Function load_pq_manifesto not implemented yet!",
                     logger)
  status <<- 2
  return()
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
  
  filter <- list(path = "party_manifestos/qc2022")
  data <- hublot::filter_lake_items(credentials, filter = filter)
  
  for (i in 1:length(data$results)) {
    if (data$results[[i]]$metadata$political_party == "QS") load_qs_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "CAQ") load_caq_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "PCQ") load_pcq_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "PLQ") load_plq_manifesto(data$results[[i]])
    if (data$results[[i]]$metadata$political_party == "PQ") load_pq_manifesto(data$results[[i]])
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
      if (!exists("scriptname")) scriptname <<- "l_party_manifestos_qc2022"
      
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
