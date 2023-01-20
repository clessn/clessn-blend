###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                               e_qc125                                       #
#                                                                             #
#  This extractor scrapes the survey agregator of QC125 at https://qc125.com/ #
#  The output of this extractor is a csv with the electoral prediction for    #
#  each riding. The csv is added to lake/qc125                                #
#                                                                             #
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

get_qc125_df <- function(){
  clessnverse::logit(scriptname, "  Web scraping of qc125.com starting", logger)
  firstPage <- rvest::read_html("https://qc125.com/districts.htm") %>%
    rvest::html_nodes("a") %>%
    rvest::html_attr('href')
  ridings_ids <- 1001:1125
  urls <- stringr::str_trim(unique(grep(paste(as.character(ridings_ids),collapse="|"),
                               firstPage, value=TRUE)))
  for (i in 1:length(urls)) {
    circ <- rvest::read_html(urls[i]) %>%
      rvest::html_nodes("h1") %>%
      rvest::html_text()
    circ <- circ[1]
    projection <- rvest::read_html(urls[i]) %>%
      rvest::html_nodes("script:contains('var moyennes')")

    print(urls[i])
    print(projection)

    projection_txt <- xml2::xml_text(projection)

    rawNames <- qdapRegex::ex_between(projection_txt, "parties = [", ",]")[[1]]
    cleanNames <- gsub('^.|.$', '', strsplit(rawNames, ",")[[1]])

    rawMeans <- qdapRegex::ex_between(projection_txt, "moyennes = [", ",]")[[1]]
    cleanMeans <- strsplit(rawMeans, ",")[[1]]

    rawMoes <- qdapRegex::ex_between(projection_txt, "moes = [", ",]")[[1]]
    cleanMoes <- strsplit(rawMoes, ",")[[1]]
    if (i == 1) {
      DataQC125 <- data.frame(riding_id=ridings_ids[i],
                              riding_name=circ,
                              party=cleanNames,
                              pred=cleanMeans,
                              moes=cleanMoes)
    }
    else {
      QC125 <- data.frame(riding_id=ridings_ids[i],
                          riding_name=circ,
                          party=cleanNames,
                          pred=cleanMeans,
                          moes=cleanMoes)
      DataQC125 <- rbind(DataQC125,QC125)
    }
    clessnverse::logit(scriptname, paste0("     ", i, "/125 -- ", ridings_ids[i], " - ", circ, " data scraped"), logger)
  }
  output <- DataQC125 %>%
    dplyr::select(riding_id, riding_name, party, pred, moes) %>%
    dplyr::mutate(pred = as.numeric(pred)/100,
           moes = as.numeric(moes)/100,
           date = format(Sys.time(), "%Y-%m-%d")) %>%
    dplyr::select(date, riding_id, riding_name, party, pred, moes)
  return(output)
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
  df <- get_qc125_df()
  key <- paste0("qc125_", format(Sys.time(), "%Y-%m-%d"))
  path <- "qc125"
  write.csv2(df, "data.csv", row.names = F)
  lake_item_data <- list(key = key, path = path, file , item = "data.csv")
  lake_item_metadata <- list(
    object_type = "raw_data",
    format = "csvfile",
    content_type = "qc125_preds",
    storage_class = "lake",
    source_type = "website",
    source = "https://qc125.com/",
    tags = "elxn_qc2022, vitrine_democratique, polqc",
    description = paste0("Données de Qc125 en date du ", format(Sys.time(), "%Y-%m-%d")),
    country = "CAN",
    province_or_state = "QC"
  )
  clessnverse::commit_lake_item(
    data = lake_item_data,
    metadata = lake_item_metadata,
    mode = "refresh",
    credentials = credentials)
  clessnverse::logit(scriptname, paste0("qc125_", format(Sys.time(), "%Y-%m-%d"), " committed as a lake item to Hublot"), logger)
  return("Done")
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
    if (!exists("scriptname")) scriptname <<- "e_qc125"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    opt <- list(log_output = c("file"))

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

