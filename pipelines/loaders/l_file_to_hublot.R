###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                              l_file_to_hublot                               #
#                                                                             #
# This script takles a csv or xlsx file from hublot and loads it to hub2.0    #
# In the appropriate table.   Currently supported tables are                  #
#                                                                             #
#  - people                                                                   #
#                                                                             #
# The table targeted for loading the data depends on the 'content_type' meta  #
# data value of the file in hublot.                                           #
#                                                                             #
# So current supported content types are                                      #
#                                                                             #
#  - people                                                                   #
#                                                                             #
# The input file must be stored in hublot in the Files blob storage and must  #
# be named import_<content_type>_<records_type>                               #
#                                                                             #
# supported records_type are                                                  #
#                                                                             #
#  - mp                                                                       #
#  - candidate                                                                #
#  - journalist                                                               #
#  - media                                                                    #
#  - partner                                                                  #
#  - political_party                                                          #
#  - political_staff                                                          #
#  - public_service                                                           #
#                                                                             #
# The file name must be passed as first paramets of the script when it runs   #
# automated                                                                   #
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


load_df_to_hub2.0 <- function(df, content_type, records_type, records_schema, key_encoding, key_columns, refresh_data) {

  if (is.null(df) || is.na(df) || nrow(df) == 0) {
    clessnverse::logit(scriptname, "invalid df given to load_df_to_hub2.0")
    return(1)
  }

  metadata_colnames <- colnames(df)[which(grepl("^metadata.", colnames(df)))]
  data_colnames <- colnames(df)[which(grepl("^data.", colnames(df)))]

  key_columns_mode <- "one"

  if (stringr::str_detect(key_columns, ",")) {
    key_columns <- gsub(" ", "", key_columns)
    key_columns <- strsplit(key_columns, ",")
    key_columns_mode <- "one"
  }

  if (stringr::str_detect(key_columns, "\\+")) {
    key_columns <- gsub(" ", "", key_columns)
    key_columns <- strsplit(key_columns, "\\+")
    key_columns_mode <- "combined"
  }

  if (content_type == "people") content_type <- "persons"

  my_list <- list()

  clessnverse::logit(scriptname, paste("about to add", nrow(df), "persons to hub2.0 with key_columns_mode = ", key_columns_mode), logger)

  for (i in 1:nrow(df)) {
    df_row <- df[i,]

    #Structure the metadata
    metadata <- as.list(
      df_row[c(metadata_colnames)]
    )
    names(metadata) <- gsub("^metadata.", "", names(metadata))
    metadata[metadata==""] <- NA
    metadata[metadata=="NA"] <- NA

    # Structure the data
    data <- as.list(
      df_row[c(data_colnames)]
    )
    names(data) <- gsub("^data.", "", names(data))
    data[data==""] <- NA
    data[data=="NA"] <- NA


    # Compute the key
    if (key_columns_mode == "one") {
      key <- df[[key_columns[[1]][1]]][i]

      j <- j

      while (is.null(key) || is.na(key) || length(key) == 0) {
            key <- df[[key_columns[[1]][j]]][i]
            j <- j + 1
      }
    }

    if (key_columns_mode == "combined") {
      j <- 1
      key <- ""

      while (j <= length(key_columns[[1]])) {
        if (grepl("^_", key_columns[[1]][j])) {
          if (grepl("^_records_schema", key_columns[[1]][j])) {
            key <- paste(key, records_schema, sep="")
          } else {
            stop(paste("unsupported key column:", key_columns[[1]][j]))
          }
        } else {
          key <- paste(key, df[[key_columns[[1]][j]]][i], sep="")
        }
        j <- j + 1
      }
    }

    if (is.null(key) || is.na(key) || length(key) == 0) {
          msg <- paste("Warning : the key for an entry of the import file could not be computed.  Row of the file:", 
                       i, 
                       "date is:",
                       paste(data, collapse = ' ')
                      )
          clessnverse::logit(scriptname, msg, logger)
          warning(msg)
         rm(msg)
         next
    }

    if (key_encoding == "digest") {
      key <- digest::digest(key)
    }

    # Commit to hub2
    tryCatch(
      {
        clessnverse::logit(scriptname, paste("about to add item to hub2.0: key=", key), logger)

        new_item_metadata <- metadata
        new_item_data <- data
        new_item_metadata$twitterAccountHasBeenScraped <- 0

        clessnhub::create_item(table = content_type, key = key, type = records_type, schema = records_schema, 
                              metadata = new_item_metadata, 
                              data = new_item_data
                              )
      },
      error= function(e) {
        if (refresh_data) {
          clessnverse::logit(scriptname, paste("modifying item in hub2.0: key=", key, "because it already exists"), logger)
          item <- clessnhub::get_item(table = content_type, key = key)

          # Here we must merge the data and metadata
          existing_item_metadata <- rlist::list.merge(item$metadata, metadata)
          existing_item_data <- rlist::list.merge(item$data, data)

          clessnhub::edit_item(table = content_type, key = item$key, type = records_type, schema = records_schema, 
                               metadata = existing_item_metadata, 
                               data = existing_item_data
                              )
        } else {
          clessnverse::logit(scriptname, paste("item with key=", key, "already exists.  Skipping.  Actual error is"), logger)
          clessnverse::logit(scriptname, e, logger)
        }  
      },

      finally={

      }
    )
  }

  return(0)

}

commit_warehouse_table <- function(table_name, df, key_columns, key_encoding, refresh_data, credentials) {
  my_table <- paste("clhub_tables_warehouse_", table_name, sep = "")

  # Check if table exists
  table_check <- hublot::filter_tables(credentials, list(verbose_name = paste("warehouse_",table_name,sep="")))
  if (length(table_check$results) == 0) stop(paste("The warehouse table specified in clessnverse::commit_warehouse_table() does not exist:", table_name))

  if (stringr::str_detect(key_columns, ",")) {
    key_columns <- gsub(" ", "", key_columns)
    key_columns <- strsplit(key_columns, ",")
    key_columns_mode <- "one"
  }

  if (stringr::str_detect(key_columns, "\\+")) {
    key_columns <- gsub(" ", "", key_columns)
    key_columns <- strsplit(key_columns, "\\+")
    key_columns_mode <- "combined"
  }

  # Compute the key
  key <- NULL
  i <- 1
  if (key_columns_mode == "one") {
    while (NA %in% df[[key_columns[[1]][i]]] || "" %in% df[[key_columns[[1]][i]]]) {
      i <- i + 1
    }

    if (i <= length(key_columns[[1]])) {
      key <- df[[key_columns[[1]][i]]]
    } else {
      stop("none of the key_columns specified is totally filled with non NA or non empty values: ", paste(key_columns[[1]], collapse = " "))
    }
  }

  if (key_columns_mode == "combined") {
    key <- rep("", 1, nrow(df))
    for (i in 1:length(key_columns[[1]])) {
      if (is.null(df[[key_columns[[1]][i]]])) {
        stop(paste("column",  key_columns[[1]][i], "does not exist in dataframe in function clessnverse::commit_warehouse_table()"))
      }

      if (NA %in% df[[key_columns[[1]][i]]] || "" %in% df[[key_columns[[1]][i]]]) {
        stop(paste("column",  key_columns[[1]][i], "contains NA or empty values and cannot be a key in function commit_warehouse_table "))
      }

      key <- paste(key, df[[key_columns[[1]][i]]], sep = "")
    }
  }

  if (key_encoding == "digest") key <- unlist(lapply(X = as.list(key), FUN = digest::digest))

  # check if there are keys that already exist in the table directly on hublot
  if (refresh_data) {

  }

  if (TRUE %in% duplicated(key)) {
    stop(
      paste(
        "\nThere are duplicated values in the unique key composed of the key_columns passed to clessnverse::commit_warehouse_table():",
        paste(key_columns, collapse = ","),
        "\nThe positions of duplicated keys in the dataframe are",
        paste(which(duplicated(key) == TRUE), collapse = ",")
      )
    )
  }

  timestamp <- rep(as.character(Sys.time()), 1, nrow(df))

  data <- purrr::pmap(df, ~as.list(list(...)))

  new_df <- df %>%
    mutate(key = key, timestamp = timestamp, data=data) %>%
    select(key, timestamp, data)

  my_list <- do.call("mapply", c(list, new_df, SIMPLIFY = FALSE, USE.NAMES=FALSE))

  ret <- hublot::batch_create_table_items(my_table, my_list, credentials)

  return(c(created=ret$created, errors=ret$errors))
}


load_df_to_hublot <- function(df, content_type, key_encoding, key_columns, refresh_data) {

  my_table <- "test_batch_load"

  #ret <- clessnverse::commit_warehouse_table(
  ret <- commit_warehouse_table(
    table_name = my_table,
    df = df,
    key_columns = key_columns,
    key_encoding = key_encoding,
    refresh_data = refresh_data,
    credentials = credentials
  )

  if (is.null(ret$created)) ret$created <- 0
  if (is.null(ret$errors)) ret$errors <- 0

  clessnverse::logit(scriptname, paste("added", ret$created, "records to", my_table, "and got", ret$errors, "errors"), logger)

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
    ret <- 0

    valid_content_types <- list(
      people = c("candidate", "mp", "journalist", "political_staff", "public_service"), 
      medias = c("journal, tv"), 
      institutions = c("parliament", "government", "public_health"), 
      partners = c("collaborateur", "partenaire_universitaire", "fournisseur"), 
      political_parties = c("provincial", "federal", "europeean", "national"))
    
    ###########################################################################
    # Start your main script here using the best practices in activity logging
    #
    # Ex: warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0)
    #     clessnverse::logit(scriptname, "Getting political parties press releases from the datalake", logger) 
    #     lakes_items_list <- get_lake_press_releases(parties_list)
    #      

    # List the import files to import from the file blob storage
    files_filter <- list(
      tags = "dimension_import_file",
      metadata__storage_class="files",
      metadata__imported = FALSE
    )
      
    file_list <- hublot::filter_files(credentials, files_filter)

    if (length(file_list$results) == 0) {
      clessnverse::logit(scriptname, "no file found for import.  exitting script normally", logger)
      quit(status)
    }

    for (file in file_list$results) {
      clessnverse::logit(scriptname, paste("importing", file$name, "containing", file$metadata$content_type, "of type", file$metadata$records_type), logger)

      if ( !(file$metadata$destination %in% c("hub2.0", "hublot")) ) {
        clessnverse::logit(scriptname, paste("bad destination in file metadata:", file$metadata$destination, "for file", file$name), logger)
        status <<- 2
        next
      } 

      if (is.null(file$metadata$format) || !(file$metadata$format %in%  c("csv","xlsx"))) {
        clessnverse::logit(scriptname, paste("invalid file format for", file$name), logger)
        status <<- 2
        next
      }

      if (is.null(file$metadata$format) || 
          !(file$metadata$content_type %in% names(valid_content_types)) &&
          !(file$metadata$records_type %in% valid_people_records_types)) {
        clessnverse::logit(scriptname, paste("invalid content_type", file$metadata$content_type, "for", file$name), logger)
        status <<- 2
        next
      }

      if (file$metadata$format == "csv") {
        header_line <- readLines(file$file, n = 1)
        numfields <- count.fields(textConnection(header_line), sep = ";")
        df <- if (numfields == 1) read.csv(file$file, encoding="UTF-8") else read.csv2(file$file, encoding="UTF-8")
        #df <- if (numfields == 1) read.csv(file$file) else read.csv2(file$file)
        if ("X" %in% names(df)) df$X <- NULL
      }

      if (file$metadata$format == "xlsx") {
        df <- openxlsx::read.xlsx(file$file)
      }

      if (file$metadata$destination == "hub2.0") {
        ret <- load_df_to_hub2.0(
          df = df,
          content_type        = file$metadata$content_type,
          records_type        = file$metadata$records_type,
          records_schema      = file$metadata$records_schema,
          key_encoding        = file$metadata$key_encoding,
          key_columns         = file$metadata$key_columns,
          refresh_data        = file$metadata$refresh
        )

        file$metadata$imported <- TRUE
        # Must update the file here
        # NOT IMPLEMENTED IN HUBLOT YET:
        # hublot::update_file(file$slug, file, credentials)

        return(ret)
      }

      if (file$metadata$destination == "hublot") {
        ret <- load_df_to_hublot(
          df = df,
          content_type   = file$metadata$content_type,
          key_encoding   = file$metadata$key_encoding,
          key_columns    = file$metadata$key_columns,
          refresh_data   = file$metadata$refresh
        )

        return(ret)
      }

    } #for (file in file_list$results)

    return(ret)
}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch( 
  {
    ###########################################################################
    # Package dplyr for the %>% 
    # All other packages must be invoked by specifying its name
    # in front ot the function to be called
    library(dplyr) 

    ###########################################################################
    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to 
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #

    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"
    

    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally") 
    if (!exists("scriptname")) scriptname <<- "l_file_to_hublot"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    opt <- list(
        log_output = c("file")
        )

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hublot", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))
    
    # Connecting to hub 2.0
    #clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))

    clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    status <<- 0

    # Call main script    
    status <<- main()
  },

  warning = function(w) {
      clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
      print(w)
      status <<- 2
  },
  
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

    #quit(status=status)
  }
)

