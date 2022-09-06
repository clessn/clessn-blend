###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                              l_file_to_hub2.0                               #
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


batch_load <- function(table_name, df, key_column, key_encoding, mode, credentials) {

}



load_df_to_hub2.0 <- function(df, content_type, records_type, records_schema, key_encoding, key_column, non_null_constraint, refresh_data) {

  if (is.null(df) || is.na(df) || nrow(df) == 0) {
    clessnverse::logit(scriptname, "invalid df given to load_df_to_hub2.0")
    return(1)
  }

  metadata_colnames <- colnames(df)[which(grepl("^metadata.", colnames(df)))]
  data_colnames <- colnames(df)[which(grepl("^data.", colnames(df)))]

  key_column_mode <- "one"

  if (stringr::str_detect(key_column, ",")) {
    key_column <- gsub(" ", "", key_column)
    key_column <- strsplit(key_column, ",")
    key_column_mode <- "one"
  }

  if (stringr::str_detect(key_column, "\\+")) {
    key_column <- gsub(" ", "", key_column)
    key_column <- strsplit(key_column, "\\+")
    key_column_mode <- "combined"
  }


  if (content_type == "people") content_type <- "persons"

  for (i in 1:nrow(df)) {
    df_row <- df[i,]

    if (is.null(df[[non_null_constraint[[1]][1]]][i]) || is.na(df[[non_null_constraint[[1]][1]]][i]) || length(df[[non_null_constraint[[1]][1]]][i]) == 0 || nchar(df[[non_null_constraint[[1]][1]]][i]) == 0) {
      clessnverse::logit(scriptname, paste("Non null constraint not met on record", i, paste(df[i,], collapse=" ")), logger)
      next
    }

    metadata <- as.list(
      df_row[c(metadata_colnames)]
    )
    names(metadata) <- gsub("^metadata.", "", names(metadata))
    metadata[metadata==""] <- NA

    data <- as.list(
      df_row[c(data_colnames)]
    )
    names(data) <- gsub("^data.", "", names(data))
    data[data==""] <- NA

    if (key_column_mode == "one") {
      key <- df[[key_column[[1]][1]]][i]

      if (is.null(key) || is.na(key) || length(key) == 0) {
            key <- df[[key_column[[1]][2]]][i]
      }
    }

    if (key_column_mode == "combined") {
      key <- paste(df[[key_column[[1]][1]]][i], df[[key_column[[1]][2]]][i], sep="")

      if (is.null(key) || is.na(key) || length(key) == 0) {
            key <- df[[key_column[[1]][2]]][i]
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

    tryCatch(
      {
        clessnverse::logit(scriptname, paste("about to add item to hub2.0: key=", key), logger)
        metadata$twitterAccountHasBeenScraped <- 0

        clessnhub::create_item(table = content_type, key = key, type = records_type, schema = records_schema, 
                              metadata = metadata, 
                              data = data
                              )
      },
      error= function(e) {
        if (refresh_data) {
          clessnverse::logit(scriptname, paste("modifying item in hub2.0: key=", key, "because it already exists"), logger)
          clessnhub::edit_item(table = content_type, key = key, type = records_type, schema = records_schema, 
                               metadata = metadata, 
                               data = data
                              )
        } else {
          clessnverse::logit(scriptname, paste("item with key=", key, "probably already exists.  Actual error is"), logger)
          clessnverse::logit(scriptname, e, logger)
        }  
      },

      finally={

      }
    )
  }

  return(0)

}





load_df_to_hublot <- function(df, content_type, records_type, key_encoding, key_column) {

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
          key_column          = file$metadata$key_column,
          non_null_constraint = file$metadata$non_null_constraint,
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
          records_type   = file$metadata$records_type,
          records_schema = file$metadata$records_schema,
          key_encoding   = file$metadata$key_encoding,
          key_column     = file$metadata$key_column,
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
        dataframe_mode = "refresh", hub_mode = "refresh",  
        log_output = c("file"), download_data = FALSE, translate=FALSE
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
    clessnverse::logclose(logger)

    # Cleanup
    closeAllConnections()
    rm(logger)

    quit(status=status)
  }
)

