###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                                l_leaders_debates                            #
#                                                                             #
# Script pour extraire les pages html (ou autre format - ex: xml) sur le web  #
# contenant les communiqués de presse des partis politiques du Québec         #
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################


calculate_key <- function(row = list(), dataframe, content_metadata) {
  
    key_columns <- content_metadata$metadata$key_columns
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
            key <-  if (grepl("^_row_number", key_columns[[1]][j])) {
                        paste(key, nrow(dataframe) + 1, sep="")
                    } else {
                    if (grepl("^_records_schema", key_columns[[1]][j])) {
                        paste(key, records_schema, sep="")
                    } else {
                    if (grepl("^_lake_item_key", key_columns[[1]][j]))  {
                        paste(key, content_metadata$key, sep="")
                    } else {
                        stop(paste("unsupported key column:", key_columns[[1]][j]))
                    }}}
        } else {
          #key <- paste(key, dataframe[[key_columns[[1]][j]]][i], sep="")
          key <- paste(key, row[[key_columns[[1]][j]]], sep="")
        }
        j <- j + 1
      }
    }

    if (content_metadata$metadata$key_encoding == "digest") key <- digest::digest(key)

    return(key)
}

load_content_to_warehouse <- function(tablename, tags, df_content, tags_metadata, content_metadata, refresh_data) {

    my_tablename <- gsub("warehouse_", "", tablename)
    table <- clessnverse::get_warehouse_table(my_tablename, credentials)

    columns <- unique(tags$conversion_column)
    for (i in 1:length(columns)) assign(columns[i], NA)

    dataframe <- data.frame()

    for (i in 1:nrow(df_content)) {
        current_tag <- df_content$tag[i]

        current_tag_index <- which(tags$tag == current_tag)

        if (length(current_tag_index) > 0) {
            current_tag_column <- tags$conversion_column[current_tag_index]
            current_tag_value <- tags$conversion_value[current_tag_index]

            assign(current_tag_column, current_tag_value) 

            row = list(current_tag_column = current_tag_value)
            names(row)[which(names(row) == "current_tag_column")] <- current_tag_column

            for (j in 1:length(columns)) {
                if (columns[j] != current_tag_column)
                row = c(row, list(new_column = get(columns[j])))
                names(row)[which(names(row) == "new_column")] <- columns[j]
            }

            if (nchar(trimws(df_content$text[i])) == 0) next
            list_text <- list(text = df_content$text[i])
            names(list_text) <- content_metadata$metadata$content_column_name

            row[row == "NA"] <- NA_character_
            row <- c(row, list_text)
            row <- c(
                row, 
                list(
                    source_key = content_metadata$key, 
                    source_description = content_metadata$metadata$description,
                    source_media = content_metadata$metadata$source_media,
                    source_date = content_metadata$metadata$source_date
                    )
                )

            key <- calculate_key(
                row = row,
                dataframe = df_content,
                content_metadata = content_metadata
            )

            dataframe <- dataframe %>% dplyr::bind_rows(data.frame(row,key)) 

            clessnverse::commit_warehouse_row(
                table = my_tablename, 
                key = key, 
                row = row, 
                refresh_data = refresh_data,
                credentials = credentials
            )

        } else {
            stop(paste("unknown tag", current_tag, "during processing the file with key", content_metadata$key, ":", content_metadata$metadata$description))
        }
    }

}


###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
  
    clessnverse::logit(scriptname,  "starting main function", logger)

    # get input lake item
    lakeitem_list  <- hublot::filter_lake_items(credentials, list(key=opt$input_lakeitem_key))
    lakeitem <- hublot::retrieve_lake_item(lakeitem_list$results[[1]]$id, credentials)
    lakeitem_content <- httr::GET(lakeitem$file)

    # get tags
    file <- hublot::retrieve_file(lakeitem$metadata$conversion_tags_file_slug, credentials)
    download.file(file$file, "tags.xlsx")
    df_tags <- readxl::read_xlsx("tags.xlsx")
    if (file.exists("tags.xlsx")) file.remove("tags.xlsx")

    # process lake item and load to hublot
    lakeitem_content <- httr::content(lakeitem_content, as = "text", encoding = "UTF-8")
    split_content <- strsplit(lakeitem_content, "#")
    df_content <- data.frame(text = split_content[[1]])
    df_content$text <- trimws(df_content$text)
    df_content$text <- stringr::str_replace(df_content$text, "^.", "\\0 ")
    df_cleancontent <- df_content %>% dplyr::filter(nchar(text) > 0)
    df <- df_cleancontent %>% tidyr::separate(text, c("tag", "text"), extra = "merge")
    df$tag <- paste("#", df$tag, sep = "")

    load_content_to_warehouse(
        tablename = lakeitem$metadata$destination,
        tags = df_tags,
        df_content = df,
        tags_metadata = file,
        content_metadata = lakeitem,
        refresh_data = opt$refresh_data
    )

    # close execution gracefully
    clessnverse::logit(scriptname, "ending main function"), logger)
    clessnverse::logit(scriptname, "write the results of your script here"), logger)

}




tryCatch( 
    withCallingHandlers(
    {
        # Package
        library(dplyr) 
        
        Sys.setlocale("LC_TIME", "fr_CA.utf8")

        # Globals : scriptname, opt, logger, credentials
        if (!exists("scriptname")) scriptname <<- "l_leaders_debates"

        opt <- list(
            log_output = c("file"), 
            input_lakeitem_key = "c82b0e16-7e29-4520-a4cb-debf7198a0e1", 
            download_data = FALSE, translate=FALSE, refresh_data=TRUE
            )

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
        cat("exit status: ", status, "\n")
        quit(status = status)
    }
)

