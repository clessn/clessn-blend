###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                                  e_factiva                                  
#                                                                             
#     This script will import all the rtf files info the Lake section        
#     of the CLESSN DataLake With metadata as follows                         
#
#     {
#        "tags": "factiva,press_articles",
#        "format": "rtf",
#        "source": "https://www.dropbox.com/home/AxelDery_M-ARancourt/FilesRTF/Factiva",
#        "country": "CAN",
#        "description": "Articles de presse",
#        "object_type": "raw_data",
#        "source_type": "factiva",
#        "content_type": "press_article",
#        "storage_class": "lake",
#        "political_party": "N/A",
#        "province_or_state": "N/A"
#     }
#
#
#                                                                             
###############################################################################

###############################################################################
########################       Package Requirements      ######################
###############################################################################
require(dplyr)
require("remotes")

if (!grepl("1.4", packageVersion("clessnverse"))) remotes::install_github("clessn/clessnverse", ref="v1")
remotes::install_github("kota7/striprtf")



###############################################################################
########################      Functions and Globals      ######################
###############################################################################

dbx_url <- "https://www.dropbox.com/home/AxelDery_M-ARancourt/FilesRTF/Factiva"
dbx_token <- Sys.getenv("DROPBOX_TOKEN")
safe_httr_GET <- purrr::safely(httr::GET)



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
  
    clessnverse::logit(scriptname, paste("Scraping the Factiva rtf files in dropbox", paste(dbx_url, collapse = '\n'), sep="\n"), logger)
  
    file_list <- clessnverse::dbxListDir("/AxelDery_M-ARancourt/FilesRTF/Factiva", dbx_token)

    #for (i in 1:length(file_list)) {
    for (i in 1:10) {
        clessnverse::logit(scriptname, paste("processing", file_list$objectName[i]), logger)

        successful_rtf_read <- clessnverse::dbxDownloadFile(
            paste(file_list$objectPath[i], file_list$objectName[i], sep="/"),
            "./",
            dbx_token
        )

        if (successful_rtf_read) {
            lake_item_metadata <- list(
                object_type = "raw_data",
                format = "rtf",
                content_type = "press_article",
                storage_class = "lake",
                source_type = "factiva",
                source = paste("https://www.dropbox.com/home/AxelDery_M-ARancourt/FilesRTF/Factiva", file_list$objectName[i], sep="/"),
                tags = "factiva,press_articles",
                description = "Articles de presse",
                political_party = "N/A",
                country = "CAN",
                province_or_state = "N/A"
            )

            lake_item_key <- gsub(".rtf","",file_list$objectName[i])
            lake_item_key <- gsub(" ", "", lake_item_key)
            lake_item_key <- gsub("\\((.*)\\)","_\\1", lake_item_key)

            lake_item_data <- list(
                key = lake_item_key,
                path = "press_articles/factiva",
                file = httr::upload_file(paste("./", file_list$objectName[i], sep=""))
            )

            existing_item <- hublot::filter_lake_items(credentials, list(key = lake_item_data$key))

            if (length(existing_item$results) == 0) {
                clessnverse::logit(scriptname, message = paste("creating new item", lake_item_data$key, "in data lake", lake_item_data$path), logger = logger)
                hublot::add_lake_item(
                body = list(
                    key = lake_item_data$key,
                    path = lake_item_data$path,
                    file = lake_item_data$file,
                    metadata = jsonlite::toJSON(lake_item_metadata, auto_unbox = T)),
                    credentials)
            } else {
                clessnverse::logit(scriptname, message = paste("updating existing item", lake_item_data$key, "in data lake", lake_item_data$path), logger = logger)
                hublot::update_lake_item(
                    id = existing_item$results[[1]]$id,
                    body = list(
                    key = lake_item_data$key,
                    path = lake_item_data$path,
                    file = lake_item_data$file,
                    metadata = jsonlite::toJSON(lake_item_metadata, auto_unbox = T)),
                    credentials)
            } #if (length(existing_item$results) == 0)

            if(file.exists(paste("./", file_list$objectName[i], sep=""))) file.remove(paste("./", file_list$objectName[i], sep="")) 

            total_files_scraped <<- total_files_scraped + 1
        } else {
            clessnverse::logit(scriptname, paste("could not read rtf file from dropbox", file_list$objectName[i]), logger)
            final_message <<- paste("could not read rtf file from dropbox", file_list$objectName[i])
        } #if (successful_rtf_read)
    } #for (i in 1:length(file_list))
}





tryCatch( 
    withCallingHandlers(
    {        
        if (!exists("scriptname")) scriptname <<- "e_factiva"

        opt <<- list(log_output = c("console"), hub_mode = "refresh")

        if (!exists("opt")) {
            opt <<- clessnverse::processCommandLineOptions()
        }

        if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
        
        # login to hublot
        clessnverse::logit(scriptname, "connecting to hublot (HUB3)", logger)

        credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))
        
        
        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

        status <<- 0
        final_message <<- ""
        total_files_scraped <<- 0
        
        main()
    },

    warning = function(w) {
        clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
        print(w)
        final_message <<- if (final_message == "") w else paste(final_message, "\n", w, sep="")    
        status <<- 2
    }),
    
    error = function(e) {
        clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
        print(e)
        final_message <<- if (final_message == "") e else paste(final_message, "\n", e, sep="")    
        status <<- 1
    },
  
    finally={
        clessnverse::logit(scriptname, final_message, logger)
        clessnverse::logit(scriptname, paste(total_files_scraped, "Factiva file were extracted from the web"), logger)
        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
        clessnverse::logclose(logger)
        rm(logger)
        print(paste("exiting with status", status))
        #quit(status = status)
    }
)


