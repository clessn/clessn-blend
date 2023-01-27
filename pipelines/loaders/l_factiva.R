###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                                  l_factiva                                  
#                                                                             
#     This script will import all the rtf files stored in the CLESSN DataLake
#     within the press_articles/factiva path and parse each rtf file in order
#     to store each article in its own row of the datawarehouse table
#
#                  clhub_tables_warehouse_factiva_press_articles                        
#
#     The table metadata is
#
#        {
#        "tags": "factiva,press_articles",
#        "type": "observations",
#        "format": "table",
#        "source": "press_articles/factiva",
#        "pillars": "media",
#        "projects": "n/a",
#        "description": "Articles de presse",
#        "source_type": "factiva",
#        "content_type": "press_article",
#        "storage_class": "warehouse"
#        }
#
#
#                                                                             
###############################################################################


            #if (doc_object$heading == "Editorial") offset <- 0 else offset <- 1 
            #doc_object$title <- trimws(rtf_document[[i+1]])
            #if (doc_object$heading != "Editorial") doc_object$author <- trimws(rtf_document[[i+3]])
            #doc_object$word_count <- trimws(rtf_document[[i+3+offset]])
            #doc_object$pubish_date <- trimws(rtf_document[[i+4+offset]])
            #doc_object$media_full_name <- trimws(rtf_document[[i+5+offset]])
            #doc_object$media_short_name <- trimws(rtf_document[[i+6+offset]])
            #doc_object$province_or_state <- trimws(rtf_document[[i+7+offset]])
            #doc_object$some_code <- trimws(rtf_document[[i+8+offset]])
            #doc_object$lang <- trimws(rtf_document[[i+9+offset]])
            #doc_object$copyright <- trimws(rtf_document[[i+10+offset]])
            #doc_object$title <- trimws(rtf_document[[i+1]])
            #if (doc_object$heading != "Editorial") doc_object$author <- trimws(rtf_document[[i+3]])
            #for (j in 1:10) {
            #    if (grepl("words", rtf_document[[i+j]], ignore.case = T)) doc_object$word_count <- trimws(rtf_document[[i+j]])
            #    if (grepl("english|french", rtf_document[[i+j]], ignore.case = T)) doc_object$lang <- trimws(rtf_document[[i+j]])
            #    if (grepl("january|february|march|april|may|june|july|august|september|october|november|december", rtf_document[[i+j]], ignore.case = T)) doc_object$pubish_date <- trimws(rtf_document[[i+j]])
            #    if (grepl("copyrighr", rtf_document[[i+j]], ignore.case = T)) doc_object$copyright <- trimws(rtf_document[[i+j]])
            #    if (grepl("words", rtf_document[[i+j]], ignore.case = T)) doc_object$word_count <- trimws(rtf_document[[i+j]])
            #    #if (grepl("words", rtf_document[[i+j]], ignore.case = T)) doc_object$word_count <- trimws(rtf_document[[i+j]])
            #    #if (grepl("words", rtf_document[[i+j]], ignore.case = T)) doc_object$word_count <- trimws(rtf_document[[i+j]])
            #    
            #}



###############################################################################
########################       Package Requirements      ######################
###############################################################################
require(dplyr)
require(gender)
require("remotes")

if (!grepl("1.4", packageVersion("clessnverse"))) remotes::install_github("clessn/clessnverse", ref="v1")
remotes::install_github("kota7/striprtf")


###############################################################################
########################      Functions and Globals      ######################
###############################################################################

datalake_path <- "press_articles/factiva"
safe_httr_GET <- purrr::safely(httr::GET)

patterns_provinces <- c(
    "^Ontario", "^ONT", "^ON", 
    "^Quebec", "^QUE", 
    "^British Columbia", "^BC", 
    "^Alberta", "^AB",
    "^Nova Scotia", "^NS",
    "^New Brunswick", "^NB",
    "^Manitoba", "^MB",
    "^Prince Edward Island", "^PEI",
    "^Saskatchewan","^SK",
    "^Newfoundland and Labrador", "^NL",
    "^Northwest Territories", "^NT",
    "^Yukon", "^YT",
    "^Nunavut", "^NU"
)

patterns_cities <- c(
    "^Toronto$", 
    "^Quebec City$",
    "^Montreal$",
    "^Vancouver$",
    "^Victoria$",
    "^Edmonton$",
    "^Calgary$",
    "^Moncton$",
    "^Fredericton$",
    "^Halifax$",
    "^Winnipeg$",
    "^Charlottetown$",
    "^Regina$",
    "^Saskatoon$",
    "^St. John's$",
    "^Yellowknife$",
    "^Whitehorse$",
    "^Iqaluit$"
)

patterns_media_full_names <- c(
    "^Toronto Star","^The Toronto Star",
    "^Vancouver Sun","^The Vancouver Sun",
    "^Montreal Gazette","^The Montreal Gazette",
    "^Le journal de Montreal",
    "^Globe and Mail","^The Globe and Mail",
    "^La Presse Canadienne",
    "^CTV National News",
    "^Canada AM",
    "^Postmedia News",
    "^Bloomberg News",
    "^Bloomberg",
    "^Financial Post"
)

patterns_media_short_names <- c(
    "^GMBN",
    "^GLOB",
    "^CTVN",
    "^CNAM",
    "^MTLG",
    "^VNCS",
    "^TOR"
)

patterns_dates <- c(
    "^[0-9]* January",
    "^[0-9]* February",
    "^[0-9]* March",
    "^[0-9]* April",
    "^[0-9]* May",
    "^[0-9]* June",
    "^[0-9]* July",
    "^[0-9]* August",
    "^[0-9]* September",
    "^[0-9]* Octobre",
    "^[0-9]* Novembre",
    "^[0-9]* Decembre"
)

patterns_word_cound <- c(
    "words"
)

patterns_lang <- c(
    "^English"
)

patterns_copyright <- c(
    "^Copyrights",
    "^Copyright",
    "^©",
    "^\\(c\\)"
)

patterns_authors <- c(
    "Associated Press",
    "^by\\s(.*)",
    "^By\\s(.*)"
)



process_rtf_document <- function(rtf_document) {
    doc_list <- list()
    doc_index <- 1

    i <- 1
    sod <- FALSE
    eod <- TRUE

    doc_object <- list(
            heading = NA_character_,
            title = NA_character_,
            author = NA_character_,
            word_count = NA_character_,
            pubish_date = NA_character_,
            media_full_name = NA_character_,
            media_short_name = NA_character_,
            province_or_state = NA_character_,
            city = NA_character_,
            lang = NA_character_,
            copyright = NA_character_,
            doc_id = NA_character_,
            lost_data = NA_character_,
            body = NA_character_
        )

    while (i < length(rtf_document) && rtf_document[i] != "Search Summary") {
        
        #blank
        if (nchar(trimws(rtf_document[[i]])) == 0) {
            #loop
            i <- i + 1
            next
        }

        if (sod == FALSE && eod == TRUE && nchar(trimws(rtf_document[[i]])) > 0) {
            sod <- TRUE
            eod <- FALSE
            doc_object$heading <- trimws(rtf_document[[i]])
        }

        #start of document
        if (sod == TRUE) {
            body_index <- 0

            for (j in 0:11) {

                if (j==0) {
                    doc_object$heading <- rtf_document[i+j]
                    next
                }

                if (j==1) {
                    doc_object$title <- rtf_document[i+j]
                    next
                }

                if (nchar(trimws(rtf_document[[i+j]])) == 0) {
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_cities)) {
                    doc_object$city <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_dates)) {
                    doc_object$publish_date <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_lang)) {
                    doc_object$lang <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_media_full_names)) {
                    doc_object$media_full_name <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_media_short_names)) {
                    doc_object$media_short_name <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_provinces)) {
                    doc_object$province_or_state <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_word_cound)) {
                    doc_object$word_count <- trimws(rtf_document[i+j])
                    next
                }

                if (TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_copyright)) {
                    doc_object$copyright <- trimws(rtf_document[i+j])
                    body_index <- j + 1
                    break
                }

                if (nrow(gender::gender(strsplit(rtf_document[i+j], " ")[[1]][1])) > 0 || TRUE %in% stringr::str_detect(rtf_document[[i+j]], patterns_authors)) {
                    doc_object$author <- trimws(rtf_document[i+j])
                    next
                }

                if (is.na(doc_object$lost_data)) {
                    doc_object$lost_data <- rtf_document[i+j]
                } else {
                    doc_object$lost_data <- paste(doc_object$lost_data, "|", rtf_document[i+j])
                }
            }

            #loop
            i <- i + body_index
            sod <- FALSE
            next
        }#if (sod == TRUE)

        #Document XXXXXXXXXXXXXXXXXXXXXXXXX - end of document
        eod_match = stringr::str_match(rtf_document[[i]], "(^Document)(\\s)([0-9A-Za-z]{25})")
        if (!is.null(eod_match[4]) && !is.na(eod_match[4]) && sod == FALSE && eod == FALSE) {
            doc_object$doc_id <- trimws(eod_match[4])
            i <- i + 1

            #write the document to the warehouse table
            doc_list[[doc_index]] <- doc_object
            clessnverse::logit(scriptname, paste("committing article", doc_object$title), logger)

            #check if item exists
            check_filter <- hublot::filter_table_items('clhub_tables_warehouse_factiva_press_articles', credentials, list(key=digest::digest(doc_object$title)))

            # if item doesn't exists
            if (length(check_filter$results) == 0) {
                #add it
                hublot::add_table_item(
                    table='clhub_tables_warehouse_factiva_press_articles',
                    body=list(
                        key = digest::digest(doc_object$title),
                        timestamp = Sys.time(),
                        data = doc_object
                    ),
                   credentials=credentials
                )
            } else {
                #update it
                hublot::update_table_item(
                   'clhub_tables_warehouse_factiva_press_articles',
                   check_filter$results[[1]]$id,
                   list(
                       key = digest::digest(doc_object$title),
                       timestamp = as.character(Sys.time()),
                       data = jsonlite::toJSON(
                           doc_object, 
                           auto_unbox = T 
                       )
                   ),
                   credentials
                )
            }

            #reset state
            sod <- FALSE
            eod <- TRUE
            doc_object <- list(
                heading = NA_character_,
                title = NA_character_,
                author = NA_character_,
                word_count = NA_character_,
                pubish_date = NA_character_,
                media_full_name = NA_character_,
                media_short_name = NA_character_,
                province_or_state = NA_character_,
                city = NA_character_,
                lang = NA_character_,
                copyright = NA_character_,
                doc_id = NA_character_,
                lost_data = NA_character_,
                body = NA_character_
            )

            #loop
            doc_index <- doc_index + 1
            i <- i + 1
            next
        }#if (!is.null(eod_match[4]) && !is.na(eod_match[4]) && sod == FALSE && eod == FALSE)

        #body or start of newdoc
        if (sod == FALSE & eod == FALSE) {
            if (is.na(doc_object$body)) {
                doc_object$body <- trimws(rtf_document[[i]])
            } else {
                doc_object$body <- paste(doc_object$body, trimws(rtf_document[[i]]), sep="\n\n")
            }
            i <- i + 1
        }#if (sod == FALSE & eod == FALSE)
    }

    total_articles_scraped <<- total_articles_scraped + doc_index

    return(doc_list)
}


###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
  
    clessnverse::logit(scriptname, paste("Scraping the Factiva rtf files in the CLESSN datalake path", datalake_path, sep=" "), logger)
  
    my_filter = list(path=datalake_path)
    lake_item_list <- hublot::filter_lake_items(credentials, my_filter)

    for (i in 1:length(lake_item_list$results)) {
        clessnverse::logit(scriptname, paste("processing lake itsm with key", lake_item_list$results[[i]]$key), logger)

        lake_item <- hublot::retrieve_lake_item(lake_item_list$results[[i]]$id, credentials)
        lake_item_file <- lake_item$file

        clessnverse::logit(scriptname, paste("converting rtf file", lake_item$key) , logger)
        rtf_document <- striprtf::read_rtf(lake_item_file)
        clessnverse::logit(scriptname, paste(lake_item$key, "converted") , logger)

        clessnverse::logit(scriptname, paste("processing", lake_item$key) , logger)        
        docs <<- process_rtf_document(rtf_document)
        clessnverse::logit(scriptname, paste(lake_item$key, "processed") , logger)    

        total_lakeitems_scraped <<- total_lakeitems_scraped + 1 

    } #for (i in 1:length(lake_items))
}





tryCatch( 
    withCallingHandlers(
    {        
        if (!exists("scriptname")) scriptname <<- "l_factiva"

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
        total_lakeitems_scraped <<- 0
        total_articles_scraped <<- 0
        
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
        clessnverse::logit(scriptname, paste(total_lakeitems_scraped, "Factiva datalake items were extracted from hublot and stored in clhub_tables_warehouse_factiva_press_articles"), logger)
        clessnverse::logit(scriptname, paste(total_articles_scraped, "Factiva articles were extracted from hublot and stored in clhub_tables_warehouse_factiva_press_articles"), logger)
        clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
        clessnverse::logclose(logger)
        rm(logger)
        print(paste("exiting with status", status))
        #quit(status = status)
    }
)


