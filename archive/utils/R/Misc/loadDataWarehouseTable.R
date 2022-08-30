    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))

    table <- "clhub_tables_warehouse_political_parties_press_releases"

    hublot::count_table_items(table, credentials)

    page <- hublot::list_table_items(table, credentials)
    data <- list()

    repeat {
        data <- c(data, page$results)
        page <- hublot::list_next(page, credentials)
        if (is.null(page)) {
            break
        }
    }

    data_warehouse_table <- tidyjson::spread_all(data)
