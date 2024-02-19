# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

#Tu peux faire ça:
table_longname <- "clhub_tables_warehouse_agoraplus_canada_house_of_commons"

hublot::count_table_items(table_longname, credentials)

data_filter <-  list(
    data__event_date__gte="2019-01-01", 
    data__event_date__lte="2019-12-31"
  )


page <- hublot::filter_table_items(table_longname, credentials, data_filter)
data <- list()

repeat {
    data <- c(data, page$results)
    if (length(data_filter) == 0) {
        page <- hublot::list_next(page, credentials)
    } else {
        page <- hublot::filter_next(page, credentials)
    }
    if (is.null(page)) {
        break
    }
}
# Ensuite il faut que tu mettes cette liste dansd un dataframe


# Tu es peut être mieux de faire ça:
table_name <- "agoraplus_canada_house_of_commons"

df <- clessnverse::get_warehouse_table(
  table_name = table_name,
  credentials = credentials,
  data_filter = list(
    data__event_date__gte="2019-01-01", 
    data__event_date__lte="2019-12-31"
  ),
  nbrows = 0
)
