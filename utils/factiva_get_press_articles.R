library(dplyr)
library(foreach)

replace_null <- function(x) {
  x <- purrr::map(x, ~ replace(.x, is.null(.x), NA_character_))
  purrr::map(x, ~ if(is.list(.x)) replace_null(.x) else .x)
}

scriptname <- "get_factiva_press_articles"
opt <- list(log_output = c("console"))
logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

#clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
clessnverse::logit(scriptname, "connecting to hub", logger)

credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))


page <- hublot::list_table_items('clhub_tables_warehouse_factiva_press_articles', credentials)
data <- list()

repeat {
    data <- c(data, page$results)
    page <- hublot::list_next(page, credentials)
    if (is.null(page)) {
        break
    }
}

data1 <- replace_null(data)

df <- data.frame(t(sapply(data1,c)))
df_data <-  data.frame(t(sapply(df$data,c)))


# Check if the structure is even or uneven
if (length(unique(sapply(df$data, length))) == 1) {
    # This is very fast on large dataframes but only works on even data schemas
    df$data <- NULL
    names(df) <- paste("hub.",names(df),sep="")
    df <- as.data.frame(cbind(df,df_data))
    #df <- df %>% replace(.data == "NULL", NA)
    for (col in names(df)) df[,col] <- unlist(df[,col])
} else {
    # This is slower on larg data sets but works on uneven data schemas
    element <- list()

    data <- data[lapply(data, class) == "list"]

    df <- foreach::foreach(element = data, .combine = bind_rows, .errorhandling = 'remove') %do% {
        df = unlist(element);
        df = as.data.frame(t(df));
        rm(element);
        return(df)
    }
}