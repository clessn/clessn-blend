send_query <- function(base_url, sql, username, password)
{
  response <- httr::POST(
    url = paste0(base_url, "/queries/create/"),
    httr::authenticate(username, password),
    body = list(
      query_sql = sql
    ),
    encode = "json"
  )
  
  query_id <- httr::content(response, as = "parsed")$query_id
  return(query_id)
}

check_query_status <- function(base_url, query_id, username, password, print_logs=F)
{
  response <- httr::GET(
    url = paste0(base_url, "/queries/status/", query_id),
    httr::authenticate(username, password)
  )
  content <- httr::content(response)
  
  print(paste("is_complete", content$is_complete))
  print(paste("date_begin", content$date_begin))
  print(paste("date_end", content$date_end))
  print(paste("has_errors", content$has_errors))
  print(paste("has_warnings", content$has_warnings))
  
  if (print_logs)
  {
    print("Logs")
    for (log in content$logs)
    {
      print(paste0("  ", log))
    }
  }
}

download_query_result <- function(base_url, query_id, username, password)
{
  response <- httr::GET(
    url = paste0(base_url, "/queries/status/", query_id),
    httr::authenticate(username, password)
  )
  content <- httr::content(response)
  
  if (content$is_complete)
  {
    return(readr::read_csv(content$url))
  }
  return(NULL)
}

json_to_df <- function(json_data)
{
  as_list <- jsonlite::fromJSON(json_data)
  as_list[lengths(as_list) == 0] <- NA
  item <- tidyr::as_tibble(as_list)
  return(item)
}

unnest_json_tibble <- function(table, columns)
{
  for (column in columns)
  {
    for (i in 1:length(table[[column]]))
    {
      table[[paste0("tmp_",column)]][[i]] <- json_to_df(table[[column]][[i]])
    }
  }
  table <- table[,!(names(table) %in% columns)]
  
  for (column in columns)
  {
    table[[column]] <- table[[paste0("tmp_", column)]]
  }
  
  table <- table[,!(names(table) %in% paste0("tmp_", columns))]
  table <- tidyr::unnest(table, cols=tidyr::all_of(columns), names_sep = ".")
  return(table)
}

# main

base_url = "https://hub.dev.clessn.cloud"
username = "patrick"
password = "soleil123"

sql = "SELECT * FROM hub_v2_data_tweet"
        #WHERE type = 'tweet' 
        #AND metadata @> '{\"twitterHandle\": \"@JdeMontreal\"}';"

# Envoie le sql au hub
query_id <- send_query(base_url, sql, username, password)

# print à l'écran le statut. si is_complete is TRUE, alors continuons
check_query_status(base_url, query_id, username, password)
check_query_status(base_url, query_id, username, password, print_logs=T)

# on télécharge le résultat
result <- download_query_result(base_url, query_id, username, password)
# on transforme le json du résultat en colonnes
result <- unnest_json_tibble(result, c("data", "metadata"))
