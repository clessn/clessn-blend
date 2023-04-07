require(httr)


bearer_token = Sys.getenv("TW_BEARER_TOKEN")
headers = c(
  `Authorization` = sprintf('Bearer %s', bearer_token)
)


params = list(
  `query` = 'from:pponcet',
  `max_results` = '100',
  `tweet.fields` = 'created_at,lang,conversation_id'
)


response <- httr::GET(url = 'https://api.twitter.com/2/tweets/search/recent', httr::add_headers(.headers=headers), query = params)


recent_search_body <-
  httr::content(
    response,
    as = 'parsed',
    type = 'application/json',
    simplifyDataFrame = TRUE
  )

View(recent_search_body$data)
