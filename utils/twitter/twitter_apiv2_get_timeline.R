############### 
# Avec des calls directs à l'API

require(httr)


bearer_token = Sys.getenv("TW_BEARER_TOKEN")

headers = c(
  `Authorization` = sprintf('Bearer %s', bearer_token)
)


params <- list(`user.fields` = 'id')

handle <- 'pponcet'

url_handle <-
  sprintf('https://api.twitter.com/2/users/by?usernames=%s', handle)

response <-
  httr::GET(url = url_handle,
            httr::add_headers(.headers = headers),
            query = params)

obj <- httr::content(response, as = "text")
df_obj <- jsonlite::fromJSON(obj)
print(df_obj$data$id)



params <- list(
  `max_results` = '100',
  `tweet.fields` = 'attachments,author_id,conversation_id,created_at,entities,id,lang',
  # `start_time` = "2020-07-01T00:00:00Z",
  # `end_time" = "2020-07-02T18:00:00Z",
  `expansions` = "attachments.poll_ids,attachments.media_keys,author_id",
  `user.fields` = "username,name,id,description",
  `media.fields` = "url", 
  `place.fields` = "country_code"
  # `poll.fields` = "options"
)


response <- httr::GET(
  url = paste('https://api.twitter.com/2/users/',df_obj$data$id, '/tweets', sep=''), 
  httr::add_headers(.headers=headers), 
  query = params
)


recent_search_body <-
  httr::content(
    response,
    as = 'parsed',
    type = 'application/json',
    simplifyDataFrame = TRUE
  )

View(recent_search_body$data)


#########
# Avec RTwitterV2

bearer_token = Sys.getenv("TW_BEARER_TOKEN")

obj <- RTwitterV2::get_user_v2(token = bearer_token, user_names =  "pponcet")

print(test$user_id)

df <- RTwitterV2::get_timelines_v2(token = bearer_token, user_id = obj$user_id)
