library(dplyr)

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

filter <- clessnhub::create_filter(type="candidate", schema="candidate_elxn_qc2022", metadata=NULL, data=NULL)

df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)


df_tweets <- data.frame()

for (i in 3:nrow(df_persons)) {
    filter <- clessnhub::create_filter(data=list(personKey = df_persons$key[i]))

    new_tweets <- clessnhub::get_items('tweets', filter, download_data = T)

    if (!is.null(new_tweets) && nrow(new_tweets) > 0 ) {
        df_tweets <- df_tweets %>% dplyr::rbind(new_tweets)
    }
}
