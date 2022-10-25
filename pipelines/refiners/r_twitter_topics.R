library(dplyr)

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


# Example 1 : all the candidates of the qc election 2022
filter <- clessnhub::create_filter(type="candidate", schema="candidate_elxn_qc2022", metadata=NULL, data=NULL)
df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)


# And finally build out tweets dataframe
df_tweets <- data.frame()

for (i in 310:nrow(df_persons)) {
    cat("getting tweets from", df_persons$data.fullName[i], "with twitter handle", df_persons$data.twitterHandle[i], "\n")

    filter <- clessnhub::create_filter(data=list(personKey = df_persons$key[i]))

    new_tweets <- clessnhub::get_items('tweets', filter, download_data = T)

    if (!is.null(new_tweets) && nrow(new_tweets) > 0) {
        df_tweets <- df_tweets %>% rbind(new_tweets)
    }
}
