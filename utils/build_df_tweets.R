library(dplyr)

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

# Below are a lot of examples on how to filter populations

# Example 1 : all the candidates of the qc election 2022
filter <- clessnhub::create_filter(type="candidate", schema="candidate_elxn_qc2022", metadata=NULL, data=NULL)

# Example 2 : all the mp of the european parliament
#filter <- clessnhub::create_filter(type="mp", schema="", metadata=list(institution="European Parliament"), data=NULL)


# Example 3 : all the mp of the quebec national assembly
#filter <- clessnhub::create_filter(type="mp", schema="", metadata=list(institution="National Assembly of Quebec"), data=NULL)


#Example 4 : All the journalists we have in our 'persons' database from the province of quebec
#filter <- clessnhub::create_filter(type="journalist", schema="", metadata=list(province_or_state="QC"), data=NULL)


# Now get the data
df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)


# And finally build out tweets dataframe
df_tweets <- data.frame()

for (i in 1:nrow(df_persons)) {
    filter <- clessnhub::create_filter(data=list(personKey = df_persons$key[i]))

    success <- 0
    while(success == 0) {
        new_tweets <- data.frame()

        tryCatch(
            { new_tweets <- clessnhub::get_items('tweets', filter, download_data = T) },
            error = function(e) {
                cat(paste("Error accessing hub - trying again...\n", e, "\n"))
                if (grepl("Connectez-vous de nouveau", e)) {
                    clessnhub::login(
                        Sys.getenv("HUB_USERNAME"),
                        Sys.getenv("HUB_PASSWORD"),
                        Sys.getenv("HUB_URL"))
                }
            },
            finally = { if (!is.null(df_tweets)) success <- 1 }
        )
    }


    if (!is.null(new_tweets) && nrow(new_tweets) > 0 ) {
        cat(
            paste(
                "retrieved", 
                nrow(df_tweets), 
                "tweets for user #", i, 
                "with twitterHandle", df_persons$data.twitterHandle[i]
                )
        )
        df_tweets <- df_tweets %>% rbind(new_tweets)
    } else {
        if (!is.null(new_tweets) && nrow(new_tweets) == 0) {
            cat(paste("no tweets matching filter for", df_persons$data.twitterHandle[i], "\n"))
        }

        if (is.null(new_tweets)) {
            cat("df_tweets is null")
        }
    }
}
