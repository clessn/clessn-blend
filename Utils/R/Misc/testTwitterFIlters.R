clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

tictoc::tic()
myfilter <- clessnhub::create_filter(metadata = list(twitterAccountHasBeenScraped = "1"))
dfPersons <- clessnhub::get_items('persons', myfilter,  download_data = F)
tictoc::toc()

tictoc::tic()
myfilter <- clessnhub::create_filter(data = list(twitterPostsCount__gte=1))
dfPersons <- clessnhub::get_items('persons', myfilter, download_data = T)
tictoc::toc()

tictoc::tic()
myfilter <- clessnhub::create_filter(metadata = list(personType="media"), data = list(creationDate__gte="2021-08-08"))
dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = T)
tictoc::toc()

tictoc::tic()
myfilter <- clessnhub::create_filter(metadata = list(personType__regex="^media$"), data = list(creationDate__gte="2021-08-31"))
dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = T)
tictoc::toc()

tictoc::tic()
myfilter <- clessnhub::create_filter(metadata = list(personType__regex=".*(?<!journalist)$"), data = list(creationDate__gte="2021-08-31"))
dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = T)
tictoc::toc()

tictoc::tic()
myfilter <- clessnhub::create_filter(metadata = list(personType__regex="mp|candidate|media|political_party|public_service"), data = list(creationDate__gte="2021-08-31"))
dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = T)
tictoc::toc()

tictoc::tic()
myfilter <- clessnhub::create_filter( data = list(creationDate__gte="2021-09-02"))
dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = T)
tictoc::toc()


# get all mps and journalists
clessnverse::logit(scriptname, "getting MPs", logger)
filter <- clessnhub::create_filter(type="mp", schema="v2")
dfMPs <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

clessnverse::logit(scriptname, "getting candidates", logger)
filter <- clessnhub::create_filter(type="candidate", schema="v2")
dfCandidates <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

clessnverse::logit(scriptname, "getting journalists", logger)
filter <- clessnhub::create_filter(type="journalist", schema="v2")
dfJournalists <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

clessnverse::logit(scriptname, "getting political parties", logger)
filter <- clessnhub::create_filter(type="political_party", schema="v1")
dfParties <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

clessnverse::logit(scriptname, "getting medias", logger)
filter <- clessnhub::create_filter(type="media", schema="v1")
dfMedias <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

dfPersons <- dfMPs %>% full_join(dfCandidates) %>% full_join(dfJournalists) %>%
  dplyr::select(key, type, data.fullName, data.twitterHandle) %>% 
  na.omit()
