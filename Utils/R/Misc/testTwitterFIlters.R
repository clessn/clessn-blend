clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

tictoc::tic()
myfilter <- clessnhub::create_filter(data = list("creationDate"="2021-08-23"))
dfTweets <- clessnhub::get_items('tweets', myfilter)
tictoc::toc()
