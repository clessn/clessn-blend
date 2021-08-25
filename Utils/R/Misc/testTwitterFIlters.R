clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

tictoc::tic()
myfilter <- clessnhub::create_filter(metadata = list("twitterHandle"="BoissonDarryl"))
dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = F)
tictoc::toc()

for (i in 1:nrow(dfTweets)) {
  #clessnhub::delete_item('tweets', dfTweets$key[i])
}

myfilter <- clessnhub::create_filter(data = list("twitterAccountVerified"=1))
dfTweets <- clessnhub::get_items('persons', myfilter, download_data = T)
