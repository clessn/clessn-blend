clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
dfTweets <- clessnhub::get_items('tweets', download_data = FALSE)

index <- which(dfTweets$metadata.twitterHandle == "JustinTrudeau")

#for (j in 1:length(index)) {
for (j in 1:nrow(dfTweets)) {
  clessnhub::delete_item('tweets',dfTweets$key[j])
}
