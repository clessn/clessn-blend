library(tidyverse)
library(rtweet)

source("/Users/williampoirier/Dropbox/Travail/Ulaval/CLESSN/loginHub2.R")

#### 0. Get Handles ####
clessnhub::login(user, mdp)
# use "mp" in the type filter to get politicians only.
filter <- clessnhub::create_filter(type = "mp", schema = "v2")
Persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
Persons <- Persons %>%
  dplyr::select(data.fullName,data.twitterHandle) %>%
  na.omit()

#### 1. get_tweets ####

# Probablement besoin d'une clé pour elle
Tweets <- rtweet::get_timeline(Persons$data.twitterHandle[1],n=3200)

# Pas de clé, mais plus limitée
Tweets2 <- rtweet::search_tweets(q = paste0("from:",Persons$data.twitterHandle[4]),
                                retryonratelimit=T)

#To check limits of use for search_tweets
Limits<-rtweet::rate_limit()
Limits$limit[235]
Limits$remaining[235]

nbCandidates <- 338*5 
totalTimeMin <- ((nbCandidates/180)*15)
totalTimeHrs <- ((nbCandidates/180)*15)/60
# so it's typically gonna take 2 hours 21 minutes

#### 2. Wrap arround the table ####
for (i in 1:nrow(Persons)){
  Tweets <- rtweet::search_tweets(q = paste0("from:",Persons$data.twitterHandle[i]),
                                   retryonratelimit=T)
  if(i==1){
    TwitterData <- Tweets
  }
  else{
    TwitterData <- rbind(TwitterData,Tweets)
  }
  print(i)
}
