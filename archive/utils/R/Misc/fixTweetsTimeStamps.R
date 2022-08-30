clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

is.empty.list <- function(list) {
  return(length(list) == 0) 
}

is.na.char <- function(x) {
  return (x=="NA")
}

myfilter <- clessnhub::create_filter(type="political_party")
dfPersons <- clessnhub::get_items('persons', myfilter)

for (p in 1:6) {
  cat(dfPersons$data.fullName[p], dfPersons$data.twitterHandleEN[p],"\n")
  
  person <- clessnhub::get_item('persons', dfPersons$key[p])
  
  if (!is.null(person$data$twitterAccountCreatedOn) && !is.null(person$data$twitterAccountCreatedAt)) {
    
    date_time <- as.POSIXct(paste(format(as.POSIXct(person$data$twitterAccountCreatedOn, "%Y-%m-%d", tz="UTC"), format("%Y-%m-%d")),
                                  person$data$twitterAccountCreatedAt
                                 ), 
                            format="%Y-%m-%d %H:%M", tz="UTC")
    
    person$data$twitterAccountCreatedAt <- format(date_time, "%H:%M %Z")
    person$data$twitterAccountCreatedOn <- format(date_time, "%Y-%m-%d")
    
    dates <- strsplit(person$data$twitterUpdateDateStamps, ",")
    times <- strsplit(person$data$twitterUpdateTimeStamps, ",")
    
    for (d in 1: length(dates[[1]])) {
      if (grepl("\\+|\\-", times[[1]][d])) {
        date_time <- as.POSIXct(paste(dates[[1]][d],times[[1]][d]), format="%Y-%m-%d %H:%M")
        attr(date_time, "tzone") <- "UTC"
        times[[1]][d] <- format(date_time, "%H:%M %Z")
        dates[[1]][d] <- format(date_time, "%Y-%m-%d")
      }
    }
    
    person$data$twitterUpdateDateStamps <- paste(dates[[1]], collapse=",")
    person$data$twitterUpdateTimeStamps <- paste(times[[1]], collapse=",")
    
    person$data[sapply(person$data,is.null)] <- NA
    person$metadata[sapply(person$metadata,is.null)] <- NA
    
    person$data[sapply(person$data,is.empty.list)] <- NA
    person$metadata[sapply(person$metadata,is.empty.list)] <- NA
    
    clessnhub::edit_item(table = 'persons', key = person$key, type = person$type, schema = person$schema, metadata = person$metadata, data = person$data)
  }
  
  
  myfilter <- clessnhub::create_filter(schema = "v1", metadata = list(twitterHandle=dfPersons$data.twitterHandleEN[p]))
  dfTweets <- clessnhub::get_items('tweets', myfilter, download_data = T)
  
  if (!is.null(dfTweets)) {
  
    for (t in 1:nrow(dfTweets)) {
    #for (t in 2:2) {
      tweet <- clessnhub::get_item('tweets', dfTweets$key[t])
      
      
      if (!is.null(tweet$metadata$lastUpdatedAt) && grepl("\\+|\\-", tweet$metadata$lastUpdatedAt)) {
        date_time <- as.POSIXct(paste(tweet$metadata$lastUpdatedOn, tweet$metadata$lastUpdatedAt), format="%Y-%m-%d %H:%M")
        attr(date_time, "tzone") <- "UTC"
        
        tweet$metadata$lastUpdatedAt <- format(date_time, "%H:%M %Z")
        tweet$metadata$lastUpdatedOn <- format(date_time, "%Y-%m-%d")
      }
      
      if (!is.null(tweet$data$creationTime) && grepl("\\+|\\-", tweet$data$creationTime)) {
        date_time <- as.POSIXct(paste(tweet$data$creationDate, tweet$data$creationTime), format="%Y-%m-%d %H:%M", tz="UTC")
        #attr(date_time, "tzone") <- "UTC"
        
        tweet$data$creationTime <- format(date_time, "%H:%M %Z")
        tweet$data$creationDate <- format(date_time, "%Y-%m-%d")
      }
      
      if (!is.null(tweet$data$retweetCreationTime) && grepl("\\+|\\-", tweet$data$retweetCreationTime)) {
        date_time <- as.POSIXct(paste(tweet$data$retweetCreationDate, tweet$data$retweetCreationTime), format="%Y-%m-%d %H:%M", tz="UTC")
        #attr(date_time, "tzone") <- "UTC"
        
        tweet$data$retweetCreationTime <- format(date_time, "%H:%M %Z")
        tweet$data$retweetCreationDate <- format(date_time, "%Y-%m-%d")
      }
      
      if (!is.null(tweet$data$quotedTweetCreationTime) && grepl("\\+|\\-", tweet$data$quotedTweetCreationTime)) {
        date_time <- as.POSIXct(paste(tweet$data$quotedTweetCreationDate, tweet$data$quotedTweetCreationTime), format="%Y-%m-%d %H:%M", tz="UTC")
        #attr(date_time, "tzone") <- "UTC"
        
        tweet$data$quotedTweetCreationTime <- format(date_time, "%H:%M %Z")
        tweet$data$quotedTweetCreationDate <- format(date_time, "%Y-%m-%d")
      }
      
      if (is.null(tweet$metadata$personType)) {
        tweet$metadata$personType <- tweet$type
        tweet$type <- tweet$metadata$tweetType
      }
      
      if (tweet$metadata$personType == "tweet" || tweet$metadata$personType == "retweet" || tweet$metadata$personType == "quote")  {
        tweet$metadata$personType <- person$type
      }
      
      tweet$data[sapply(tweet$data,is.null)] <- NA_character_ 
      tweet$metadata[sapply(tweet$metadata,is.null)] <- NA_character_
      tweet$data[sapply(tweet$data,is.na)] <- NA_character_ 
      tweet$metadata[sapply(tweet$metadata,is.na)] <- NA_character_
      tweet$data[sapply(tweet$data,is.na.char)] <- NA_character_ 
      tweet$metadata[sapply(tweet$metadata,is.na.char)] <- NA_character_
      
      if (!is.null(tweet$metadata$tweetType)) {
        tweet$metadata$tweetType <- NULL
        tweet$data$type <- NULL
      }
      
      tweet$schema <- "v2"
      
      if (tweet$type == "retweet" && is.null(tweet$data$originalTweetID)) {
        tweet$data$originalTweetID <-tweet$data$retweetID
        tweet$data$originalTweetCreationDate <- tweet$data$retweetCreationDate
        tweet$data$originalTweetCreationTime <- tweet$data$retweetCreationTime
        tweet$data$originalTweetText <- tweet$data$retweetText
        tweet$data$originalTweetFrom <-tweet$data$retweetFrom
        
        tweet$data$quotedTweetID <- NULL
        tweet$data$quotedTweetCreationDate <- NULL
        tweet$data$quotedTweetCreationTime <- NULL
        tweet$data$quotedTweetText <- NULL
        tweet$data$quotedTweetFrom <- NULL
        
        tweet$data$retweetID <- NULL
        tweet$data$retweetCreationDate <- NULL
        tweet$data$retweetCreationTime <- NULL
        tweet$data$retweetText <- NULL
        tweet$data$retweetFrom <- NULL
      }
      
      if (tweet$type == "quote" && is.null(tweet$data$originalTweetID)) {
        tweet$data$originalTweetID <- tweet$data$quotedTweetID
        tweet$data$originalTweetCreationDate <- tweet$data$quotedTweetCreationDate
        tweet$data$originalTweetCreationTime <- tweet$data$quotedTweetCreationTime
        tweet$data$originalTweetText <- tweet$data$quotedTweetText
        tweet$data$originalTweetFrom <-tweet$data$quotedTweetFrom
        
        tweet$data$quotedTweetID <- NULL
        tweet$data$quotedTweetCreationDate <- NULL
        tweet$data$quotedTweetCreationTime <- NULL
        tweet$data$quotedTweetText <- NULL
        tweet$data$quotedTweetFrom <- NULL
        
        tweet$data$retweetID <- NULL
        tweet$data$retweetCreationDate <- NULL
        tweet$data$retweetCreationTime <- NULL
        tweet$data$retweetText <- NULL
        tweet$data$retweetFrom <- NULL
      }

      if (tweet$type == "tweet") {
        tweet$data$originalTweetID <- NA_character_
        tweet$data$originalTweetCreationDate <- NA_character_
        tweet$data$originalTweetCreationTime <- NA_character_
        tweet$data$originalTweetText <- NA_character_
        tweet$data$originalTweetFrom <-NA_character_
        
        tweet$data$quotedTweetID <- NULL
        tweet$data$quotedTweetCreationDate <- NULL
        tweet$data$quotedTweetCreationTime <- NULL
        tweet$data$quotedTweetText <- NULL
        tweet$data$quotedTweetFrom <- NULL
        
        tweet$data$retweetID <- NULL
        tweet$data$retweetCreationDate <- NULL
        tweet$data$retweetCreationTime <- NULL
        tweet$data$retweetText <- NULL
        tweet$data$retweetFrom <- NULL
      }
      
      
      clessnhub::edit_item('tweets', key = tweet$key, type = tweet$type, schema = tweet$schema, metadata = tweet$metadata, data = tweet$data)
    
    }
  }
}

