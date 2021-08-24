###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        all-persons-tweets-collector                         #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################


###############################################################################
########################      Functions and Globals      ######################
###############################################################################


###############################################################################
# Function : installPackages
# This function installs all packages requires in this script and all the
# scripts called by this one
#
installPackages <- function() {
  # Define the required packages if they are not installed
  required_packages <- c("stringr", 
                         "tidyr",
                         "optparse",
                         "RCurl", 
                         "httr",
                         "jsonlite",
                         "dplyr", 
                         "XML", 
                         "tm",
                         "textcat",
                         "tidytext", 
                         "tibble",
                         "devtools",
                         "rtweet",
                         "clessn/clessnverse",
                         "clessn/clessn-hub-r")
  
  # Install missing packages
  new_packages <- required_packages[!(required_packages %in% installed.packages()[,"Package"])]
  
  for (p in 1:length(new_packages)) {
    if ( grepl("\\/", new_packages[p]) ) {
      if (grepl("clessnverse", new_packages[p])) {
        devtools::install_github(new_packages[p], ref = "v1", upgrade = "never", quiet = FALSE, build = FALSE)
      } else {
        devtools::install_github(new_packages[p], upgrade = "never", quiet = FALSE, build = FALSE)
      }
    } else {
      install.packages(new_packages[p])
    }  
  }
  
  # load the packages
  # We will not invoque the CLESSN packages with 'library'. The functions 
  # in the package will have to be called explicitely with the package name
  # in the prefix : example clessnverse::evaluateRelevanceIndex
  for (p in 1:length(required_packages)) {
    if ( !grepl("\\/", required_packages[p]) ) {
      library(required_packages[p], character.only = TRUE)
    } else {
      if (grepl("clessn-hub-r", required_packages[p])) {
        packagename <- "clessnhub"
      } else {
        packagename <- stringr::str_split(required_packages[p], "\\/")[[1]][2]
      }
    }
  }
} # </function installPackages>






getTweets <- function(handle, dfPerson, token, scriptname, logger) {
  
  if (is.na(handle)) {
    clessnverse::logit(scriptname, paste("twitter handle is NA for", dfPerson$data.fullName,", exiting..."), logger)
    return()
  }
  
  clessnverse::logit(scriptname, paste("checking if there are already tweets in the hub for", dfPerson$data.fullName, "(", handle, ")"), logger)
  myfilter <- clessnhub::create_filter(metadata = list("twitterHandle"=handle))
  dfTweets <- clessnhub::get_items('tweets', filter = myfilter, max_pages = 1, download_data = FALSE)

  if (!is.null(dfTweets) && nrow(dfTweets) > 0 ) clessnverse::logit(scriptname, paste("found", nrow(dfTweets), "tweets in the hub for", handle), logger) else clessnverse::logit(scriptname, paste("no tweets found in the hub for", handle), logger)
    
  if ((handle %in% dfTweets$metadata.twitterHandle)) {
    # we already scraped the tweets of this person => let's get only the last few tweets
    clessnverse::logit(scriptname, paste("getting tweets from",dfPerson$data.fullName,"(",handle,")"), logger)
    this_pass_tweets <- search_tweets(q = paste("from:",handle,sep=''), retryonratelimit=T, token = token)
  } else {
    # we never scraped the tweets of this person => let's get the full timeline
    clessnverse::logit(scriptname, paste("getting full twitter timeline from",dfPerson$data.fullName,"(",handle,")"), logger)
    this_pass_tweets <- rtweet::get_timeline(handle,n=3200, token = token)
  }
  
  if (nrow(this_pass_tweets) > 0) {
    clessnverse::logit(scriptname, "Merging tweet types", logger)
    data.type <- dplyr::case_when(this_pass_tweets$is_retweet == TRUE ~ rep("retweet", nrow(this_pass_tweets)), TRUE ~ NA_character_)
    data.type1 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
    
    data.type = dplyr::case_when(this_pass_tweets$is_quote == TRUE ~ rep("quote", nrow(this_pass_tweets)),TRUE ~ NA_character_)
    data.type2 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
    
    data.type3 = data.frame(index = rep(as.character(1:nrow(this_pass_tweets))), data.type = rep("tweet", nrow(this_pass_tweets)))
    
    df <- left_join(data.type1, data.type2, by="index") %>% mutate(column=ifelse(!is.na(data.type1$data.type), data.type1$data.type, data.type2$data.type))
    df <- left_join(df, data.type3, by="index") %>% mutate(data.type=ifelse(!is.na(df$column), df$column, data.type3$data.type)) %>% select(data.type)
    
    
    # construct a datafra,e corresponding to the datastructure we want to write to the hub
    clessnverse::logit(scriptname, "Building dataframe to commit", logger)
    df_to_commit <- data.frame(key = paste("t", this_pass_tweets$status_id, sep='') %>% gsub("tt","t",.),
                               type = rep(dfPerson$type, nrow(this_pass_tweets)),
                               schema = rep("v1",nrow(this_pass_tweets)),
                               data.tweetID = this_pass_tweets$status_id,
                               data.creationDate = format(this_pass_tweets$created_at, "%Y-%m-%d"),
                               data.creationTime = format(this_pass_tweets$created_at, "%H:%M %z"),
                               data.screenName = this_pass_tweets$screen_name,
                               data.personKey = rep(dfPerson$key, nrow(this_pass_tweets)),
                               
                               data.text = this_pass_tweets$text,
                               data.source = this_pass_tweets$source,
                               data.type = df$data.type,
                               
                               data.likeCount = this_pass_tweets$favorite_count,
                               data.retweetCount = this_pass_tweets$retweet_count,
                               data.quoteCount = this_pass_tweets$quote_count,
                               
                               data.hashtags = sapply(this_pass_tweets$hashtags[!!length(this_pass_tweets$hashtags)], toString),
                               data.urls = sapply(this_pass_tweets$urls_expanded_url[!!length(this_pass_tweets$urls_expanded_url)], toString),
                               data.mediaUrls = sapply(this_pass_tweets$media_expanded_url[!!length(this_pass_tweets$media_expanded_url)], toString),
                               data.mentions = sapply(this_pass_tweets$mentions_screen_name[!!length(this_pass_tweets$mentions_screen_name)], toString),
                               
                               data.retweetID = this_pass_tweets$retweet_status_id,
                               data.retweetCreationDate = format(this_pass_tweets$retweet_created_at, "%Y-%m-%d"),
                               data.retweetCreationTime = format(this_pass_tweets$retweet_created_at, "%H:%M %z"),
                               data.retweetText = this_pass_tweets$retweet_text,
                               data.retweetFrom = this_pass_tweets$retweet_screen_name,
                               
                               data.quotedTweetID = this_pass_tweets$quoted_status_id,
                               data.quotedTweetCreationDate = format(this_pass_tweets$quoted_created_at, "%Y-%m-%d"),
                               data.quotedTweetCreationTime = format(this_pass_tweets$quoted_created_at, "%H:%M %z"),
                               data.quotedTweetText = this_pass_tweets$quoted_text,
                               data.quotedTweetFrom = this_pass_tweets$quoted_screen_name,
                               
                               metadata.tweetUrl = sapply(this_pass_tweets$status_url[!!length(this_pass_tweets$status_url)], toString),
                               metadata.tweetLang = this_pass_tweets$lang,
                               metadata.tweetType = df$data.type,
                               metadata.lastUpdatedOn = format(rep(Sys.time(), nrow(this_pass_tweets)),"%Y-%m-%d"),
                               metadata.lastUpdatedAt = format(rep(Sys.time(), nrow(this_pass_tweets)),"%H:%M %z"),
                               metadata.twitterHandle = handle
    )
    
    clessnverse::logit(scriptname, paste("about to commit",nrow(df_to_commit),"tweets to HUB"), logger)
    
    # write the constructed df to the hub 
    for (i in 1:nrow(df_to_commit)) {
      
      if (i %% 50 == 0) clessnverse::logit(scriptname, paste("written", i, "over", nrow(df_to_commit), "items to the hub"), logger)
      
      key    <- paste("t", df_to_commit$key[i], sep='') %>% gsub("tt","t",.)
      type   <- df_to_commit$type[i]
      schema <- df_to_commit$schema[i]
      
      data_to_commit <- as.list(df_to_commit[i,which(grepl("^data.",names(df_to_commit[i,])))])
      names(data_to_commit) <- gsub("^data.", "", names(data_to_commit))
      metadata_to_commit <- as.list(df_to_commit[i,which(grepl("^metadata.",names(df_to_commit[i,])))])
      names(metadata_to_commit) <- gsub("^metadata.", "", names(metadata_to_commit))
      
      myfilter <- clessnhub::create_filter(key = paste("t", df_to_commit$key[i], sep='') %>% gsub("tt","t",.))
      dfTestIfTweetExist <- clessnhub::get_items('tweets', myfilter, max_pages = 1)
      
      if (is.null(dfTestIfTweetExist)) {
        #cat("\n\n\n")
        #cat("creating", i, "\n")
        #cat("===============================================================\n")
        #cat(key, type, schema,"\n")
        #cat(paste(names(metadata_to_commit), collapse = " * "),"\n")
        #cat(paste(metadata_to_commit, collapse = " * "), "\n")
        #cat(paste(names(data_to_commit), collapse = " * "),"\n")
        #cat(paste(data_to_commit, collapse = " * "), "\n")
        clessnhub::create_item('tweets', key = key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
      } else {
        #cat("\n\n\n")
        #cat("updating", i, "\n")
        #cat("===============================================================\n")
        #cat(key, type, schema,"\n")
        #cat(paste(names(metadata_to_commit), collapse = " * "),"\n")
        #cat(paste(metadata_to_commit, collapse = " * "), "\n")
        #cat(paste(names(data_to_commit), collapse = " * "),"\n")
        #cat(paste(data_to_commit, collapse = " * "), "\n")
        clessnhub::edit_item('tweets', key = key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
      }#if (!df_to_commit$key[i] %in% dfTweets$key)
    }#for (i in 1:nrow(df_to_commit))
  }#if (nrow(this_pass_tweets) > 0)
  
  
  # update the person's twitter info in the hub from the last tweet collected
  i <- nrow(this_pass_tweets)
  twitter_name <- this_pass_tweets$name[i]
  twitter_id <- this_pass_tweets$user_id[i]
  twitter_location <- this_pass_tweets$location[i]
  twitter_desc <- this_pass_tweets$description[i]
  twitter_account_protected <- if (this_pass_tweets$protected[i]) 1 else 0
  twitter_account_lang <- this_pass_tweets$account_lang[i]
  twitter_account_created_on <- format(this_pass_tweets$account_created_at[i],"%Y-%m-%d")
  twitter_account_created_at <- format(this_pass_tweets$account_created_at[i],"%H:%M %z")
  twitter_account_verified <- if (this_pass_tweets$verified[i]) 1 else 0
  twitter_followers_count <- this_pass_tweets$followers_count[i]
  twitter_friends_count <- this_pass_tweets$friends_count[i]
  twitter_listed_count <- this_pass_tweets$listed_count[i]
  twitter_posts_count <- this_pass_tweets$statuses_count[i]
  twitter_profile_url <-  paste("https://www.twitter.com/", handle, sep='')
  twitter_profile_banner_url <- this_pass_tweets$profile_banner_url[i]
  twitter_profile_image_url <- this_pass_tweets$profile_image_url[i]
  
  # get person from the hub
  person <- clessnhub::get_item('persons', dfPerson$key)
  if (!is.null(person) && (handle != "@LesVertsCanada" && handle != "@pcc_hq" && handle != "@NPD_QG" && handle != "@parti_liberal" && handle != "@ppopulaireca") ) {
    person_data <- person$data
    person_metadata <- person$metadata
    
    person$data$twitterName <- twitter_name
    person$data$twitterID <- twitter_id
    person$data$twitterLocation <- twitter_location
    person$data$twitterDesc <- twitter_desc
    person$data$twitterAccountProtected <- twitter_account_protected
    person$data$twitterLang <- twitter_account_lang
    person$data$twitterAccountCreatedOn <- twitter_account_created_on
    person$data$twitterAccountCreatedAt <- twitter_account_created_at
    person$data$twitterAccountVerified <- twitter_account_verified
    person$data$twitterProfileURL <- twitter_profile_url
    person$data$twitterProfileBannerURL <- twitter_profile_banner_url
    person$data$twitterProfileImageURL <- twitter_profile_image_url
    
    if (!is.null(person$data$twitterUpdateDateStamps) && nchar(person$data$twitterUpdateDateStamps) > 0) {
      twitter_update_list <- as.list(strsplit(person$data$twitterUpdateDateStamps, ',')[[1]])
      latest_twitter_update <- twitter_update_list[length(twitter_update_list)][[1]]
    } else {
      latest_twitter_update <- "2000-01-01 00:00:00 EDT"
    }
    
    if (is.null(person$data$twitterUpdateDateStamps) || nchar(person$data$twitterUpdateDateStamps) == 0 || difftime(Sys.time(),latest_twitter_update,units = "hours") >= 24) {    
      clessnverse::logit(scriptname, paste("updating new twitter data for", person$data$fullName,"(",handle,")"), logger)
      person$data$twitterFollowersCount <- if (is.null(person$data$twitterFollowersCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) twitter_followers_count else paste(person$data$twitterFollowersCount,twitter_followers_count,sep=',')
      person$data$twitterFriendsCount <- if (is.null(person$data$twitterFriendsCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) twitter_friends_count else paste(person$data$twitterFriendsCount,twitter_friends_count,sep=',')
      person$data$twitterListedCount <- if (is.null(person$data$twitterListedCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) twitter_listed_count else paste(person$data$twitterListedCount,twitter_listed_count,sep=',')
      person$data$twitterPostsCount <- if (is.null(person$data$twitterPostsCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) twitter_posts_count else paste(person$data$twitterPostsCount,twitter_posts_count,sep=',')
      person$data$twitterUpdateDateStamps <- if (is.null(person$data$twitterUpdateDateStamp) || nchar(person$data$twitterUpdateDateStamps) == 0) format(Sys.time(),"%Y-%m-%d") else paste(person$data$twitterUpdateDateStamp,format(Sys.time(),"%Y-%m-%d"),sep=',')
      person$data$twitterUpdateTimeStamps <- if (is.null(person$data$twitterUpdateTimeStamp) || nchar(person$data$twitterUpdateTimeStamps) == 0) format(Sys.time(),"%H:%M %z") else paste(person$data$twitterUpdateTimeStamp,format(Sys.time(),"%H:%M %z"),sep=',')
    }
    
    log_data <- paste("updating", person$data$fullName, "with key", person$key, "\n")
    log_data <- paste(log_data, "===============================================================\n")
    log_data <- paste(log_data, person$key, person$type, person$schema,"\n")
    log_data <- paste(log_data, paste(names(person$metadata), collapse = " * "),"\n")
    log_data <- paste(log_data, paste(person$metadata, collapse = " * "), "\n")
    log_data <- paste(log_data, paste(names(person$data), collapse = " * "),"\n")
    log_data <- paste(log_data, paste(person$data, collapse = " * "))
    log_metadata <- paste("Local time", Sys.time())
    clessnverse::logit(scriptname, paste("pushing twitter data for", person$data$fullName,"(",handle,") to hub"), logger)
    clessnhub::edit_item('persons', key = person$key, type = person$type, schema = person$schema, metadata = person$metadata, data = person$data)
  } #if (!is.null(person))
} #</getTweets>




###############################################################################
########################               MAIN              ######################
###############################################################################

main <- function(scriptname, logger) {
  
  # load twitter token rds file
  if (file.exists(".rtweet_token.rds")) token <- readRDS(".rtweet_token.rds")
  if (file.exists("~/.rtweet_token.rds")) token <- readRDS("~/.rtweet_token.rds")
  
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
  
  #clessnverse::logit(scriptname, "getting tweets", logger)
  #dfTweets <- clessnhub::get_items('tweets', download_data = FALSE)
  
  
  ###############################################################################
  # Let's get serious!!!
  # Run through the persons list, get the tweets content 
  # and store them in the hub
  # Take the opportunity to add attributes to the persons
  # such as url of the photo etc
  #
  
  # Loop through the entire table of persons
  clessnverse::logit(scriptname, paste("start looping through",nrow(dfPersons),"persons"), logger)

  persons_index <- which(dfPersons$key == "72773" | dfPersons$key == "58733" | dfPersons$key == "71588" | 
                         dfPersons$key == "00eefdd89b55ced5b61f7b82297e5787" | dfPersons$key == "bea0eb58fd0768bc91c0a8cb6ac52cd5" | 
                         dfPersons$key == "104669")
  
  for (i_person in persons_index) {
  #for (i_person in 1:nrow(dfPersons)) {
    getTweets(handle = dfPersons$data.twitterHandle[i_person], dfPerson = dfPersons[i_person,],
              token = token, scriptname = scriptname, logger = logger)
  } #for (i_person in 1:nrow(dfPersons))
  
  
  # Loop through the entire table of parties EN twitter accounts
  clessnverse::logit(scriptname, paste("start looping through",nrow(dfParties),"english parties' accounts"), logger)
  
  for (i_party in 1:nrow(dfParties)) {
    getTweets(handle = dfParties$data.twitterHandleEN[i_party], dfPerson = dfParties[i_party,],
              token = token, scriptname = scriptname, logger = logger)
  } #for (i_party in 1:nrow(dfParties))
  
  # Loop through the entire table of parties FR twitter accounts
  clessnverse::logit(scriptname, paste("start looping through",nrow(dfParties),"french parties' accounts"), logger)
  
  for (i_party in 1:nrow(dfParties)) {
    getTweets(handle = dfParties$data.twitterHandleFR[i_party], dfPerson = dfParties[i_party,],
              token = token, scriptname = scriptname, logger = logger)
  } #for (i_party in 1:nrow(dfParties))
  
  # Loop through the entire table of medias twitter accounts
  clessnverse::logit(scriptname, paste("start looping through",nrow(dfMedias),"medias' accounts"), logger)
  
  for (i_media in 1:nrow(dfMedias)) {
    getTweets(handle = dfMedias$data.twitterHandle[i_media], dfPerson = dfMedias[i_media,],
              token = token, scriptname = scriptname, logger = logger)
  } #for (i_party in 1:nrow(dfParties))

} #</main>





tryCatch( 
  {
    installPackages()
    library(dplyr)
    
    log_output <- c("file","hub")
    #log_output <- "file"
    
    if (!exists("scriptname")) scriptname <<- "tweets-and-friends.R"
    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, log_output, Sys.getenv("LOG_PATH"))

    # login to the hub
    clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    clessnverse::logit(scriptname, "connecting to hub", logger)
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)
    
    main(scriptname, logger)
  },
  
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=''), logger)
    print(e)
  },
  
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::logclose(logger)
    rm(logger)
  }
)
