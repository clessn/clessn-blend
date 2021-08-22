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

###############################################################################
########################               MAIN              ######################
###############################################################################

main <- function(scriptname, logger) {
  
  # login to the hub
  clessnverse::logit(scriptname, "connecting to hub", logger)
  clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
  
  # load twitter token rds file
  if (file.exists(".rtweet_token.rds")) token <- readRDS(".rtweet_token.rds")
  if (file.exists("~/.rtweet_token.rds")) token <- readRDS("~/.rtweet_token.rds")
  
  # get all mps and journalists
  clessnverse::logit(scriptname, "getting journalists", logger)
  filter <- clessnhub::create_filter(type="mp", schema="v2")
  dfMPs <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
  
  clessnverse::logit(scriptname, "getting candidates", logger)
  filter <- clessnhub::create_filter(type="candidate", schema="v2")
  dfCandidates <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
  
  clessnverse::logit(scriptname, "getting journalists", logger)
  filter <- clessnhub::create_filter(type="journalist", schema="v2")
  dfJournalists <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
  
  dfPersons <- dfMPs %>% full_join(dfCandidates) %>% full_join(dfJournalists) %>%
    dplyr::select(key, type, data.fullName, data.twitterHandle) %>%
    na.omit()
  
  clessnverse::logit(scriptname, "getting tweets", logger)
  dfTweets <- clessnhub::get_items('tweets', download_data = FALSE)
  
  if (is.null(dfTweets)) {
    dfTweets <- data.frame(key = character(),
                           type = character(),
                           schema = character(),
                           data.tweetID = character(),
                           data.creationDate = character(),
                           data.screenName = character(),
                           data.personKey = character(),
                           data.text = character(),
                           data.source = character(),
                           data.type = character(),
                           data.likeCount = integer(),
                           data.retweetCount = integer(),
                           data.quoteCount = integer(),
                           metadata.tweetUrl = character(),
                           metadata.tweetLang = character(),
                           metadata.tweetType = character(),
                           metadata.scrapeDate = character()
    )
  }
  ###############################################################################
  # Let's get serious!!!
  # Run through the persons list, get the tweets content 
  # and store them in the hub
  # Take the opportunity to add attributes to the persons
  # such as url of the photo etc
  #
  
  # Loop through the entire table
  #for (i_person in 1:nrow(dfPersons)) {
  person_index <- which(dfPersons$key == "72773")
  clessnverse::logit(scriptname, paste("start looping through",nrow(dfPersons),"persons"), logger)
  for (i_person in person_index:person_index) {
    
    if (dfPersons$data.twitterHandle[i_person] %in% dfTweets$metadata.twitterHandle) {
      # we already scraped the tweets of this person => let's get only the last few tweets
      clessnverse::logit(scriptname, paste("getting tweets from",dfPersons$data.fullName[i_person],"(",dfPersons$data.twitterHandle[i_person],")"), logger)
      this_pass_tweets <- search_tweets(q = paste("from:",dfPersons$data.twitterHandle[i_person],sep=''), retryonratelimit=T, token = token)
    } else {
      # we never scraped the tweets of this person => let's get the full timeline
      clessnverse::logit(scriptname, paste("getting full twitter timeline from",dfPersons$data.fullName[i_person],"(",dfPersons$data.twitterHandle,")"), logger)
      this_pass_tweets <- rtweet::get_timeline(dfPersons$data.twitterHandle[i_person],n=3200, token = token)
    }
    
    if (nrow(this_pass_tweets) > 0) {
      data.type <- dplyr::case_when(this_pass_tweets$is_retweet == TRUE ~ rep("retweet", nrow(this_pass_tweets)), TRUE ~ NA_character_)
      data.type1 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
      
      data.type = dplyr::case_when(this_pass_tweets$is_quote == TRUE ~ rep("quote", nrow(this_pass_tweets)),TRUE ~ NA_character_)
      data.type2 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
      
      data.type3 = data.frame(index = rep(as.character(1:nrow(this_pass_tweets))), data.type = rep("tweet", nrow(this_pass_tweets)))
      
      df <- left_join(data.type1, data.type2, by="index") %>% mutate(column=ifelse(!is.na(data.type1$data.type), data.type1$data.type, data.type2$data.type))
      df <- left_join(df, data.type3, by="index") %>% mutate(data.type=ifelse(!is.na(df$column), df$column, data.type3$data.type)) %>% select(data.type)
      
      # construct a datafra,e corresponding to the datastructure we want to write to the hub
      df_to_commit <- data.frame(key = paste("t", this_pass_tweets$status_id, sep='') %>% gsub("tt","t",.),
                                 type = rep(dfPersons$type[i_person], nrow(this_pass_tweets)),
                                 schema = rep("v1",nrow(this_pass_tweets)),
                                 data.tweetID = this_pass_tweets$status_id,
                                 data.creationDate = this_pass_tweets$created_at,
                                 data.screenName = this_pass_tweets$screen_name,
                                 data.personKey = rep(dfPersons$key[i_person], nrow(this_pass_tweets)),
                                 
                                 data.text = this_pass_tweets$text,
                                 data.source = this_pass_tweets$source,
                                 data.type = df$data.type,
                                 
                                 data.likeCount = this_pass_tweets$favorite_count,
                                 data.retweetCount = this_pass_tweets$retweet_count,
                                 data.quoteCount = this_pass_tweets$quote_count,
                                 
                                 data.hashtags = sapply(this_pass_tweets$hashtags[!!length(this_pass_tweets$hashtags)], toString),
                                 data.urls = sapply(this_pass_tweets$status_url[!!length(this_pass_tweets$status_url)], toString),
                                 data.mentions = sapply(this_pass_tweets$mentions_screen_name[!!length(this_pass_tweets$mentions_screen_name)], toString),
                                 
                                 data.retweetID = this_pass_tweets$retweet_status_id,
                                 data.retweetText = this_pass_tweets$retweet_text,
                                 data.retweetFrom = this_pass_tweets$retweet_screen_name,
                                 
                                 data.quotedTweetID = this_pass_tweets$quoted_status_id,
                                 data.quotedTweetCreationDate = this_pass_tweets$quoted_created_at,
                                 data.quotedTweetText = this_pass_tweets$quoted_text,
                                 data.quotedTweetAuthor = this_pass_tweets$quoted_screen_name,
                                 
                                 metadata.tweetUrl = sapply(this_pass_tweets$status_url[!!length(this_pass_tweets$status_url)], toString),
                                 metadata.tweetLang = this_pass_tweets$lang,
                                 metadata.tweetType = df$data.type,
                                 metadata.scrapeDate = rep(Sys.time(), nrow(this_pass_tweets)),
                                 metadata.twitterHandle = dfPersons$data.twitterHandle[i_person]
                                 )
      
      clessnverse::logit(scriptname, paste("constructed dataframe of",nrow(df_to_commit),"tweets to commit"), logger)
      clessnverse::logit(scriptname, paste("about to commit",nrow(df_to_commit),"tweets to HUB"), logger)
      
      # write the constructed df to the hub 
      for (i in 1:nrow(df_to_commit)) {
        key    <- paste("t", df_to_commit$key[i], sep='') %>% gsub("tt","t",.)
        type   <- df_to_commit$type[i]
        schema <- df_to_commit$schema[i]
        
        data_to_commit <- as.list(df_to_commit[i,which(grepl("^data.",names(df_to_commit[i,])))])
        names(data_to_commit) <- gsub("^data.", "", names(data_to_commit))
        metadata_to_commit <- as.list(df_to_commit[i,which(grepl("^metadata.",names(df_to_commit[i,])))])
        names(metadata_to_commit) <- gsub("^metadata.", "", names(metadata_to_commit))
        
        if (!df_to_commit$key[i] %in% dfTweets$key) {
          clessnhub::create_item('tweets', key = key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
          cat("\n\n\n")
          cat("creating", i, "\n")
          cat("===============================================================\n")
          cat(key, type, schema,"\n")
          cat(paste(names(metadata_to_commit), collapse = " * "),"\n")
          cat(paste(metadata_to_commit, collapse = " * "), "\n")
          cat(paste(names(data_to_commit), collapse = " * "),"\n")
          cat(paste(data_to_commit, collapse = " * "), "\n")
        } else {
          cat("\n\n\n")
          cat("updating", i, "\n")
          cat("===============================================================\n")
          cat(key, type, schema,"\n")
          cat(paste(names(metadata_to_commit), collapse = " * "),"\n")
          cat(paste(metadata_to_commit, collapse = " * "), "\n")
          cat(paste(names(data_to_commit), collapse = " * "),"\n")
          cat(paste(data_to_commit, collapse = " * "), "\n")
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
    twitter_account_protected <- this_pass_tweets$protected[i]
    twitter_account_lang <- this_pass_tweets$account_lang[i]
    twitter_account_created_on <- this_pass_tweets$account_created_at[i]
    twitter_account_verified <- this_pass_tweets$verified[i]
    twitter_followers_count <- this_pass_tweets$followers_count[i]
    twitter_friends_count <- this_pass_tweets$friends_count[i]
    twitter_listed_count <- this_pass_tweets$listed_count[i]
    twitter_posts_count <- this_pass_tweets$statuses_count[i]
    twitter_profile_url <-  paste("https://www.twitter.com/", dfPersons$data.twitterHandle[i_person], sep='')
    twitter_profile_banner_url <- this_pass_tweets$profile_banner_url[i]
    twitter_profile_image_url <- this_pass_tweets$profile_image_url[i]
    
    # get person from the hub
    person <- clessnhub::get_item('persons', dfPersons$key[i_person])
    if (!is.null(person)) {
      person_data <- person$data
      person_metadata <- person$metadata
      
      person$data$twitterName <- twitter_name
      person$data$twitterID <- twitter_id
      person$data$twitterLocation <- twitter_location
      person$data$twitterDesc <- twitter_desc
      person$data$twitterAccountProtected <- twitter_account_protected
      person$data$twitterLang <- twitter_account_lang
      person$data$twitterAccountCreatedOn <- twitter_account_created_on
      person$data$twitterAccountVerified <- twitter_account_verified
      person$data$twitterProfileURL <- twitter_profile_url
      person$data$twitterProfileBannerURL <- twitter_profile_banner_url
      person$data$twitterProfileImageURL <- twitter_profile_image_url
      
      if (!is.null(person$data$twitterUpdateTimeStamps)) {
        twitter_update_list <- as.list(strsplit(person$data$twitterUpdateTimeStamps, ',')[[1]])
        latest_twitter_update <- twitter_update_list[length(twitter_update_list)][[1]]
      }
  
      if (is.null(person$data$twitterUpdateTimeStamps) || difftime(Sys.time(),latest_twitter_update,units = "hours") >= 24) {    
        clessnverse::logit(scriptname, paste("updating new twitter data for", person$data$fullName,"(",person$data$twitterHandle,")"), logger)
        person$data$twitterFollowersCount <- if (is.null(person$data$twitterFollowersCount)) twitter_followers_count else paste(person$data$twitterFollowersCount,twitter_followers_count,sep=',')
        person$data$twitterFriendsCount <- if (is.null(person$data$twitterFriendsCount)) twitter_friends_count else paste(person$data$twitterFriendsCount,twitter_friends_count,sep=',')
        person$data$twitterListedCount <- if (is.null(person$data$twitterListedCount)) twitter_listed_count else paste(person$data$twitterListedCount,twitter_listed_count,sep=',')
        person$data$twitterPostsCount <- if (is.null(person$data$twitterPostsCount)) twitter_posts_count else paste(person$data$twitterPostsCount,twitter_posts_count,sep=',')
        person$data$twitterUpdateTimeStamps <- if (is.null(person$data$twitterUpdateTimeStamp)) Sys.time() else paste(person$data$twitterUpdateTimeStamp,Sys.time(),sep=',')
      }
      
      log_data <- paste("updating", person$data$fullName, "with key", person$key, "\n")
      log_data <- paste(log_data, "===============================================================\n")
      log_data <- paste(log_data, person$key, person$type, person$schema,"\n")
      log_data <- paste(log_data, paste(names(person$metadata), collapse = " * "),"\n")
      log_data <- paste(log_data, paste(person$metadata, collapse = " * "), "\n")
      log_data <- paste(log_data, paste(names(person$data), collapse = " * "),"\n")
      log_data <- paste(log_data, paste(person$data, collapse = " * "))
      log_metadata <- paste("Local time", Sys.time())
      clessnverse::logit(scriptname, paste("pushing twitter data for", person$data$fullName,"(",person$data$twitterHandle,") to hub"), logger)
      clessnhub::edit_item('persons', key = person$key, type = person$type, schema = person$schema, metadata = person$metadata, data = person$data)
    }
  
    
  } #for (i_person in 1:nrow(dfPersons))
  
  
  clessnverse::logclose(logger)
}


tryCatch( 
  {
    installPackages()
    library(dplyr)
    
    if (!exists("scriptname")) scriptname <- "tweets-and-friends.R"
    if (!exists("logger") || is.null(logger) || logger == 0) logger <- clessnverse::loginit(scriptname, c("file","hub"), Sys.getenv("LOG_PATH"))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)
    
    main(scriptname, logger)
  },
  
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=''), logger)
    print(e)
  },
  
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
  }
)