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


is.empty.list <- function(list) {
  return(length(list) == 0) 
}

is.retweet <- function(tweet) {
  return <- tweet$type == "retweet"
}

is.quote <- function(tweet) {
  return <- tweet$type == "quote"
}

is.empty.list <- function(list) {
  return(length(list) == 0) 
}

is.na.char <- function(x) {
  return (x=="NA")
}

processCommandLineOptions <- function() {

  option_list = list(
    optparse::make_option(c("-o", "--log_output"), type="character", default="file,console",
                          help="where to output the logs [default= %default]", metavar="character"),
    optparse::make_option(c("-m", "--max_timeline"), type="numeric", default=100,
                          help="max number of tweets to get in a user timeline [default= %default]", metavar="numeric"),
    optparse::make_option(c("-t", "--type"), type="character", default="all",
                          help="the record type of the persons records to select in hub2 [default= %default]", metavar="character"),
    optparse::make_option(c("-s", "--schema"), type="character", default="all",
                          help="the schema of the persons records to select in hub2 [default= %default]", metavar="character"),
    optparse::make_option(c("-f", "--filter"), type="character", default="all",
                          help="additional django filter to select records from the persons table in hub2 [default= %default]", metavar="character")
  )
  
  opt_parser = optparse::OptionParser(option_list=option_list)
  opt = optparse::parse_args(opt_parser)
  
  return(opt)
}
  


getTweets <- function(handle, key, opt, token, scriptname, logger) {
  
  if (is.na(handle) || is.null(handle) || nchar(handle) == 0) {
    clessnverse::logit(scriptname, paste("twitter handle is NA exiting..."), logger)
    return()
  }

  # get person from the hub (to keep schema intact) and harvest the last twitter scraping we did for him/her
  person <- clessnhub::get_item('persons', key)
  
  env <- new.env()
  env$dfTweets <- data.frame()
 
  clessnverse::logit(scriptname, paste("checking if there are already tweets in the hub for", person$data$fullName, "(", handle, ")"), logger)
  #myfilter <- clessnhub::create_filter(metadata = list("twitterHandle"=handle))
  #tryCatch(
  #  {env$dfTweets <- clessnhub::get_items('tweets', filter = myfilter, max_pages = 1, download_data = FALSE)},
  #  error= function(e) {
  #    clessnverse::logit(scriptname, paste("Something wrong occured when talking to the HUB.  Waiting 120 seconds.", e), logger)
  #    Sys.sleep(120)
  #    clessnverse::logit(scriptname, paste("Trying again", e), logger)
  #    env$dfTweets <- clessnhub::get_items('tweets', filter = myfilter, max_pages = 1, download_data = FALSE)
  #  },
  #  finally={}
  #)
  #if ( !is.null(env$dfTweets) && nrow(env$dfTweets) > 0 ) clessnverse::logit(scriptname, paste("found tweets in the hub for", handle), logger) else clessnverse::logit(scriptname, paste("no tweets found in the hub for", handle), logger)
  if ( !is.null(person$metadata$twitterAccountHasBeenScraped) && !is.na(person$metadata$twitterAccountHasBeenScraped) && (person$metadata$twitterAccountHasBeenScraped == "1" || person$metadata$twitterAccountHasBeenScraped == 1) ) clessnverse::logit(scriptname, paste(handle, "has been scraped before"), logger) else clessnverse::logit(scriptname, paste(handle, "has not been scraped before"), logger)
  

  if (!is.null(person$data$twitterUpdateDateStamps) && !is.na(person$data$twitterUpdateDateStamps) && nchar(person$data$twitterUpdateDateStamps) > 0) {
    twitter_update_date_list <- as.list(strsplit(person$data$twitterUpdateDateStamps, ',')[[1]])
    twitter_update_time_list <- as.list(strsplit(person$data$twitterUpdateTimeStamps, ',')[[1]])
    latest_twitter_update <- paste(twitter_update_date_list[length(twitter_update_date_list)][[1]], twitter_update_time_list[length(twitter_update_time_list)][[1]]) 
  } else {
    latest_twitter_update <- "2000-01-01 00:00 UTC"
  }
  
  latest_twitter_update <- as.POSIXct(latest_twitter_update, tz="UTC")

  #if (handle %in% dfTweets$metadata.twitterHandle) {
  env$newtweets <- data.frame()
    
  #if (!is.null(env$dfTweets) && !is.null(person$metadata$twitterAccountHasBeenScraped) && !is.na(person$metadata$twitterAccountHasBeenScraped) && (person$metadata$twitterAccountHasBeenScraped == "1" || person$metadata$twitterAccountHasBeenScraped == 1)) {
  if (!is.null(person$metadata$twitterAccountHasBeenScraped) && !is.na(person$metadata$twitterAccountHasBeenScraped) && (person$metadata$twitterAccountHasBeenScraped == "1" || person$metadata$twitterAccountHasBeenScraped == 1)) {
    # we already scraped the tweets of this person => let's get only the last few tweets
    clessnverse::logit(scriptname, paste("getting new tweets from",person$data$fullName,"(",handle,")"), logger)
    tryCatch(
      { env$newtweets <- rtweet::search_tweets(q = paste("from:",handle,sep=''), retryonratelimit=T, token = token) },
      error = function(e) {
        clessnverse::logit(scriptname, paste("Error getting new tweets using search_tweets function from package rtweet.  Sleeping 120 seconds.",e), logger)
        Sys.sleep(120)
        clessnverse::logit(scriptname, "Trying again", logger)
        tryCatch (
          { env$newtweets <- rtweet::search_tweets(q = paste("from:",handle,sep=''), retryonratelimit=T, token = token) },
          error = function(e) {
            clessnverse::logit(scriptname, paste("Error getting new tweets using search_tweets function from package rtweet.  Skipping this user",e), logger)
            env$newtweets <- data.frame()
          },
          finally = {}
        )
      },
      finally = {}
    )
  } else {
    # we never scraped the tweets of this person => let's get the full timeline
    clessnverse::logit(scriptname, paste("getting full twitter timeline from",person$data$fullName,"(",handle,") witn n =", opt$max_timeline), logger)
    tryCatch (
      { env$newtweets <- rtweet::get_timeline(handle,n=opt$max_timeline, token = token) },
      error = function(e) {
        clessnverse::logit(scriptname, paste("Error getting full timeline using get_timeline function from package rtweet.  Sleeping 120 seconds.",e), logger)
        Sys.sleep(120)
        clessnverse::logit(scriptname, "Trying again", logger)
        tryCatch (
          { env$newtweets <- rtweet::get_timeline(handle,n=opt$max_timeline, token = token) },
          error = function(e) {
            clessnverse::logit(scriptname, paste("Error getting full timeline using get_timeline function from package rtweet.  Skipping this user",e), logger)
            env$newtweets <- data.frame()
          },
          finally = {}
        )
      },
      
      finally = {}
    )
  }
  
  if (nrow(env$newtweets) > 0) {
    data.type <- dplyr::case_when(env$newtweets$is_retweet == TRUE ~ rep("retweet", nrow(env$newtweets)), TRUE ~ NA_character_)
    data.type1 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
    
    data.type = dplyr::case_when(env$newtweets$is_quote == TRUE ~ rep("quote", nrow(env$newtweets)),TRUE ~ NA_character_)
    data.type2 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
    
    data.type3 = data.frame(index = rep(as.character(1:nrow(env$newtweets))), data.type = rep("tweet", nrow(env$newtweets)))
    
    df <- left_join(data.type1, data.type2, by="index") %>% mutate(column=ifelse(!is.na(data.type1$data.type), data.type1$data.type, data.type2$data.type))
    df <- left_join(df, data.type3, by="index") %>% mutate(data.type=ifelse(!is.na(df$column), df$column, data.type3$data.type)) %>% select(data.type)
    
    current_time <- as.POSIXct(Sys.time(), format="%Y-%m-%d %H:%M", tz="EDT")
    attr(current_time, "tzone") <- "UTC"
    
    time_diff <- difftime(current_time, latest_twitter_update)
    
    # construct a datafra,e corresponding to the datastructure we want to write to the hub
    df_to_commit <- data.frame(key = paste("t", env$newtweets$status_id, sep='') %>% gsub("tt","t",.),
                               type = df$data.type,
                               schema = rep("v2",nrow(env$newtweets)),
                               data.tweetID = env$newtweets$status_id,
                               data.creationDate = format(env$newtweets$created_at, "%Y-%m-%d"),
                               data.creationTime = format(env$newtweets$created_at, "%H:%M %Z"),
                               data.screenName = env$newtweets$screen_name,
                               data.personKey = rep(person$key, nrow(env$newtweets)),
                               
                               data.text = env$newtweets$text,
                               data.source = env$newtweets$source,

                               data.likeCount = env$newtweets$favorite_count,
                               data.retweetCount = env$newtweets$retweet_count,
                               data.quoteCount = env$newtweets$quote_count,
                               
                               data.hashtags = sapply(env$newtweets$hashtags[!!length(env$newtweets$hashtags)], toString),
                               data.urls = sapply(env$newtweets$urls_expanded_url[!!length(env$newtweets$urls_expanded_url)], toString),
                               data.mediaUrls = sapply(env$newtweets$media_expanded_url[!!length(env$newtweets$media_expanded_url)], toString),
                               data.mentions = sapply(env$newtweets$mentions_screen_name[!!length(env$newtweets$mentions_screen_name)], toString),
                               
                               data.originalTweetID = rep(NA, nrow(env$newtweets)),
                               data.originalTweetCreationDate = rep(NA, nrow(env$newtweets)),
                               data.originalTweetCreationTime = rep(NA, nrow(env$newtweets)),
                               data.originalTweetText = rep(NA, nrow(env$newtweets)),
                               data.originalTweetFrom = rep(NA, nrow(env$newtweets)),
                               
                               metadata.tweetUrl = env$newtweets$status_url,
                               metadata.tweetLang = env$newtweets$lang,
                               metadata.personType = rep(person$type , nrow(env$newtweets)),
                               metadata.lastUpdatedOn = format(rep(current_time, nrow(env$newtweets)),"%Y-%m-%d"),
                               metadata.lastUpdatedAt = format(rep(current_time, nrow(env$newtweets)),"%H:%M %Z"),
                               metadata.twitterHandle = rep(handle, nrow(env$newtweets))
    ) 
    
    df_to_commit$data.originalTweetID[which(env$newtweets$is_quote==TRUE)] <- env$newtweets$quoted_status_id[env$newtweets$is_quote==TRUE]
    df_to_commit$data.originalTweetCreationDate[which(env$newtweets$is_quote==TRUE)] <- format(env$newtweets$quoted_created_at[env$newtweets$is_quote==TRUE], "%Y-%m-%d")
    df_to_commit$data.originalTweetCreationTime[which(env$newtweets$is_quote==TRUE)] <- format(env$newtweets$quoted_created_at[env$newtweets$is_quote==TRUE], "%H:%M %Z")
    df_to_commit$data.originalTweetText[which(env$newtweets$is_quote==TRUE)] <- env$newtweets$quoted_text[env$newtweets$is_quote==TRUE]
    df_to_commit$data.originalTweetFrom[which(env$newtweets$is_quote==TRUE)] <- env$newtweets$quoted_screen_name[env$newtweets$is_quote==TRUE]
    
    df_to_commit$data.originalTweetID[which(env$newtweets$is_retweet==TRUE)] = env$newtweets$retweet_status_id[env$newtweets$is_retweet==TRUE]
    df_to_commit$data.originalTweetCreationDate[which(env$newtweets$is_retweet==TRUE)] = format(env$newtweets$retweet_created_at[env$newtweets$is_retweet==TRUE], "%Y-%m-%d")
    df_to_commit$data.originalTweetCreationTime[which(env$newtweets$is_retweet==TRUE)] = format(env$newtweets$retweet_created_at[env$newtweets$is_retweet==TRUE], "%H:%M %Z")
    df_to_commit$data.originalTweetText[which(env$newtweets$is_retweet==TRUE)] = env$newtweets$retweet_text[env$newtweets$is_retweet==TRUE]
    df_to_commit$data.originalTweetFrom[which(env$newtweets$is_retweet==TRUE)] = env$newtweets$retweet_screen_name[env$newtweets$is_retweet==TRUE]
    
    df_to_commit <- df_to_commit %>% filter(difftime(paste(df_to_commit$data.creationDate, df_to_commit$data.creationTime), latest_twitter_update, units = "hours") >= 24)

    clessnverse::logit(scriptname, paste("committing",nrow(df_to_commit),"tweets to HUB"), logger)
    
    if (nrow(df_to_commit) == 0) return()
    
    # write the constructed df to the hub 
    for (i in 1:nrow(df_to_commit)) {
     
      t_key  <- paste("t", df_to_commit$key[i], sep='') %>% gsub("tt","t",.)
      type   <- df_to_commit$type[i]
      schema <- df_to_commit$schema[i]
      
      data_to_commit <- as.list(df_to_commit[i,which(grepl("^data.",names(df_to_commit[i,])))])
      names(data_to_commit) <- gsub("^data.", "", names(data_to_commit))
      metadata_to_commit <- as.list(df_to_commit[i,which(grepl("^metadata.",names(df_to_commit[i,])))])
      names(metadata_to_commit) <- gsub("^metadata.", "", names(metadata_to_commit))
      
      data_to_commit[sapply(data_to_commit,is.null)] <- NA_character_ 
      metadata_to_commit[sapply(metadata_to_commit,is.null)] <- NA_character_
      data_to_commit[sapply(data_to_commit,is.na)] <- NA_character_ 
      metadata_to_commit[sapply(metadata_to_commit,is.na)] <- NA_character_
      data_to_commit[sapply(data_to_commit,is.na.char)] <- NA_character_ 
      metadata_to_commit[sapply(metadata_to_commit,is.na.char)] <- NA_character_
      
      #clessnverse::logit(scriptname, t_key, logger)
      tweet_commit_type <- "added"

      tryCatch(
        {
          tweet_commit_type <- "modified"
          clessnhub::edit_item('tweets', key = t_key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
        },

        error = function(e) {
          tryCatch(
            {
              tweet_commit_type <- "added"
              clessnhub::create_item('tweets', key = t_key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
            },
            
            error = function(e) {
              # clessnverse::logit(scriptname, paste("Something went wrong when adding a new item to the hub:", e), logger)
              # clessnverse::logit(scriptname, "waiting 20 seconds", logger)
              # Sys.sleep(20)
              # clessnverse::logit(scriptname, "trying again", logger)
              # clessnhub::create_item('tweets', key = t_key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
            },
            
            finally = {}
          )
        },

        finally={
          clessnverse::logit(scriptname, paste(tweet_commit_type, "tweet", t_key), logger)
        }
      )
     
    }#for (i in 1:nrow(df_to_commit))
  
    # update the person's twitter info in the hub from the last tweets collected
    i <- nrow(df_to_commit)
      
    person$metadata$twitterAccountHasBeenScraped <- "1"
    
    person$data$twitterName <- env$newtweets$name[i]
    person$data$twitterID <- env$newtweets$user_id[i]
    person$data$twitterLocation <- env$newtweets$location[i]
    person$data$twitterDesc <- env$newtweets$description[i]
    person$data$twitterAccountProtected <- if (env$newtweets$protected[i]) 1 else 0
    person$data$twitterLang <- env$newtweets$account_lang[i]
    person$data$twitterAccountCreatedOn <- format(env$newtweets$account_created_at[i],"%Y-%m-%d")
    person$data$twitterAccountCreatedAt <- format(env$newtweets$account_created_at[i],"%H:%M %Z")
    person$data$twitterAccountVerified <- if (env$newtweets$verified[i]) 1 else 0
    person$data$twitterProfileURL <- paste("https://www.twitter.com/", handle, sep='')
    person$data$twitterProfileBannerURL <- env$newtweets$profile_banner_url[i]
    person$data$twitterProfileImageURL <- env$newtweets$profile_image_url[i]
    
    if (is.null(person$data$twitterUpdateDateStamps) || nchar(person$data$twitterUpdateDateStamps) == 0 || difftime(Sys.time(),latest_twitter_update,units = "hours") >= 24) {
      clessnverse::logit(scriptname, paste("updating new twitter data for", person$data$fullName,"(",handle,")"), logger)
      person$data$twitterFollowersCount <- if (is.null(person$data$twitterFollowersCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) env$newtweets$followers_count[i] else paste(person$data$twitterFollowersCount,env$newtweets$followers_count[i],sep=', ')
      person$data$twitterFriendsCount <- if (is.null(person$data$twitterFriendsCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) env$newtweets$friends_count[i] else paste(person$data$twitterFriendsCount,env$newtweets$friends_count[i],sep=', ')
      person$data$twitterListedCount <- if (is.null(person$data$twitterListedCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) env$newtweets$listed_count[i] else paste(person$data$twitterListedCount,env$newtweets$listed_count[i],sep=', ')
      person$data$twitterPostsCount <- if (is.null(person$data$twitterPostsCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) env$newtweets$statuses_count[i] else paste(person$data$twitterPostsCount,env$newtweets$statuses_count[i],sep=', ')
      
      current_time <- as.POSIXct(Sys.time(), format="%Y-%m-%d %H:%M:%S", tz="EDT")
      attr(current_time, "tzone") <- "UTC"
        
      person$data$twitterUpdateDateStamps <- if (is.null(person$data$twitterUpdateDateStamps) || nchar(person$data$twitterUpdateDateStamps) == 0) format(current_time,"%Y-%m-%d") else paste(person$data$twitterUpdateDateStamps,format(current_time,"%Y-%m-%d"),sep=', ')
      person$data$twitterUpdateTimeStamps <- if (is.null(person$data$twitterUpdateTimeStamps) || nchar(person$data$twitterUpdateTimeStamps) == 0) format(current_time,"%H:%M %Z") else paste(person$data$twitterUpdateTimeStamps,format(current_time,"%H:%M %Z"),sep=', ')
    }
    
    
    person$data[sapply(person$data,is.null)] <- NA_character_
    person$metadata[sapply(person$metadata,is.null)] <- NA_character_
    
    person$data[sapply(person$data,is.empty.list)] <- NA_character_
    person$metadata[sapply(person$metadata,is.empty.list)] <- NA_character_
    
    person$data[sapply(person$data,is.na)] <- NA_character_ 
    person$metadata[sapply(person$metadata,is.na)] <- NA_character_
    
    person$data[sapply(person$data,is.na.char)] <- NA_character_ 
    person$metadata[sapply(person$metadata,is.na.char)] <- NA_character_
    
    clessnverse::logit(scriptname, paste("pushing twitter data for", person$data$fullName,"(",handle,") to hub"), logger)
    clessnhub::edit_item('persons', key = key, type = person$type, schema = person$schema, metadata = person$metadata, data = person$data)
    
  } else {
    clessnverse::logit(scriptname, paste("no tweet found on Twitter for", person$data$fullName,"(",handle,") to hub"), logger)
  }
  #if (nrow(env$newtweets) > 0)
  
  rm(env)
  
} #</getTweets>




###############################################################################
########################               MAIN              ######################
###############################################################################

main <- function(opt, scriptname, logger) {
  
  # load twitter token rds file
  if (file.exists(".rtweet_token.rds")) token <- readRDS(".rtweet_token.rds")
  if (file.exists("~/.rtweet_token.rds")) token <- readRDS("~/.rtweet_token.rds")
  
  # get all persons dataframes
  clessnverse::logit(scriptname, "getting persons", logger)

  if (opt$type == "all" || nchar(opt$type) == 0) opt$type <- NULL
  if (opt$schema == "all" || nchar(opt$schema) == 0) opt$schema <- NULL
  if (opt$filter == "all" || nchar(opt$filter) == 0) opt$filter <- NULL

  if (!is.null(opt$filter)) {
    metadata_filter <- eval(parse(text=opt$filter))
    metadata_filter <- metadata_filter[which(grepl("^metadata.", names(metadata_filter)))]
    names(metadata_filter) <- gsub("^metadata.", "", names(metadata_filter))

    data_filter <- eval(parse(text=opt$filter))
    data_filter <- data_filter[which(grepl("^data.", names(data_filter)))]
    names(data_filter) <- gsub("^data.", "", names(data_filter))
  } else {
    metadata_filter <- NULL
    data_filter <- NULL
  }

  filter <- clessnhub::create_filter(type=opt$type, schema=opt$schema, metadata=metadata_filter, data=data_filter)

  dfPersons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)

  if (!is.null(dfPersons) && nrow(dfPersons) > 0) {

    clessnverse::logit(scriptname, paste(nrow(dfPersons),"persons were fetched from hub2"), logger)

    if (TRUE %in% grepl("data.twitterHandleFR", names(dfPersons)) && TRUE %in% grepl("data.twitterHandleEN", names(dfPersons))) {
      dfPersons <- dfPersons %>%
          filter(!is.na(data.twitterHandleFR) & !is.na(data.twitterHandleEN)) %>% 
          dplyr::select(key, type, data.fullName, data.twitterHandleFR, data.twitterHandleEN)
    } else {
      dfPersons <- dfPersons %>%
        filter(!is.na(data.twitterHandle)) %>% 
        dplyr::select(key, type, data.fullName, data.twitterHandle)
    }

    clessnverse::logit(scriptname, paste(nrow(dfPersons),"persons were kept with a twitter handle from hub2"), logger)
  
    # Define population subset
    index <- rep(1:nrow(dfPersons))
    
    if (nrow(dfPersons) > 0) {
      clessnverse::logit(scriptname, paste("start looping through",length(index),"persons"), logger)
    } else {
      clessnverse::logit(scriptname, "no persons with a twitter handle were found in hub2", logger)
      return(2)
    }
    
    for (i_person in index) {
      clessnverse::logit(scriptname, paste("scraping count:", which(index == i_person), "of", length(index), ":", dfPersons$data.fullName[i_person]), logger)
      
      if (dfPersons$type[i_person] != "political_party") {
        #clessnverse::logit(scriptname, paste("getting tweets from", dfPersons$data.twitterHandle[i_person]), logger)
        getTweets(handle = dfPersons$data.twitterHandle[i_person], key = dfPersons$key[i_person],
                  opt = opt, token = token, scriptname = scriptname, logger = logger)
      } else {
        if (!is.na(dfPersons$data.twitterHandleEN[i_person])) {
          #clessnverse::logit(scriptname, paste("getting tweets from", dfPersons$data.twitterHandleEN[i_person]), logger)
          getTweets(handle = dfPersons$data.twitterHandleEN[i_person], key = dfPersons$key[i_person],
                    opt = opt, token = token, scriptname = scriptname, logger = logger)
        }
        
        if (!is.na(dfPersons$data.twitterHandleFR[i_person])) {
          #clessnverse::logit(scriptname, paste("getting tweets from", dfPersons$data.twitterHandleFR[i_person]), logger)
          getTweets(handle = dfPersons$data.twitterHandleFR[i_person], key = dfPersons$key[i_person],
                    opt = opt, token = token, scriptname = scriptname, logger = logger)
        }
      }
    } #for (i_person in 1:nrow(dfPersons))
  } else {
    clessnverse::logit(scriptname, "no persons matching filter criteria were found in hub2", logger)
  }
} #</main>





tryCatch( 
  {
    installPackages()
    library(dplyr)
    #opt <- opt <- list(max_timeline = 3200, log_output = "file,console", population = "all")
    #opt <- opt <- list(max_timeline = 3200, log_output = "file,console", population = "politicians")
    #opt <- opt <- list(max_timeline = 200, log_output = "file,console", population = "medias")
    #opt <- opt <- list(max_timeline = 100, log_output = "console", population = "small_sample")
    #opt <- list(max_timeline = 100, log_output = "file", type = "mp", schema = "all", filter = 'list(metadata.institution="National Assembly of Quebec")')
    #opt <- list(max_timeline = 100, log_output = "file", type = "all", schema = "all", filter = 'all')
    #opt <- list(max_timeline = 100, log_output = "file", type = "political_party", schema = "all", filter = 'all')
    
    if (!exists("opt")) {
      opt <- processCommandLineOptions()
    }
    
    if (exists("logger")) rm(logger)
    if (!exists("scriptname")) scriptname <<- paste("twitter_blender_",opt$type,sep='')
    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname, "starting with options", paste(names(opt), "=", opt, collapse = " ")), logger)
    
    # login to the hub
    clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    clessnverse::logit(scriptname, "connecting to hub", logger)
    
    main(opt, scriptname, logger)
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
