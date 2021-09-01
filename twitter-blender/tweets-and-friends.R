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
    optparse::make_option(c("-t", "--max_timeline"), type="numeric", default=100,
                          help="max number of tweets to get in a user timeline [default= %default]", metavar="numeric"),
    optparse::make_option(c("-p", "--population"), type="character", default="small_sample",
                          help="population to scrape [default= %default]", metavar="character")
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
  dfTweets <- data.frame()
 
  clessnverse::logit(scriptname, paste("checking if there are already tweets in the hub for", person$data$fullName, "(", handle, ")"), logger)
  myfilter <- clessnhub::create_filter(metadata = list("twitterHandle"=handle))
  tryCatch(
    {dfTweets <- clessnhub::get_items('tweets', filter = myfilter, max_pages = 1, download_data = FALSE)},
    error= function(e) {
      clessnverse::logit(scriptname, paste("Something wrong occured when talking to the HUB.  Waiting 120 seconds.", e), logger)
      Sys.sleep(120)
      clessnverse::logit(scriptname, paste("Trying again", e), logger)
      dfTweets <- clessnhub::get_items('tweets', filter = myfilter, max_pages = 1, download_data = FALSE)
    },
    finally={}
  )

  if ( !is.null(dfTweets) && nrow(dfTweets) > 0 ) clessnverse::logit(scriptname, paste("found tweets in the hub for", handle), logger) else clessnverse::logit(scriptname, paste("no tweets found in the hub for", handle), logger)

  if (!is.null(person$data$twitterUpdateDateStamps) && !is.na(person$data$twitterUpdateDateStamps) && nchar(person$data$twitterUpdateDateStamps) > 0) {
    twitter_update_date_list <- as.list(strsplit(person$data$twitterUpdateDateStamps, ',')[[1]])
    twitter_update_time_list <- as.list(strsplit(person$data$twitterUpdateTimeStamps, ',')[[1]])
    latest_twitter_update <- paste(twitter_update_date_list[length(twitter_update_date_list)][[1]], twitter_update_time_list[length(twitter_update_time_list)][[1]]) 
  } else {
    latest_twitter_update <- "2000-01-01 00:00 UTC"
  }
  
  latest_twitter_update <- as.POSIXct(latest_twitter_update, tz="UTC")

  #if (handle %in% dfTweets$metadata.twitterHandle) {
  this_pass_tweets <- data.frame()
    
  if (!is.null(dfTweets) && !is.null(person$metadata$twitterAccountHasBeenScraped) && !is.na(person$metadata$twitterAccountHasBeenScraped) && (person$metadata$twitterAccountHasBeenScraped == "1" || person$metadata$twitterAccountHasBeenScraped == 1)) {
    # we already scraped the tweets of this person => let's get only the last few tweets
    clessnverse::logit(scriptname, paste("getting new tweets from",person$data$fullName,"(",handle,")"), logger)
    tryCatch(
      { this_pass_tweets <- rtweet::search_tweets(q = paste("from:",handle,sep=''), retryonratelimit=T, token = token) },
      error = function(e) {
        clessnverse::logit(scriptname, paste("Error getting new tweets using search_tweets function from package rtweet.  Sleeping 120 seconds.",e), logger)
        Sys.sleep(120)
        clessnverse::logit(scriptname, "Trying again", logger)
        tryCatch (
          { this_pass_tweets <- rtweet::search_tweets(q = paste("from:",handle,sep=''), retryonratelimit=T, token = token) },
          error = function(e) {
            clessnverse::logit(scriptname, paste("Error getting new tweets using search_tweets function from package rtweet.  Skipping this user",e), logger)
            this_pass_tweets <- data.frame()
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
      { this_pass_tweets <- rtweet::get_timeline(handle,n=opt$max_timeline, token = token) },
      error = function(e) {
        clessnverse::logit(scriptname, paste("Error getting full timeline using get_timeline function from package rtweet.  Sleeping 120 seconds.",e), logger)
        Sys.sleep(120)
        clessnverse::logit(scriptname, "Trying again", logger)
        tryCatch (
          { this_pass_tweets <- rtweet::get_timeline(handle,n=opt$max_timeline, token = token) },
          error = function(e) {
            clessnverse::logit(scriptname, paste("Error getting full timeline using get_timeline function from package rtweet.  Skipping this user",e), logger)
            this_pass_tweets <- data.frame()
          },
          finally = {}
        )
      },
      
      finally = {}
    )
  }
  
  if (nrow(this_pass_tweets) > 0) {
    clessnverse::logit(scriptname, "building tweets types vectors", logger)
    data.type <- dplyr::case_when(this_pass_tweets$is_retweet == TRUE ~ rep("retweet", nrow(this_pass_tweets)), TRUE ~ NA_character_)
    data.type1 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
    
    data.type = dplyr::case_when(this_pass_tweets$is_quote == TRUE ~ rep("quote", nrow(this_pass_tweets)),TRUE ~ NA_character_)
    data.type2 <- data.frame(index=row.names(as.data.frame(data.type)),as.data.frame(data.type))
    
    data.type3 = data.frame(index = rep(as.character(1:nrow(this_pass_tweets))), data.type = rep("tweet", nrow(this_pass_tweets)))
    
    df <- left_join(data.type1, data.type2, by="index") %>% mutate(column=ifelse(!is.na(data.type1$data.type), data.type1$data.type, data.type2$data.type))
    df <- left_join(df, data.type3, by="index") %>% mutate(data.type=ifelse(!is.na(df$column), df$column, data.type3$data.type)) %>% select(data.type)
    
    current_time <- as.POSIXct(Sys.time(), format="%Y-%m-%d %H:%M", tz="EDT")
    attr(current_time, "tzone") <- "UTC"
    
    time_diff <- difftime(current_time, latest_twitter_update)
    
    # construct a datafra,e corresponding to the datastructure we want to write to the hub
    clessnverse::logit(scriptname, "building tweets data frame to commit 1", logger)
    df_to_commit <- data.frame(key = paste("t", this_pass_tweets$status_id, sep='') %>% gsub("tt","t",.),
                               type = df$data.type,
                               schema = rep("v2",nrow(this_pass_tweets)),
                               data.tweetID = this_pass_tweets$status_id,
                               data.creationDate = format(this_pass_tweets$created_at, "%Y-%m-%d"),
                               data.creationTime = format(this_pass_tweets$created_at, "%H:%M %Z"),
                               data.screenName = this_pass_tweets$screen_name,
                               data.personKey = rep(person$key, nrow(this_pass_tweets)),
                               
                               data.text = this_pass_tweets$text,
                               data.source = this_pass_tweets$source,

                               data.likeCount = this_pass_tweets$favorite_count,
                               data.retweetCount = this_pass_tweets$retweet_count,
                               data.quoteCount = this_pass_tweets$quote_count,
                               
                               data.hashtags = sapply(this_pass_tweets$hashtags[!!length(this_pass_tweets$hashtags)], toString),
                               data.urls = sapply(this_pass_tweets$urls_expanded_url[!!length(this_pass_tweets$urls_expanded_url)], toString),
                               data.mediaUrls = sapply(this_pass_tweets$media_expanded_url[!!length(this_pass_tweets$media_expanded_url)], toString),
                               data.mentions = sapply(this_pass_tweets$mentions_screen_name[!!length(this_pass_tweets$mentions_screen_name)], toString),
                               
                               data.originalTweetID = rep(NA, nrow(this_pass_tweets)),
                               data.originalTweetCreationDate = rep(NA, nrow(this_pass_tweets)),
                               data.originalTweetCreationTime = rep(NA, nrow(this_pass_tweets)),
                               data.originalTweetText = rep(NA, nrow(this_pass_tweets)),
                               data.originalTweetFrom = rep(NA, nrow(this_pass_tweets)),
                               
                               metadata.tweetUrl = this_pass_tweets$status_url,
                               metadata.tweetLang = this_pass_tweets$lang,
                               metadata.personType = rep(person$type , nrow(this_pass_tweets)),
                               metadata.lastUpdatedOn = format(rep(current_time, nrow(this_pass_tweets)),"%Y-%m-%d"),
                               metadata.lastUpdatedAt = format(rep(current_time, nrow(this_pass_tweets)),"%H:%M %Z"),
                               metadata.twitterHandle = rep(handle, nrow(this_pass_tweets))
    ) 
    
    clessnverse::logit(scriptname, "building tweets data frame to commit 2", logger)
    
    df_to_commit$data.originalTweetID[which(this_pass_tweets$is_quote==TRUE)] <- this_pass_tweets$quoted_status_id[this_pass_tweets$is_quote==TRUE]
    df_to_commit$data.originalTweetCreationDate[which(this_pass_tweets$is_quote==TRUE)] <- format(this_pass_tweets$quoted_created_at[this_pass_tweets$is_quote==TRUE], "%Y-%m-%d")
    df_to_commit$data.originalTweetCreationTime[which(this_pass_tweets$is_quote==TRUE)] <- format(this_pass_tweets$quoted_created_at[this_pass_tweets$is_quote==TRUE], "%H:%M %Z")
    df_to_commit$data.originalTweetText[which(this_pass_tweets$is_quote==TRUE)] <- this_pass_tweets$quoted_text[this_pass_tweets$is_quote==TRUE]
    df_to_commit$data.originalTweetFrom[which(this_pass_tweets$is_quote==TRUE)] <- this_pass_tweets$quoted_screen_name[this_pass_tweets$is_quote==TRUE]
    
    df_to_commit$data.originalTweetID[which(this_pass_tweets$is_retweet==TRUE)] = this_pass_tweets$retweet_status_id[this_pass_tweets$is_retweet==TRUE]
    df_to_commit$data.originalTweetCreationDate[which(this_pass_tweets$is_retweet==TRUE)] = format(this_pass_tweets$retweet_created_at[this_pass_tweets$is_retweet==TRUE], "%Y-%m-%d")
    df_to_commit$data.originalTweetCreationTime[which(this_pass_tweets$is_retweet==TRUE)] = format(this_pass_tweets$retweet_created_at[this_pass_tweets$is_retweet==TRUE], "%H:%M %Z")
    df_to_commit$data.originalTweetText[which(this_pass_tweets$is_retweet==TRUE)] = this_pass_tweets$retweet_text[this_pass_tweets$is_retweet==TRUE]
    df_to_commit$data.originalTweetFrom[which(this_pass_tweets$is_retweet==TRUE)] = this_pass_tweets$retweet_screen_name[this_pass_tweets$is_retweet==TRUE]
    
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
 
      tryCatch(
        {
          clessnhub::edit_item('tweets', key = t_key, type = type, schema = schema, metadata = metadata_to_commit, data = data_to_commit)
        },

        error = function(e) {
          tryCatch(
            {
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

        finally={}
      )
     
    }#for (i in 1:nrow(df_to_commit))
  
    # update the person's twitter info in the hub from the last tweet collected
    i <- nrow(df_to_commit)

    if ( handle != "@LesVertsCanada" && handle != "@pcc_hq" && handle != "@NPD_QG" && handle != "@parti_liberal" && handle != "@ppopulaireca" ) {
      
      person$metadata$twitterAccountHasBeenScraped <- "1"
      
      person$data$twitterName <- this_pass_tweets$name[i]
      person$data$twitterID <- this_pass_tweets$user_id[i]
      person$data$twitterLocation <- this_pass_tweets$location[i]
      person$data$twitterDesc <- this_pass_tweets$description[i]
      person$data$twitterAccountProtected <- if (this_pass_tweets$protected[i]) 1 else 0
      person$data$twitterLang <- this_pass_tweets$account_lang[i]
      person$data$twitterAccountCreatedOn <- format(this_pass_tweets$account_created_at[i],"%Y-%m-%d")
      person$data$twitterAccountCreatedAt <- format(this_pass_tweets$account_created_at[i],"%H:%M %Z")
      person$data$twitterAccountVerified <- if (this_pass_tweets$verified[i]) 1 else 0
      person$data$twitterProfileURL <- paste("https://www.twitter.com/", handle, sep='')
      person$data$twitterProfileBannerURL <- this_pass_tweets$profile_banner_url[i]
      person$data$twitterProfileImageURL <- this_pass_tweets$profile_image_url[i]
      
      if (is.null(person$data$twitterUpdateDateStamps) || nchar(person$data$twitterUpdateDateStamps) == 0 || difftime(Sys.time(),latest_twitter_update,units = "hours") >= 24) {
        clessnverse::logit(scriptname, paste("updating new twitter data for", person$data$fullName,"(",handle,")"), logger)
        person$data$twitterFollowersCount <- if (is.null(person$data$twitterFollowersCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) this_pass_tweets$followers_count[i] else paste(person$data$twitterFollowersCount,this_pass_tweets$followers_count[i],sep=',')
        person$data$twitterFriendsCount <- if (is.null(person$data$twitterFriendsCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) this_pass_tweets$friends_count[i] else paste(person$data$twitterFriendsCount,this_pass_tweets$friends_count[i],sep=',')
        person$data$twitterListedCount <- if (is.null(person$data$twitterListedCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) this_pass_tweets$listed_count[i] else paste(person$data$twitterListedCount,this_pass_tweets$listed_count[i],sep=',')
        person$data$twitterPostsCount <- if (is.null(person$data$twitterPostsCount) || nchar(person$data$twitterUpdateTimeStamps) == 0) this_pass_tweets$statuses_count[i] else paste(person$data$twitterPostsCount,this_pass_tweets$statuses_count[i],sep=',')
        
        current_time <- as.POSIXct(Sys.time(), format="%Y-%m-%d %H:%M:%S", tz="EDT")
        attr(current_time, "tzone") <- "UTC"
          
        person$data$twitterUpdateDateStamps <- if (is.null(person$data$twitterUpdateDateStamps) || nchar(person$data$twitterUpdateDateStamps) == 0) format(current_time,"%Y-%m-%d") else paste(person$data$twitterUpdateDateStamps,format(current_time,"%Y-%m-%d"),sep=',')
        person$data$twitterUpdateTimeStamps <- if (is.null(person$data$twitterUpdateTimeStamps) || nchar(person$data$twitterUpdateTimeStamps) == 0) format(current_time,"%H:%M %Z") else paste(person$data$twitterUpdateTimeStamps,format(current_time,"%H:%M %Z"),sep=',')
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
    } #if (handle != "@LesVertsCanada" && handle != "@pcc_hq" && handle != "@NPD_QG" && handle != "@parti_liberal" && handle != "@ppopulaireca") 
    
  } else {
    clessnverse::logit(scriptname, paste("no tweet found on Twitter for", person$data$fullName,"(",handle,") to hub"), logger)
  }
  #if (nrow(this_pass_tweets) > 0)
  
} #</getTweets>




###############################################################################
########################               MAIN              ######################
###############################################################################

main <- function(opt, scriptname, logger) {
  
  # load twitter token rds file
  if (file.exists(".rtweet_token.rds")) token <- readRDS(".rtweet_token.rds")
  if (file.exists("~/.rtweet_token.rds")) token <- readRDS("~/.rtweet_token.rds")
  
  # get all persons dataframes
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
  
  dfPersons <- dfMPs %>% bind_rows(dfCandidates) %>% bind_rows(dfJournalists) %>% bind_rows(dfMedias) %>%  
    filter(!is.na(data.twitterHandle)) %>%
    bind_rows(dfParties) %>%
    dplyr::select(key, type, data.fullName, data.twitterHandle, data.twitterHandleFR, data.twitterHandleEN)

  # Define population subset
  all_index <- rep(1:nrow(dfPersons))
  candidates_index <- which(dfPersons$key %in% dfCandidates$key)
  mps_index <- which(dfPersons$key %in% dfMPs$key)
  journalists_index <- which(dfPersons$key %in% dfJournalists$key)
  medias_index <- which(dfPersons$key %in% dfMedias$key)
  parties_index <- which(dfPersons$key %in% dfParties$key)
  
  leaders_index <-  which(dfPersons$key == "72773" | dfPersons$key == "58733" | dfPersons$key == "71588" |
                            dfPersons$key == "00eefdd89b55ced5b61f7b82297e5787" | dfPersons$key == "bea0eb58fd0768bc91c0a8cb6ac52cd5" |
                            dfPersons$key == "104669")
  
  politicians_index <- c(mps_index,candidates_index)
  
  sample_index <- which(dfPersons$key == "72773" | dfPersons$key == "58733" | dfPersons$key == "71588" |
                          dfPersons$key == "00eefdd89b55ced5b61f7b82297e5787" | dfPersons$key == "bea0eb58fd0768bc91c0a8cb6ac52cd5" |
                          dfPersons$key == "104669" | dfPersons$key == "376927648" | dfPersons$key == "4131" | dfPersons$key == "12223" |
                          dfPersons$key == "999945387" | dfPersons$key == "999956328" | dfPersons$key == "99998856744")
  
  index <- if (opt$population == "all") all_index else 
           if (opt$population == "medias") medias_index else
           if (opt$population == "mps") mps_index else
           if (opt$population == "candidates") candidates_index else
           if (opt$population == "leaders") leaders_index else 
           if (opt$population == "politicians") politicians_index else
           if (opt$population == "journalists") journalists_index else
           if (opt$population == "parties") parties_index else
           if (opt$population == "small_sample") sample_index
  
  clessnverse::logit(scriptname, paste("start looping through",length(index),"persons"), logger)
  
  for (i_person in index) {
    clessnverse::logit(scriptname, paste("scraping count:", which(index == i_person), "of", length(index)), logger)
    
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
} #</main>





tryCatch( 
  {
    installPackages()
    library(dplyr)

    #opt <- opt <- list(max_timeline = 3200, log_output = "file,console", population = "all")
    #opt <- opt <- list(max_timeline = 3200, log_output = "file,console", population = "politicians")
    #opt <- opt <- list(max_timeline = 200, log_output = "file,console", population = "medias")
    #opt <- opt <- list(max_timeline = 100, log_output = "console", population = "small_sample")
    
    if (!exists("opt")) {
      opt <- processCommandLineOptions()
    }
    
    if (exists("logger")) rm(logger)
    if (!exists("scriptname")) scriptname <<- paste("tweets-and-friends-",opt$population,sep='')
    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname, "starting with options", paste(opt, collapse = " ")), logger)
    
    valid_population <- c("all", "medias", "mps", "candidates", "leaders", "politicians", "parties", "journalists", "small_sample")
    
    if (!opt$population %in% valid_population) stop (paste("invalid population, possible values are", paste(valid_population, collapse = " | ")))
    
   
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
