clessnhub::connect()
clessnhub::v1_configure()

journalists_v1 <- clessnhub::v1_download_light('warehouse_journalists')

for (i_journalist in 1:nrow(journalists_v1)) {
  journalist_first_name     <- stringr::str_to_title(journalists_v1$firstName[i_journalist])
  journalist_last_name      <- stringr::str_to_title(journalists_v1$lastName[i_journalist])
  journalist_full_name      <- paste(journalist_first_name, journalist_last_name, sep=' ')
  journalist_is_female      <- journalists_v1$isFemale[i_journalist]
  journalist_current_media  <- journalists_v1$source[i_journalist]
  journalist_previous_media_list  <- NA
  journalist_previous_media_date_list  <- NA
  
  journalist_twitter_handle <- as.character(journalists_v1$twitterHandle[i_journalist])
  if (substr(journalist_twitter_handle,1,1) == "_") journalist_twitter_handle <- paste("@", substr(journalist_twitter_handle,2,nchar(journalist_twitter_handle)), sep='')
  if (substr(journalist_twitter_handle,1,1) != "@") journalist_twitter_handle <- paste("@", journalist_twitter_handle, sep='')

  
  journalist_twitterID <- gsub("[^0-9]","", journalists_v1$twitterID[i_journalist]) 
  if (journalist_twitterID=="" || journalist_twitterID==0 || journalist_twitterID=="0") journalist_twitterID <- as.character(sample.int(999999,1))
  
  journalist_twitter_account_protected <- NA
  journalist_twitter_title <- NA
  
  metadata <- list(source = "manual", location = "CA")
  data = list(firstName = journalist_first_name, lastName = journalist_last_name, fullName = journalist_full_name,
              isFemale = journalist_is_female, currentMedia = journalist_current_media, previousMediaList = journalist_previous_media_list,
              previousMediaDatesList = journalist_previous_media_date_list, twitterHandle = journalist_twitter_handle,
              twitterID = journalist_twitterID, twitterAccountProtected = journalist_twitter_account_protected,
              twitterTitle = journalist_twitter_title)
  
  tryCatch(
    {  
      clessnhub::create_item(table = "persons", key = journalist_twitterID, type = "journalist", schema = "v2", metadata = metadata, data = data)
    },
    error = function(e) {
      clessnhub::edit_item(table = "persons", key = journalist_twitterID, type = "journalist", schema = "v2", metadata = metadata, data = data)
    }
  )

}
