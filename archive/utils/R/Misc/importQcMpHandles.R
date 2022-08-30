clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

handles <- readr::read_csv("/Users/patrick/Downloads/qcDep_handle.csv")


for (i in 1:nrow(handles)) {
  if (!is.na(handles$name[i])) {
    cat(handles$name[i], "\n")
    
    mp_full_name  <- strsplit(handles$name[i], " ")
    mp_first_name <- mp_full_name[[1]][1]
    mp_last_name  <- mp_full_name[[1]][2]
    
    filter <- clessnhub::create_filter(type = "mp", schema = "v2", data = list("firstName"=mp_first_name, "lastName"=mp_last_name))
    mp <- clessnhub::get_items('persons', filter = filter, download_data = T)
    
    if (!is.null(mp)) {
      mp_twitter_handle <- handles$screen_name[i]
      
      mp$data.twitterHandle <- mp_twitter_handle
      
      data_to_commit <- as.list(mp[which(grepl("^data.", names(mp)))])
      names(data_to_commit) <- gsub("^data.", "", names(data_to_commit)) 
      
      clessnhub::edit_item('persons', key = mp$key, data=data_to_commit)
    } else {
      cat("could not update", mp_first_name, mp_last_name, "\n", sep = " ")
    }
  }
}

