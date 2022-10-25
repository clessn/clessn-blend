dbxListDir <- function(dir, token) {
  body <- paste('{\"path\": \"',
                dir,
                '\",\"recursive\": false,\"include_media_info\": false,\"include_deleted\": false,\"include_has_explicit_shared_members\": false,\"include_mounted_folders\": true,\"include_non_downloadable_files\": true}',
                sep='')
  
  r <- httr::POST(url = 'https://api.dropboxapi.com/2/files/list_folder',
                  httr::add_headers('Authorization' = paste("Bearer", token),
                                    'Content-Type' = 'application/json'),
                  body = body,
                  encode = "form")
  
  if (r$status_code == 200) {
    l <- httr::content(r)
    
    df <- data.frame(objectType=character(),
                     objectName=character(),
                     objetcPath=character(),
                     objectID=character())
    
    
    if (length(l$entries) > 0) {
      for (i in 1:length(l$entries)) {
        df <- df %>% rbind(data.frame(objectType = l$entries[i][[1]]$.tag,
                                      objectName = basename(l$entries[i][[1]]$path_display),
                                      objectPath = dirname(l$entries[i][[1]]$path_display),
                                      objectID = l$entries[i][[1]]$id))
      }
      clessnverse::logit(message=paste("directory", dir, "sucessfully listed."), logger = logger)
    } else {
      clessnverse::logit(message=paste("warning : directory", dir, "is empty"), logger = logger)
    }
    return(df)
  } else {
    if (grepl("invalid_access_token", httr::content(r))) stop("invalid dropbox token")
    
    clessnverse::logit(message=paste(httr::content(r), "when attempting list content of folder", dir), logger = logger)
    return(data.frame(objectType = r$entries[i][[1]]$.tag,
                      objectName = r$entries[i][[1]]$path_lower,
                      objectID = r$entries[i][[1]]$id))
  }
  
}



library(dplyr)

token <- Sys.getenv("DROPBOX_TOKEN")

file.list <- dbxListDir("/_sharedFolder_TheseSaillancePromesses/data/EnjeuxPhysio/tobii/", token)

#for (file in file.list) {
for (i in 1:3) {
  clessnverse::dbxDownloadFile(paste(file$objectPath[i], "/", file$objectName[i], sep=""), ".", token)
  
  data <- read.csv2(file$objectName[i], sep="\t", header = T, fileEncoding="UTF-16", skipNul = T)
  
  write.csv2(data,paste(file$objectName[i], ".csv", sep=""), row.names = FALSE)
  
  if (file.exists(file$objectName[i])) file.remove(file$objectName[i])
}