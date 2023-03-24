library(clessnverse)
scriptname <- "convertthesisfiles"
log_output <- "console"
log_path <- "."

token <- Sys.getenv("DROPBOX_TOKEN")

logger <- clessnverse::loginit(scriptname, log_output, log_path)

file.list <- clessnverse::dbxListDir("/Academique/theseSaillancePromesses/_sharedFolder_TheseSaillancePromesses/data/EnjeuxPhysio/tobii/", token)

for (i in 1:nrow(file.list)) {
  file <- file.list[i,]
  download_status <- clessnverse::dbxDownloadFile(paste(file$objectPath, "/", file$objectName, sep=""), ".", token)

  if (download_status == TRUE) {
  
    data <- read.csv2(file$objectName, sep="\t", header = T, fileEncoding="UTF-16", skipNul = T)
    
    write.csv2(data,paste(file$objectName, ".csv", sep=""), row.names = FALSE, quote = FALSE)

    upload_status <- clessnverse::dbxUploadFile(
      paste(file$objectName, ".csv", sep=""), 
      paste(file$objectPath, "/", sep=""), 
      token, 
      overwrite = TRUE
      )

    if (upload_status == TRUE) {
      if (file.exists(file$objectName)) file.remove(file$objectName)
      if (file.exists(paste(file$objectName, ".csv", sep=""))) file.remove(paste(file$objectName, ".csv", sep=""))
    } else {
          clessnverse::logit(scriptname, paste("could not upload", paste(file$objectPath, "/",     clessnverse::logit(scriptname, paste("could not download", paste(file$objectPath, "/", file$objectName, sep=""), "to dropbox"), logger), sep=""), "from dropbox"), logger)
    }
  } else {
    clessnverse::logit(scriptname, paste("could not download", paste(file$objectPath, "/", file$objectName, sep=""), "from dropbox"), logger)
  }
}
