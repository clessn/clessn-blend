token <- Sys.getenv("DROPBOX_TOKEN")

logger <- clessnverse::loginit("convertthesisfiles", "console", ".")

file.list <- clessnverse::dbxListDir("/_sharedFolder_TheseSaillancePromesses/data/EnjeuxPhysio/tobii/", token)

for (i in 1:nrow(file.list)) {
  file <- file.list[i,]
  clessnverse::dbxDownloadFile(paste(file$objectPath, "/", file$objectName, sep=""), ".", token)
  
  data <- read.csv2(file$objectName, sep="\t", header = T, fileEncoding="UTF-16", skipNul = T)
  
  write.csv2(data,paste(file$objectName, ".csv", sep=""), row.names = FALSE, quote = FALSE)
  
  if (file.exists(file$objectName)) file.remove(file$objectName)
}
