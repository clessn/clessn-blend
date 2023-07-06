r <- httr::GET("https://s3.valeria.science/clessn-hub/agoraplus_interventions.csv")

write.csv(content, "file.csv")
df <- read.csv("file.csv")
nrow(df)
