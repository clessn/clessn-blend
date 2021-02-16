setwd("~/infoscope/Clients/CLESSN/Produits/Agora+/")

dfSimple <- read.csv2(file="dfSimpleAgoraPlus-v3.csv", header = TRUE, sep = ";", quote = "\"", encoding = "UTF-8")
dfDeep <- read.csv2(file="dfDeepAgoraPlus-v3.csv", header = TRUE, sep = ";", quote = "\"", encoding = "UTF-8")

dfFuuuullll <- left_join(dfDeep, dfSimple, by="eventID")
dataAgora <- dfFuuuullll