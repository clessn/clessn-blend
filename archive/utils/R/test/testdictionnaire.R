library(textcat)
library(stringr)
library(tidyr)
library(dplyr)
library(tm)
library(quanteda)
library(tidytext)
library(tibble)
library(readxl)
library(tictoc)

install.packages('devtools')
devtools::install_github('clessn/clessn-hub-r', force = TRUE)
devtools::install_github('clessn/clessn-verse')
library(clessnverse)

patterns.relevance.covid.wordmatch <- clessnverse::getDictionary(topic = "covid", method = "wordmatch")
patterns.relevance.covid.regex <- clessnverse::getDictionary(topic = "covid", method = "regex")
dictionary_covid <- clessnverse::getDictionary(topic = "covid", method = "dfm")
dictionary_fr <- clessnverse::getDictionary(topic = "sentiment", method = "dfm", language = "fr")
dictionary_en <- clessnverse::getDictionary(topic = "sentiment", method = "dfm", language = "en")

dfCache <- read.csv2(file=
                       "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfCacheAgoraPlus-v3.csv",
                     sep=";", comment.char="#", encoding = "UTF-8")

dfSimple <- read.csv2(file=
                        "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfSimpleAgoraPlus-v3.csv",
                      sep=";", comment.char="#", encoding = "UTF-8")

dfDeep <- read.csv2(file=
                      "../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfDeepAgoraPlus-v3.csv",
                    sep=";", comment.char="#", encoding = "UTF-8")


dfSimple1 <- dfSimple
dfDeep1 <- dfDeep


#####  Tester la validitè des patterns du dictionnaire vs les mots ou expressions identifiées dans le dictionnaire
#DictionnaireFinal <- read_excel("../projet-quorum/_SharedFolder_projet-quorum/DictionnaireCOVID/DictionnaireDetaille.xlsx")

#for (i in 1:nrow(DictionnaireFinal) ) {
#  cat("\r",i)
#  if (!is.na(DictionnaireFinal$FR[i])) {
#    dfmA <- dfm(tolower(corpus(DictionnaireFinal$FR[i])), dictionary = dictionary_fr )
#    if (length(dfmA@x) == 0) print(DictionnaireFinal$FR[i])
#  }
#  
#  cat("\r",i)
#  if (!is.na(DictionnaireFinal$EN[i])) {
#    dfmA <- dfm(tolower(corpus(DictionnaireFinal$EN[i])), dictionary = dictionary_en )
#    if (length(dfmA@x) == 0) print(DictionnaireFinal$EN[i])
#  }
#}


#####  Calculer l'indice de pertinence sur dfSimple avec le nouvo dico REGEX
vec.relevance <- c()
for ( i in 1:nrow(dfSimple1) ) {
   cat("\r",i)
   vec.relevance <- c(vec.relevance, clessnverse::evaluateRelevanceIndex(dfSimple1$eventContent[i], patterns.relevance.covid.regex, base = "sentence", method = "regex" ))
}
dfSimple1[13] <- vec.relevance
names(dfSimple1)[13] <- "indexREGEX"


##### Calculer l'indice de pertinence sur dfSimple avec le dico *** DFM ***
vec.relevance <- c()
for ( i in 1:nrow(dfSimple1) ) {
  cat("\r",i)
  vec.relevance <- c(vec.relevance, clessnverse::evaluateRelevanceIndex(dfSimple1$eventContent[i], dictionary_covid, base = "sentence", method = "dfm"))
}
dfSimple1[14] <- vec.relevance
names(dfSimple1)[14] <- "indexDFM"
write.csv2(dfSimple1, file="../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfSimpleAgoraPlus-testIndex.csv",row.names = FALSE)




#####  Calculer l'indice de pertinence sur dfDeep avec le nouvo dico REGEX
vec.relevance <- c()
for ( i in 1:nrow(dfDeep1) ) {
  cat("\r",i)
  vec.relevance <- c(vec.relevance, clessnverse::evaluateRelevanceIndex(dfDeep1$speakerSpeech[i], patterns.relevance.covid.regex, base = "sentence" , method = "regex"))
}
dfDeep1[17] <- vec.relevance
names(dfDeep1)[17] <- "indexREGEX"


##### Calculer l'indice de pertinence sur dfDeep avec le dico *** DFM ***
vec.relevance <- c()
for ( i in 1:nrow(dfDeep1) ) {
  cat("\r",i)
  vec.relevance <- c(vec.relevance, clessnverse::evaluateRelevanceIndex(dfDeep1$speakerSpeech[i], dictionary_covid, base = "sentence", method = "dfm"))
}
dfDeep1[18] <- vec.relevance
names(dfDeep1)[18] <- "indexDFM"

write.csv2(dfDeep1, file="../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfDeepAgoraPlus-testIndex.csv",row.names = FALSE)




##### Calculer le ton sur dfDeep pour la demo Tableau
df.toneIndex <- data.frame()
for ( i in 1:nrow(dfDeep1) ) {
  cat("\r",i)
  if (is.na(dfDeep1$speakerSpeechLang[i]) || dfDeep1$speakerSpeechLang[i] == "fr") {
    dfm.test <- dfm(dfDeep1$speakerSpeech[i], dictionary = dictionary_fr)
    dfm.df <- convert(dfm.test, to = "data.frame")
    neg <- dfm.df$NEGATIVE
    pos <- dfm.df$POSITIVE
  }
  else {
    dfm.test <- dfm(dfDeep1$speakerSpeech[i], dictionary = data_dictionary_LSD2015)
    dfm.df <- convert(dfm.test, to = "data.frame")
    neg <- dfm.df$negative - dfm.df$neg_negative
    pos <- dfm.df$positive - dfm.df$neg_positive
  }
  

  prop.neg <- neg / lengths(gregexpr("\\W+", dfDeep1$speakerSpeech[i])) + 1
  prop.pos <- pos / lengths(gregexpr("\\W+", dfDeep1$speakerSpeech[i])) + 1
  tone <- prop.pos - prop.neg
  df.toneIndex <- rbind( df.toneIndex, data.frame(toneIndexREGX = tone*(1+dfDeep1$indexREGEX[i]), 
                                                  toneIndexFDM = tone*(1+dfDeep1$indexDFM[i]) ))
}

dfDeep1.toneIndex <- cbind(dfDeep1, df.toneIndex)

write.csv2(dfDeep1.toneIndex, file="../quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/data/dfDeepAgoraPlus-testTon.csv",row.names = FALSE)


