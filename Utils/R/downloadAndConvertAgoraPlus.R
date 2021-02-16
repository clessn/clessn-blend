library(dplyr)
library(ggplot2)
#install.packages('devtools')
#devtools::install_github('clessn/quorum-r')


##########################################################################################################
####  
####  Aller chercher les données Agora+ sur la BD Quorum
####  Ces données combinent deux datasets : agoraSimple et agoraDeep
####  Pour faire de l'analyse au niveau des conférences entières, on utilise agoraSimple
####  Pour faire de l'analyse au niveau des interventions à l'intérieu des conférences, on utilise agoraDeep 
####  
##########################################################################################################

agora <- quorum::listAgoraplusTransformedData(auth)


##########################################################################################################
####  
####  On trie le dataset obtenu
####
##########################################################################################################
agorasorted <- arrange(agora, eventStartTime, id, interventionSeqNum)


##########################################################################################################
####  
####  Séparation du dataset et épuration des slugs et blob provenant de la BD Quorum
####
##########################################################################################################
agoraSimple <- dplyr::filter(agorasorted, grepl('dfSimple', slug))
agoraDeep <- dplyr::filter(agorasorted, grepl('dfDeep', slug))


##########################################################################################################
####  
####  Épuration de agoraDeep et épuration des colonnes de contenu brut pour éviter les lenteurs dans RStudio
####
##########################################################################################################

# On épure les colonnes de la BD Quorum
agoraSimple$content <- NULL
agoraSimple$slug <- NULL
agoraSimple$created <- NULL


# On enlève les données brutes de la page web scrapée
agoraSimple$html <- NULL


# On enlève les données de agoraDeep
agoraSimple$interventionSeqNum <- NULL
agoraSimple$speakerFirstName <- NULL
agoraSimple$speakerLastName <- NULL
agoraSimple$speakerGender <- NULL
agoraSimple$speakerType <- NULL
agoraSimple$SpeakerParty <- NULL
agoraSimple$speakerSpeechType <- NULL
agoraSimple$speakerCirconscription <- NULL
agoraSimple$speakerMedia <- NULL
agoraSimple$speakerSpeechLang <- NULL
agoraSimple$speakerSpeechSentenceCount <- NULL
agoraSimple$speakerSpeechParagraphCount <- NULL
agoraSimple$speakerSpeechRelevanceIndex <- NULL
agoraSimple$speakerSpeech <- NULL
agoraSimple$speakerTranslatedSpeech <- NULL


##########################################################################################################
####  
####  Épuration de agoraSimple et épuration des colonnes de contenu brut pour éviter les lenteurs dans RStudio
####
##########################################################################################################

# On épure les colonnes de la BD Quorum
agoraDeep$content <- NULL
agoraDeep$slug <- NULL
agoraDeep$created <- NULL

# On enlève les données brutes de la page web scrapée
agoraDeep$html <- NULL

# On enlève les données qui appartiennent à agoraSimple
agoraDeep$eventContent <- NULL



######  Validation par graphs

a <- ggplot(agoraSimple, aes(x = eventRelevanceIndexByParagraph))

a + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)
a + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)

b <- ggplot(agoraSimple, aes(x = eventRelevanceIndexBySentence))

b + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)
b + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)

c <- ggplot(agoraDeep, aes(x = eventRelevanceIndexByParagraph))

c + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)
c + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)

d <- ggplot(agoraDeep, aes(x = eventRelevanceIndexBySentence))

d + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)
d + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)

e <- ggplot(agoraDeep, aes(x = speakerSpeechRelevanceIndexByParagraph))

e + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)
e + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)

f <- ggplot(agoraDeep, aes(x = speakerSpeechRelevanceIndexBySentence))

f + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)
f + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)



a <- ggplot(agoraSimple, aes(x = eventNormRelevanceIndexByParagraph))

a + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)
a + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)

b <- ggplot(agoraSimple, aes(x = eventNormRelevanceIndexBySentence))

b + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)
b + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)

c <- ggplot(agoraDeep, aes(x = eventNormRelevanceIndexByParagraph))

c + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)
c + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)

d <- ggplot(agoraDeep, aes(x = eventNormRelevanceIndexBySentence))

d + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)
d + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(eventNormRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)

e <- ggplot(agoraDeep, aes(x = speakerSpeechNormRelevanceIndexByParagraph))

e + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechNormRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)
e + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechNormRelevanceIndexByParagraph)), linetype = "dashed", size = 0.6)

f <- ggplot(agoraDeep, aes(x = speakerSpeechNormRelevanceIndexBySentence))

f + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechNormRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)
f + geom_histogram(aes(y=..density..)) + geom_density() + geom_vline(aes(xintercept = mean(speakerSpeechNormRelevanceIndexBySentence)), linetype = "dashed", size = 0.6)


