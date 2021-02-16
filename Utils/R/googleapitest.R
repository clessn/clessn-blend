library(googleLanguageR)

gl_auth("agoraplus-025ac9bb492b.json")

my_config <- list(encoding = "LINEAR16",
                  #diarizationConfig = list(
                  #  enableSpeakerDiarization = TRUE,
                  #  minSpeakerCount = 2,
                  #  maxSpeakerCount = 3),
                  sampleRateHertz = 8000,
                  languageCode = "fr-CA",
                  enableAutomaticPunctuation = TRUE,
                  useEnhanced = FALSE
                  )

gl <- gl_speech("/Users/patrick/Downloads/Audio/Sans titre 3.wav",  customConfig = my_config)

test_audio <- system.file("woman1_wb.wav", package = "googleLanguageR")

gl <- gl_speech(test_audio)





curl --location --request POST 'https://canadacentral.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=en-US' \
--header 'Ocp-Apim-Subscription-Key: b5421646c3b449a0856a089a67d84b2a' \
--header 'Content-Type: audio/wav' \
--data-binary '/Users/patrick/Downloads/Audio/test.wav'




library(http)
library(jsonlite)
library(data.table)
library(dplyr)

cogapikey<-"b5421646c3b449a0856a089a67d84b2a"
cogapi<-"https://canadacentral.stt.speech.microsoft.com/speech/recognition/conversation/cognitiveservices/v1?language=fr-CA"

text=c("is this english?"
       ,"tak er der mere kage"
       ,"merci beaucoup"
       ,"guten morgen"
       ,"bonjour"
       ,"merde"
       ,"That's terrible"
       ,"R is awesome")

# Prep data
df<-data_frame(id=1:8,text)
mydata<-list(documents= df)

response<-POST(cogapi, 
               add_headers(`Ocp-Apim-Subscription-Key`=cogapikey),
               body=toJSON(mydata))
