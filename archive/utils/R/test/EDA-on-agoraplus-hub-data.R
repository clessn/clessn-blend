# Stats à propos des données Agora+

ggplot(dfSimple_hub, aes(x=eventSourceType)) + geom_bar(stat = 'count') + geom_label(aes(label = ..count..),stat = 'count')

df %>% filter(eventSourceType == "Conférences et points de presse") %>% mutate(speakerType = case_when(speakerType == "" ~ "journaliste", speakerType == "journaliste" ~ "journaliste", TRUE ~ speakerType)) %>% group_by(speakerSpeechType) %>% count() %>% ggplot(aes(x=speakerSpeechType, y=n)) + geom_bar(stat = "Identity") + geom_text(aes(label = n), vjust = -1)

df %>% filter(eventSourceType == "Conférences et points de presse") %>% mutate(speakerType = case_when(speakerType == "" ~ "journaliste", speakerType == "journaliste" ~ "journaliste", TRUE ~ speakerType), speakerSpeechType = case_when(speakerSpeechType == "commentaire" ~ "réponse",  TRUE ~ speakerSpeechType)) %>% group_by(speakerSpeechType) %>% count() %>% ggplot(aes(x=speakerSpeechType, y=n)) + geom_bar(stat = "Identity") + geom_text(aes(label = n), vjust = -1)

df %>% filter(eventSourceType == "Conférences et points de presse") %>% mutate(speakerType = case_when(speakerType == "" ~ "journaliste", speakerType == "journaliste" ~ "journaliste", TRUE ~ speakerType)) %>% group_by(speakerType) %>% count() %>% ggplot(aes(x=speakerType, y=n)) + geom_bar(stat = "Identity") + geom_text(aes(label = n), vjust = -1)

