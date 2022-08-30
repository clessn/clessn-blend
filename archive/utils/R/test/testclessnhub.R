
library(devtools)

devtools::install_github("clessn/clessn-hub-r")


clessnhub::configure()

clessnhub::refresh_token()

########################################
###### POUR AJOUTER DES LIGNE AU DATASET

# On va chercher les données sur le HUB
# Ça nous donne un objet de type list dans lequel il y a une viariabl edf
hub.data <- clessnhub::download('agoraplus_warehouse_cache_items') 

# On sort les données sous forme de data.frame pour travailler dedans avec nos algos
df <- as.data.frame(hub.data$df)

# Par exemple on peut ajouter une ligne à notre df
# Les 4 première colonnes ne sont pas nos données à nous, ce sont de metadonnées du HUB 
# d'où les "","","",""
df[nrow(df),] <- data.frame("","","","",eventID = "line 2340", eventHtml = "iowqqopieiwqywehwdjkhdjksah hakl dsh")
# Ensuite quand on est prêt on peut l'ajouter
clessnhub::upload('agoraplus_warehouse_cache_items', as.list(df[nrow(df),]))

# On peut reloader le data à partir de ce qu'on vient de pousser sur le hub
hub.data <- clessnhub::refresh(hub.data)
df <- as.data.frame(hub.data$df)

# On peut même ajouter + de lignes
df.ajout <- data.frame(uuid = c("","",""),
                       created = c("","",""),
                       modified = c("","",""),
                       metadata = c("","",""),
                       eventID = c("ligne 10","ligne 20", "ligne 30"),
                       eventHtml = c("dasio uisoad ywetwhg dguaidi djsi","31dwqw dsads ewq wq rfvfd","da 42feerf ywetd2ww12hg dgudsasasaaidi 9128 90812")
                       )
df <- df %>% rbind(df.ajout)

# Ensuite quand on est prêt on peut l'ajouter
# CETTE FAÇON NE FONCTIONNE PAS POUR L'INSTANT
# clessnhub::upload('agoraplus_warehouse_cache_items', as.list(df[df$uuid=="",]))

# EN ATTENDANT ON PEUT FAIRE OBSERVATION PAR OBSERVATION
df.to.push <- df[which(df$uuid ==""),]
for (i in 1:nrow(df[df$uuid=="",])) {
  line.to.push <- df.to.push[i,]
  clessnhub::upload('agoraplus_warehouse_cache_items', as.list(line.to.push))
}

# On peut reloader le data à partir de ce qu'on vient de pousser sur le hub
hub.data <- clessnhub::refresh(hub.data)
df <- as.data.frame(hub.data$df)


###### POUR SUPPRIMER DES LIGNES DU DATASET

# ligne par ligne
ligne1 <- df[which(df$eventID == "ligne 1"),]
clessnhub::delete(tablename='agoraplus_warehouse_cache_items', uuid=ligne1$uuid)
hub.data <- clessnhub::download('agoraplus_warehouse_cache_items')
df <- as.data.frame(hub.data$df)

# ligne par ligne en boucle avec une condition 
clessnhub::refresh_token()
hub.data <- clessnhub::download('agoraplus_warehouse_cache_items') 
df <- as.data.frame(hub.data$df)
df.uuid.of.the.data.to.delete <- df$uuid[which(df$created < Sys.time())]
for (i in 1:length(df.uuid.of.the.data.to.delete)) {
  clessnhub::delete(tablename='agoraplus_warehouse_cache_items', uuid=df.uuid.of.the.data.to.delete[i])
}
