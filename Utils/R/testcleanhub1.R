clessnhub::configure()

dfCache.hub <- clessnhub::download_table('agoraplus_warehouse_cache_items')
dfSimple.hub <- clessnhub::download_table('agoraplus_warehouse_event_items')
dfDeep.hub <- clessnhub::download_table('agoraplus_warehouse_intervention_items')

#####
##### Purge all
#####
#for (i in 1:nrow(dfCache.hub)) {
#  clessnhub::delete_item(dfCache.hub$uuid[i], 'agoraplus_warehouse_cache_items')
#}

for (i in 1:nrow(dfSimple.hub)) {
  clessnhub::delete_item(dfSimple.hub$uuid[i], 'agoraplus_warehouse_event_items')
}

for (i in 1:nrow(dfDeep.hub)) {
  clessnhub::delete_item(dfDeep.hub$uuid[i], 'agoraplus_warehouse_intervention_items')
}


#####
##### Delete blanks
#####
for (i in 1:nrow(dfCache.hub)) {
  #if (str_detect(dfCache.hub$eventID[i], "debats")) clessnhub::delete_item(dfCache.hub$uuid[i], 'agoraplus_warehouse_cache_items')
  if (dfCache.hub$eventID[i] == "") cat("trouvé", i, "\n", sep=" ") 
}


for (i in 1:nrow(dfSimple.hub)) {
  #if (str_detect(dfSimple.hub$eventID[i], "debats")) clessnhub::delete_item(dfSimple.hub$uuid[i], 'agoraplus_warehouse_event_items')
  if (dfSimple.hub$eventID[i] == "") cat("trouvé", i, "\n", sep=" ") 
  
}


for (i in 1:nrow(dfDeep.hub)) {
  #if (str_detect(dfDeep.hub$eventID[i], "debats")) clessnhub::delete_item(dfDeep.hub$uuid[i], 'agoraplus_warehouse_intervention_items')
  if (dfDeep.hub$eventID[i] == "") cat("trouvé", i, "\n", sep=" ") 
}
