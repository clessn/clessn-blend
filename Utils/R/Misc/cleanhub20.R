
clessnhub::connect()

dfCache.to.clean <- clessnhub::get_items('agoraplus_cache')
dfInterventions.to.clean <- clessnhub::get_items('agoraplus_interventions')



#####
##### Purge all
#####
cat("Cache contains", nrow(dfCache.to.clean), "rows.", sep = ' ')
cache_answer <- readline(prompt="Continue (y/N): ")
if (tolower(cache_answer) == "y" && nrow(dfCache.to.clean) > 0) {
  for (i in 1:nrow(dfCache.to.clean)) {
    clessnhub::delete_item('agoraplus_cache', key = dfCache.to.clean$key[i])
  }
}


cat("Interventions contains", nrow(dfInterventions.to.clean), "rows.", sep = ' ')
interventions_answer <- readline(prompt="Continue (y/N): ")
if (tolower(interventions_answer) == "y" && nrow(dfInterventions.to.clean) > 0) {
  for (i in 1:nrow(dfInterventions.to.clean)) {
    clessnhub::delete_item('agoraplus_interventions', dfInterventions.to.clean$key[i])
  }
}

#####
##### Delete blanks
#####
#for (i in 1:nrow(dfCache.hub)) {
#  #if (str_detect(dfCache.hub$eventID[i], "debats")) clessnhub::delete_item(dfCache.hub$uuid[i], 'agoraplus_warehouse_cache_items')
#  if (dfCache.hub$eventID[i] == "") cat("trouvé", i, "\n", sep=" ") 
#}


#for (i in 1:nrow(dfSimple.hub)) {
#  #if (str_detect(dfSimple.hub$eventID[i], "debats")) clessnhub::delete_item(dfSimple.hub$uuid[i], 'agoraplus_warehouse_event_items')
#  if (dfSimple.hub$eventID[i] == "") cat("trouvé", i, "\n", sep=" ") 
#  
#}


#for (i in 1:nrow(dfDeep.hub)) {
#  #if (str_detect(dfDeep.hub$eventID[i], "debats")) clessnhub::delete_item(dfDeep.hub$uuid[i], 'agoraplus_warehouse_intervention_items')
#  if (dfDeep.hub$eventID[i] == "") cat("trouvé", i, "\n", sep=" ") 
#}
