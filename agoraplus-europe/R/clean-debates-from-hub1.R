nullToNA <- function(x) {
  x[sapply(x, is.null)] <- NA
  return(x)
}

key <- dfInterventions_all1$key[which(dfInterventions_all1$data.speakerParty=="Parti Socialiste")]

for (i in 1:length(key)) {
  item <- clessnhub::get_item('agoraplus_interventions', key[[i]])
  
  item$data$speakerParty <- 	"Parti socialiste"
  
  item <- lapply(item, nullToNA)
  
  clessnhub::edit_item('agoraplus_interventions', item$key, item$type, item$schema, item$metadata, item$data)
}
