dfStats <- dfInterventions %>% 
  group_by(data.eventID) %>% 
  summarize(duration = mean(difftime(as.POSIXct(paste("2020-01-01", data.eventEndTime)), 
                                     as.POSIXct(paste("2020-01-01", data.eventStartTime)), 
                                     units = 'hours')))

for (i in 1:nrow(dfStats)) {
  if(dfStats$duration[i] <= 0) {
    dfStats$duration[i] <- 24 + abs(dfStats$duration[i])
  }
}