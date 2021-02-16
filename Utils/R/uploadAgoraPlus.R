for (i in 1:nrow(dfSimple)) {

  data <- as.list(dfSimple[i,])
  data$eventDate <- as.character(data$eventDate)
  data$eventStartTime <- as.character(data$eventStartTime)
  data$eventEndTime <- as.character(data$eventEndTime)
  current.slug <- paste("dfSimple", dfSimple$id[i], sep = "")
  
  cat("********************** Conf:", i, "**********************\n", sep = " ")
  cat(current.slug, "\n")
  
  
  if ( is.null(quorum::getAgoraplusTransformedData(auth, current.slug)) ) 
    quorum::createAgoraplusTransformedData(auth, current.slug, data)
  else
    quorum::updateAgoraplusTransformedData(auth, current.slug, data)
  
  deep.indexes <- which(dfSimple$id[i] == dfDeep$id)

  for (j in 1:length(deep.indexes)) {
    current.deep.slug <- paste("dfDeep", dfDeep$id[deep.indexes[j]], dfDeep$interventionSeqNum[deep.indexes[j]], sep = "")
    
    cat("Conf:", i, "Intervention:", j, current.deep.slug, "\n", sep = " ")
    
    data.deep <- as.list(dfDeep[deep.indexes[j],])
    data.deep$eventDate <- as.character(data.deep$eventDate)
    data.deep$eventStartTime <- as.character(data.deep$eventStartTime)
    data.deep$eventEndTime <- as.character(data.deep$eventEndTime)
    
    if ( is.null(quorum::getAgoraplusTransformedData(auth, current.deep.slug)) )
      quorum::createAgoraplusTransformedData(auth, current.deep.slug, data.deep)
    else
      quorum::updateAgoraplusTransformedData(auth, current.deep.slug, data.deep)
  }
}


  for (i in 14078:nrow(dfDeep)) {
    current.deep.slug <- paste("dfDeep", dfDeep$id[i], dfDeep$interventionSeqNum[i], sep = "")
    
    cat("Row", i, current.deep.slug, "\n", sep = " ")
    
    data.deep <- as.list(dfDeep[i,])
    data.deep$eventDate <- as.character(data.deep$eventDate)
    data.deep$eventStartTime <- as.character(data.deep$eventStartTime)
    data.deep$eventEndTime <- as.character(data.deep$eventEndTime)
    
    if ( is.null(quorum::getAgoraplusTransformedData(auth, current.deep.slug)) )
      quorum::createAgoraplusTransformedData(auth, current.deep.slug, data.deep)
    else
      quorum::updateAgoraplusTransformedData(auth, current.deep.slug, data.deep)
  }

