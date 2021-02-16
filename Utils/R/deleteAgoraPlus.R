agora <- quorum::listAgoraplusTransformedData(auth)


for (i in 1:nrow(agora)) {
  quorum::deleteAgoraplusTransformedData(auth, agora$slug[i])
}