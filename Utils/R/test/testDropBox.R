df <- data.frame(objectType=character(),
                 objectName=character(),
                 objectID=character())

for (i in 1:length(r$entries)) {

  df <- df %>% rbind(data.frame(objectType = r$entries[i][[1]]$.tag, 
                                objectName = r$entries[i][[1]]$path_lower,
                                objectID = r$entries[i][[1]]$id))
}