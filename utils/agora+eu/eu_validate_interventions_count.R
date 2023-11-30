library(magrittr)
library(dplyr)

# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

table_name <- "agoraplus_european_parliament"


df <- clessnverse::get_mart_table(
  table_name = table_name,
  data_filter = list(
     data__.schema="202303"
  ),
  credentials = credentials,
  nbrows = 0
)

events <- unique(df$event_id)

diffdf <- data.frame()

for (event in events) {
  cat("event", event, "\n")
  checkdf <- df[df$event_id == event,]
  
  lake_item <- hublot::filter_lake_items(
    credentials = credentials,
    filter = list(key = event)
  )

  if (length(lake_item$results) > 1) {
    stop("too many lake items - expecting 1")
  }

  lake_item <- lake_item$results[[1]]

  r <- rvest::session(lake_item$file)

  if (r$response$status_code != 200) {
    stop("bad status code")
  }

  if (lake_item$metadata$format == "html") {
    content <- rvest::read_html(r)
    body <- content %>% rvest::html_nodes("body")# %>% rvest::html_text()

    table_nodes <- body %>% rvest::html_nodes('table[width="100%"][border="0"][cellpadding="5"][cellspacing="0"]')
    
    interventions_count <- 0
    outputdf <- data.frame()

    for (table_node in table_nodes) {
      intervention_node <- table_node %>% rvest::html_nodes('td[align="left"][valign="top"]') 

      if (length(intervention_node) > 0) {
        is_intervention <- intervention_node %>% rvest::html_nodes('img') %>% rvest::html_attr('src') %>% grepl("data/img/arrow_summary.gif", .)
        if (TRUE %in% is_intervention) {
          outputdf <- outputdf %>% rbind(data.frame(rvest::html_text(intervention_node[[1]])))
          interventions_count <- interventions_count + 1
        }
      }
    }


    if (nrow(checkdf) != nrow(outputdf)) {
      cat("event", event, "interventions_count", interventions_count, "checkdf", nrow(checkdf), "outputdf", nrow(outputdf), "\n")
      diffdf <- diffdf %>% rbind(data.frame(event, lake_item$metadata$source, nrow(checkdf), nrow(outputdf)))
    }  

  }
}


mydf <- diffdf
mydf$diff <- mydf$nrow.outputdf. - mydf$nrow.checkdf.

summary(mydf$diff)

hist(mydf$diff)
ggplot2::ggplot(mydf, ggplot2::aes(x = diff)) + ggplot2::geom_histogram()
ggplot2::geom_histogram(binwidth = 5, fill = "skyblue", color = "black", ggplot2::aes(y = ..density..)) +
  ggplot2::geom_density(alpha = 0.2, fill = "orange") +
  ggplot2::labs(title = "Histogram of Values", x = "Values", y = "Density")



dfhtml <- df[which(df$.lake_item_format == "html"),]
dfcountinterventions <- dfhtml %>% group_by(event_id) %>% count()

mydf1 <- mydf %>% left_join(dfcountinterventions, by = c("event" = "event_id"))
mydf1$diffratio <- mydf1$diff / mydf1$n
ggplot2::ggplot(mydf1, ggplot2::aes(x = diffratio)) + ggplot2::geom_histogram()
