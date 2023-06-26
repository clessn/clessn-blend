library(dplyr)

scriptname <- "get_canada_interventions"
opt <- list(dataframe_mode = "update", log_output = c("console"), hub_mode = "skip", download_data = TRUE, translate=TRUE)
logger <- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

#clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
clessnverse::logit(scriptname, "connecting to hub", logger)

clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


my_filter <- clessnhub::create_filter(
  type="parliament_debate", 
  schema="v2", 
  metadata=list(
    institution="House of Commons of Canada"), 
    #format="xml"),
  data=list(
    #eventID="432123HAN123"
    #eventDate__gte="2023-01-01"
    eventDate__lte="2007-01-29"
  )
)

df <- clessnhub::get_items(
  table = 'agoraplus_interventions',
  filter = my_filter,
  download_data = TRUE,
  max_pages = -1
) 

df$data.interventionSeqNum <- as.numeric(df$data.interventionSeqNum)
df <- df %>% arrange(data.interventionSeqNum)

#for (i in 1:nrow(df)) {
#  clessnhub::delete_item('agoraplus_interventions', df$key[i])
#}
