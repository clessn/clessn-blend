scriptname <- "get_quebec_interventions"

opt <- list(dataframe_mode = "update", log_output = c("console"),
            hub_mode = "skip", download_data = TRUE, translate = TRUE)

logger <- clessnverse::log_init(scriptname, opt$log_output,
                                Sys.getenv("LOG_PATH"))

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
clessnverse::logit(scriptname, "connecting to hub", logger)

my_filter <- clessnhub::create_filter(
  type = "parliament_debate",
  schema = "vintage",
  # metadata = list(
  #   location = "CA-QC"),
  # data = list(
  #   eventDate__gte = "2020-01-01",
  #   eventDate__lte = "2020-1-28"
  # )
)

df <- clessnhub::get_items(
  table = "agoraplus_interventions",
  filter = my_filter,
  download_data = TRUE,
  max_pages = -1
)
