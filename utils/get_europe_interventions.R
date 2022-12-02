library(dplyr)

scriptname <- "get_europe_interventions"
opt <- list(dataframe_mode = "update", log_output = c("console"), hub_mode = "skip", download_data = TRUE, translate=TRUE)
logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))


# Download v3 MPs information
metadata_filter <- list(institution="European Parliament")
filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=metadata_filter)  
dfPersons <- clessnhub::get_items('persons', filter=filter, download_data = TRUE)

# Download all interventions
clessnverse::logit(scriptname, "Retreiving interventions from hub with download data = FALSE", logger)
dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(type = "parliament_debate", schema = "v2", 
                                                            location = "EU", format = "html",
                                                            download_data = opt$download_data,
                                                            token = Sys.getenv('HUB_TOKEN'))


#df <- data.frame(full_name=dfInterventions$data.speakerFullName, party = dfInterventions$data.speakerParty, pol_group = dfInterventions$data
#    .speakerPolGroup, country = dfInterventions$data.speakerCountry)

df <- dfInterventions %>% filter(is.na(data.speakerCountry) | is.na(data.speakerParty) | is.na(data.speakerPolGroup))

df1 <- unique(df)

which(is.na(df1$data.speakerPolGroup)) %>% length()
which(is.na(df1$data.speakerParty)) %>% length()
which(is.na(df1$data.speakerCountry)) %>% length()

write.csv2(df1, "missingvalues.csv")
getwd()
ncol(df1)
