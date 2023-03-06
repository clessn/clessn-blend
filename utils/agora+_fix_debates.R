library(dplyr)

scriptname <- "fix_agoraplus_interventions"
opt <- list(dataframe_mode = "update", log_output = c("console"), hub_mode = "skip", download_data = TRUE, translate=TRUE)
logger <- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))


#dfInterventions <- clessnverse::loadAgoraplusInterventionsDf(
#  type = "parliament_debate", 
#  schema = "v2", 
#  location = "CA-QC", 
#  format = "html",
#  download_data = TRUE,
#  token = Sys.getenv('HUB_TOKEN')
#)

filter <- clessnhub::create_filter(
    type = "parliament_debate", 
    schema = "v2",
    metadata = list(location="CA-QC", format="html", parliament_number="/4")
)

df <- clessnhub::get_items('agoraplus_interventions',filter = filter)

df$data.interventionID <- paste(df$data.eventID,"-",df$data.interventionSeqNum, sep='')

for (i in 1:nrow(df)) {
    row <- df[i,]
    row$metadata.parliament_number = "43"
    row$metadata.parliament_session = "1"

    row_metadata <- as.list(row[,which(grepl("^metadata\\.",names(df)))])
    row_data <- as.list(row[,which(grepl("^data\\.",names(df)))])

    names(row_data) <- gsub("^data.","",names(row_data))
    names(row_metadata) <- gsub("^metadata.","",names(row_metadata))

    clessnhub::edit_item(
        'agoraplus_interventions', 
        df$key[i], 
        type=df$type[i], 
        schema=df$schema[i],
        metadata = row_metadata,
        data = row_data 
    )
}
