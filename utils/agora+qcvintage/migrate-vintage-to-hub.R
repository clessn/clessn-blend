library(dplyr)

#connect to hub
clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))

#get rds
clessnverse::dbxDownloadFile(
  filename = "/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-vintage/data/total.rds", 
  local_path = "./", 
  token = Sys.getenv("DROPBOX_TOKEN")
)

df_rds <- readRDS("./total.rds")
new_df <- df_rds[df_rds$data.eventDate < "2020-02-04",]

df <- new_df

#push to hub2
for (i in 318619:nrow(df)) {
    print(i)
    row <- df[i,]

    row_metadata <- as.list(row[,which(grepl("^metadata\\.",names(df)))])
    row_data <- as.list(row[,which(grepl("^data\\.",names(df)))])

    names(row_data) <- gsub("^data.","",names(row_data))
    names(row_metadata) <- gsub("^metadata.","",names(row_metadata))

    tryCatch(
      expr = {
        clessnhub::create_item(
            'agoraplus_interventions', 
            df$key[i], 
            type=df$type[i], 
            schema="vintage",
            metadata = row_metadata,
            data = row_data 
        )
      },
      error = function(e) {
        print(e$message)
      }
    )
}
