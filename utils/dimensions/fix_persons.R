# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


df <- clessnhub::get_items(
  table = 'persons',
  filter = clessnhub::create_filter(type = "mp", metadata=list(institution="European Parliament")),
  download_data = TRUE,
  max_pages = -1
)

# df$data.currentPolGroup[which(df$data.currentPolGroup == "Group of the European People's Party (Christian Democrats)")] <- "Group Of The European People's Party (Christian Democrats)"
# df$data.currentPolGroup[which(df$data.currentPolGroup == "Group of the Greens/European Free Alliance")] <- "Group Of The Greens/European Free Alliance"
# df$data.currentPolGroup[which(df$data.currentPolGroup == "Identity and Democracy Group")] <- "Identity And Democracy Group"
# df$data.currentPolGroup[which(df$data.currentPolGroup == "Non-attached Members")] <- "Non-Attached Members"
# df$data.currentPolGroup[which(df$data.currentPolGroup == "European Conservatives and Reformists Group")] <- "European Conservatives And Reformists Group"
# df$data.currentPolGroup[which(df$data.currentPolGroup == "S&D")] <- "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament"
# df$data.currentPolGroup[which(df$data.currentPolGroup == "The Left group in the European Parliament - GUE/NGL")] <- "The Left Group In The European Parliament - GUE/NGL"

df$data.currentPolGroup[which(df$data.currentPolGroup == "Group of the Progressive Alliance of Socialists and Democrats in the European Parliament")] <- "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"

table(df$data.currentPolGroup)

df_to_process <- df[which(df$data.currentPolGroup == "Group Of The Progressive Alliance Of Socialists And Democrats In The European Parliament"),]


for (i in 1:nrow(df_to_process)) {
    row <- df_to_process[i,]

    row_metadata <- as.list(row[,which(grepl("^metadata\\.",names(row)))])
    row_data <- as.list(row[,which(grepl("^data\\.",names(row)))])

    names(row_data) <- gsub("^data.","",names(row_data))
    names(row_metadata) <- gsub("^metadata.","",names(row_metadata))

    clessnhub::edit_item(
        'persons', 
        df_to_process$key[i], 
        type=df_to_process$type[i], 
        schema=df_to_process$schema[i],
        metadata = row_metadata,
        data = row_data 
    )
}
