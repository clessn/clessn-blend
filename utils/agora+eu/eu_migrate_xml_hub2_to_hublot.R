# connect to hublot
credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))

# connect to hub2
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


# create filter
filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=list(location="EU", format="xml"), data=NULL)
# Now get the data
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = TRUE)

for (i in 1:nrow(df)) {
  #grab a row from hub2
  source <- df[i,]

  #
}