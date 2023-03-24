library(dplyr)

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


filter <- clessnhub::create_filter(type="candidate", schema="candidate_elxn_qc2022", metadata=NULL, data=NULL)
df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
