# requis
# remotes::install_github("clessn/hublotr")

credentials <- hublot::get_credentials(
    Sys.getenv("HUB3_URL"), 
    Sys.getenv("HUB3_USERNAME"), 
    Sys.getenv("HUB3_PASSWORD"))

lakeitem_list  <- hublot::filter_lake_items(
    credentials, 
    list(
        path = "radarplus/headline",
        metadata__media = "TTS"
    )
)

length(lakeitem_list$results)

for (lake_item in lakeitem_list$results) {

  current_lake_item <- hublot::retrieve_lake_item(
    id = lake_item$id, 
    credentials = credentials
  )

  lakeitem_content <- httr::GET(lakeitem$file)
  lakeitem_parsed <- httr::content(lakeitem_content, as = "text", encoding = "UTF-8")
}

