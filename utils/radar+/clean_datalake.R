credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD")
)

medias_urls <- list(
   cbcnews = list(
     long_name  = "CBC News",
     short_name = "CBC",
     country    = "CAN",
     base       = "https://www.cbc.ca",
     front      = "/news"
   ),
   jdm = list(
     long_name  = "Le Journal de Montréal",
     short_name = "JDM",
     country    = "CAN",
     base  = "https://www.journaldemontreal.com",
     front = "/"
   ),
   radiocan = list(
     long_name  = "Radio-Canada Info",
     short_name = "RCI",
     country    = "CAN",
     base  = "https://ici.radio-canada.ca",
     front = "/info"
   ),
  nationalPost = list(
    long_name  = "National Post",
    short_name = "NP",
    country    = "CAN",
    base  = "https://nationalpost.com",
    front = "/"
  ),
  tvaNouvelles = list(
    long_name  = "TVA Nouvelles",
    short_name = "TVA",
    country    = "CAN",
    base  = "https://www.tvanouvelles.ca",
    front = "/"
  ),
  globeAndMail = list(
    long_name  = "The Globe and Mail",
    short_name = "GAM",
    country    = "CAN",
    base  = "https://www.theglobeandmail.com",
    front = "/"
  ),
  vancouverSun = list(
    long_name  = "Vancouver Sun",
    short_name = "VS",
    country    = "CAN",
    base  = "https://vancouversun.com",
    front = "/"
  ),
  laPresse = list(
    long_name  = "La Presse",
    short_name = "LAP",
    country    = "CAN",
    base  = "https://www.lapresse.ca",
    front = "/"
  ),
  leDevoir = list(
    long_name  = "Le Devoir",
    short_name = "LED",
    country    = "CAN",
    base  = "https://www.ledevoir.com",
    front = "/"
  ),
  montrealGazette = list(
    long_name  = "Montreal Gazette",
    short_name = "MG",
    country    = "CAN",
    base  = "https://montrealgazette.com",
    front = "/"
  ),
  CTVNews = list(
    long_name  = "CTV News",
    short_name = "CTV",
    country    = "CAN",
    base  = "https://www.ctvnews.ca",
    front = "/"
  ),
  globalNews = list(
    long_name  = "Global News",
    short_name = "GN",
    country    = "CAN",
    base  = "https://globalnews.ca",
    front = "/"
  ),
  theStar = list(
    long_name  = "The Toronto Star",
    short_name = "TTS",
    country    = "CAN",
    base  = "https://www.thestar.com",
    front = "/"
  )
)

datalake_base_path <<- "radarplus"

  r <- hublot::filter_lake_items(
    filter = list(
      path=paste(datalake_base_path, "frontpage", sep="/"),
      metadata__schema = "prod",
      metadata__start_timestamp__isnull=FALSE
    ),
    credentials = credentials
  )

  length(r$results)

  for (lake_item in r$results) {
    cat(lake_item$key, " ", lake_item$metadata$schema, " ", lake_item$metadata$start_timestamp, "\n")

    #hublot::remove_lake_item(lake_item$id, credentials)
  }


  r <- hublot::filter_lake_items(
    filter = list(
      path=paste(datalake_base_path, "headline", sep="/"),
      metadata__schema = "prod",
      metadata__start_timestamp__isnull=FALSE
    ),

    credentials = credentials
  )

  length(r$results)

  for (lake_item in r$results) {
    cat(lake_item$key, " ", lake_item$metadata$schema, " ", lake_item$metadata$start_timestamp, "\n")

    #hublot::remove_lake_item(lake_item$id, credentials)
  }

