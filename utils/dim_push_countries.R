"%>%" <- dplyr::`%>%`

# connect to hublot
credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))



clessnverse::dbxDownloadFile("/clessn-blend/_SharedFolder_clessn-blend/data/countrylanguagecodes-eu.csv", "./", Sys.getenv("DROPBOX_TOKEN"))
df <- read.csv2("countrylanguagecodes-eu.csv")
names(df) <- tolower(gsub("\\.","_",names(df)))

df_country <- df %>% 
  dplyr::mutate(
    code2 = sub('.*(?=.{2}$)', '', two_letter, perl = T), 
    code3 = sub('.*(?=.{3}$)', '', three_letter, perl = T)
  ) %>% 
  dplyr::select(country,code2,code3) %>% 
  unique()

for (i in 1:nrow(df_country)) {
  languages <- paste(
    df %>% 
    dplyr::filter(country == df_country$country[i]) %>% 
    dplyr::select(language) %>% as.list() %>% unlist(), collapse = ","
  )

  two_letters <- paste(
    df %>% 
    dplyr::filter(country == df_country$country[i]) %>% 
    dplyr::select(two_letter) %>% as.list() %>% unlist(), collapse = ","
  )

  three_letters <- paste(
    df %>% 
    dplyr::filter(country == df_country$country[i]) %>% 
    dplyr::select(three_letter) %>% 
    as.list() %>% unlist(), collapse = ","
  )

  clessnverse::commit_warehouse_row(
    table="countries",
    key=df_country$code3[i],
    row=list(
      .id = df_country$code3[i],
      name = df_country$country[i],
      languages = languages,
      short_name_2 = df_country$code2[i],
      short_name_3 = df_country$code3[i],
      locales_2 = two_letters,
      locales_3 = three_letters,
      union = "EU",
      leading_party = NA,
      leader = NA,
      wikipedia_url = paste("https://en.wikipedia.org/wiki/", df_country$country[i], sep="")
    ),
    refresh_data=TRUE,
    credentials=credentials
  )

}


