# connect to hublot
credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))

df <- read.csv2("countrylanguagecodes-eu.csv")
names(df) <- tolower(gsub("\\.","_",names(df)))

df_country <- df %>% 
  mutate(
    code2 = sub('.*(?=.{2}$)', '', two_letter, perl = T), 
    code3 = sub('.*(?=.{3}$)', '', three_letter, perl = T)
  ) %>% 
  select(country,code2,code3) %>% 
  unique()

for (i in 1:nrow(df_country)) {
  languages <- paste(df %>% filter(country == df_country$country[i]) %>% select(language) %>% as.list() %>% unlist(), collapse = ",")
  two_letters <- paste(df %>% filter(country == df_country$country[i]) %>% select(two_letter) %>% as.list() %>% unlist(), collapse = ",")
  three_letters <- paste(df %>% filter(country == df_country$country[i]) %>% select(three_letter) %>% as.list() %>% unlist(), collapse = ",")

  clessnverse::commit_warehouse_row(
    table="countries",
    key=df_country$country[i],
    row=list(
      country = df_country$country[i],
      two_letter = df_country$code2[i],
      three_letter = df_country$code3[i],
      languages = languages,
      two_letter_locales = two_letters,
      three_letter_locales = three_letters,
      union = "EU"
    ),
    refresh_data=TRUE,
    credentials=credentials
  )

}


strsplit(test,"-")[[1]][2]
