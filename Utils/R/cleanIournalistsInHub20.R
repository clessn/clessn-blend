clessnhub::connect()


my_filter  <- clessnhub::create_filter(type = "journalist")

df <- clessnhub::get_items("persons", filter = my_filter)

for (i_journalist in 1:nrow(df)) {
  clessnhub::delete_item('persons', key = df$key[i_journalist])
}
