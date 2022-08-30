clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

myfilter <- clessnhub::create_filter(type="candidate", schema="v2")
dfC <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="mp", metadata = list("institution"="House of Commons of Canada"))
dfM <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="mp", metadata = list("institution"="National Assembly of Quebec"))
dfQM <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="political_party", schema="v1")
dfP <- clessnhub::get_items('persons', myfilter)
myfilter <- clessnhub::create_filter(type="journalist", schema="v2")
dfJ <- clessnhub::get_items('persons', myfilter)


df <- clessnhub::get_items('persons')
df$data.fullName[grep("Gobeil", df)]

