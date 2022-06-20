###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                             agora-plus-canada                               #
#                                                                             #
#                                                                             #
#                                                                             #
###############################################################################

library(dplyr)



###############################################################################
########################      Functions and Globals      ######################
###############################################################################


# modified method from clessnhub. Force json and ignore some parts
http_post <- function(path, body, options=NULL, verify=T) {
    token <- hub_config$token
    token_prefix <- hub_config$token_prefix
    response <- httr::POST(url=paste0(hub_config$url, path), body=body, httr::accept_json(), httr::content_type_json(), config=httr::add_headers(Authorization=paste(token_prefix, token)), verify=verify, httr::timeout(30))
    return(response)
}




# function that will extract all data from a specified table based on a hubr type filter and an optional maximum of loops
extract_data <- function(table_name, hubr_filter=list(), max_pages=-1) {
    #hubr_filter <- jsonlite::toJSON(hubr_filter)
    path <- paste("/data/", table_name, "/count/", sep="")
    response <- http_post(path, body=hubr_filter)
    result <- httr::content(response)
    count <- result$count
    print(paste("count:", count))
    
    path <- paste("/data/", table_name, "/filter/", sep="")
    response <- http_post(path, body=hubr_filter)
    page <- httr::content(response)
    data = list()
    
    repeat {
        
        data <- c(data, page$results)
        print(paste(length(data), "/", count))
        path <- page$"next"
        
        if (is.null(path)) {
            break
        }
        
        max_pages <- max_pages - 1
        if (max_pages == 0)
        {
            break
        }
        
        path <- strsplit(path, "science")[[1]][[2]]
        response <- http_post(path, body=hubr_filter)
        page <- httr::content(response)
    }
    
    return(data)
}





compute_nb_sentences <- function(txt_bloc) {
    df_sentences <- tibble::tibble(text = txt_bloc) %>%
                        tidytext::unnest_tokens(sentence, text, token="sentences",format="text", to_lower = T)
                    
    nb_sentences <- nrow(df_sentences)

    return(nb_sentences)
}




compute_nb_words <- function(txt_bloc) {
    df_words <- tibble::tibble(text = txt_bloc) %>%
                        tidytext::unnest_tokens(sentence, text, token="words",format="text", to_lower = T)
                    
    nb_words <- nrow(df_words)

    return(nb_words)
}




compute_relevance_score <- function(txt_bloc, dictionary) {
    # Prepare corpus
    txt <- stringr::str_replace_all(string = txt_bloc, pattern = "M\\.|Mr\\.|Dr\\.", replacement = "")
    tokens <- quanteda::tokens(txt, remove_punct = TRUE)
    tokens <- quanteda::tokens_remove(tokens, quanteda::stopwords("french"))
    tokens <- quanteda::tokens_remove(tokens, quanteda::stopwords("spanish"))
    tokens <- quanteda::tokens_remove(tokens, quanteda::stopwords("english"))

    tokens <- quanteda::tokens_replace(
                            tokens, 
                            quanteda::types(tokens), 
                            stringi::stri_replace_all_regex(quanteda::types(tokens), "[lsd]['\\p{Pf}]", ""))

    if (length(tokens[[1]]) == 0) {
        tokens <- quanteda::tokens("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", remove_punct = TRUE)
    } 

    dfm_corpus <- quanteda::dfm(tokens)

    # Compute Relevance on the entire corpus
    lookup <- quanteda::dfm_lookup(dfm_corpus, dictionary = dictionary, valuetype = "glob")
    df_score <- quanteda::convert(lookup, to="data.frame")
    df_score$doc_id <- NULL

    return(df_score)
}




compute_catergory_sentiment_score <- function(txt_bloc, category_dictionary, sentiment_dictionary) {    
    # Build one corpus per category and compute sentiment on each corpus
    corpus <- data.frame(doc_id = integer(), category = character(), text = character())

    df_sentences <- tibble::tibble(text = txt_bloc) %>%
                        tidytext::unnest_tokens(sentence, text, token="sentences",format="text", to_lower = T)
                    
    toks <- quanteda::tokens(df_sentences$sentence)

    dfm_corpus <- quanteda::dfm(toks)
    lookup <- quanteda::dfm_lookup(dfm_corpus, dictionary = category_dictionary, valuetype = "glob")
    df <- quanteda::convert(lookup, to="data.frame") %>% select(-c("doc_id"))

    df_sentences <- df_sentences %>% cbind(df)
    df_sentences <- df_sentences %>% tidyr::pivot_longer(-c(sentence), names_to = "category", values_to = "relevance")
    df_sentences <- df_sentences %>% filter(relevance > 0)

    df_categories <- df_sentences %>% dplyr::group_by(category) %>% dplyr::summarise(text = paste(sentence, collapse = " "), relevance = sum(relevance))

    df_categories$text <- stringr::str_replace_all(string = df_categories$text, pattern = "M\\.|Mr\\.|Dr\\.", replacement = "") 

    toks <- quanteda::tokens(df_categories$text)
    toks <- quanteda::tokens(df_categories$text, remove_punct = TRUE)
    # On n'enlève pas les stopwords parce qu'on veut garder "pas" ou "ne" car connotation négative
    # toks <- quanteda::tokens_remove(toks, quanteda::stopwords("french"))
    # toks <- quanteda::tokens_remove(toks, quanteda::stopwords("spanish"))
    # toks <- quanteda::tokens_remove(toks, quanteda::stopwords("english"))
    toks <- quanteda::tokens_replace(
                                toks, 
                                quanteda::types(toks), 
                                stringi::stri_replace_all_regex(quanteda::types(toks), "[lsd]['\\p{Pf}]", ""))


    if (length(toks) == 0) {
        tokens <- quanteda::tokens("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.", remove_punct = TRUE)
    } 

    dfm_corpus <- quanteda::dfm(toks)
    lookup <- quanteda::dfm_lookup(dfm_corpus, dictionary = sentiment_dictionary, valuetype = "glob")
    df <- quanteda::convert(lookup, to="data.frame") %>% select(-c("doc_id"))

    df_categories <- df_categories %>% 
                     cbind(df)

    if (nrow(df_categories) > 0) {
        df_categories <- df_categories %>%
                            dplyr::mutate(sentiment = positive - neg_positive - negative + neg_negative) %>%
                            select(-c("text"))
    }

    df_category_pads <- data.frame(category = names(category_dictionary), relevance=rep(0L, length(category_dictionary)), 
                                negative=rep(0L, length(category_dictionary)), positive=rep(0L, length(category_dictionary)), 
                                neg_positive=rep(0L, length(category_dictionary)), neg_negative=rep(0L, length(category_dictionary)),
                                sentiment=rep(0L, length(category_dictionary)))

    df_sentiments <- df_categories %>% rbind(df_category_pads)  

    df_sentiments <- aggregate(df_sentiments[,-c(1)], list(df_sentiments$category), FUN=sum)
    names(df_sentiments)[1] <- "category"

    return(df_sentiments)
}




commit_hub_row <- function(table, key, row, mode = "refresh", credentials) {

    # If the row with the same key exist and mode=refresh then overwrite it with the new data
    # Otherwise, do nothing (just log a message)

    table <- paste("clhub_tables_datamart_", table, sep="")

    filter <- list(key__exact = key)
    item <- hubr::filter_table_items(table, credentials, filter)

    if(length(item$results) == 0) {
        # l'item n'existe pas déjà dans hublot
        hubr::add_table_item(table,
                body = list(
                    key = key,
                    timestamp = Sys.time(),
                    data = as.list(row)
                ),
                credentials
            )
    } else {
        # l'item existe déjà dans hublot
        if (mode == "refresh") {
            hubr::update_table_item(table, id = item$result[[1]]$id,
                                    body = list(
                                        key = key,
                                        timestamp = as.character(Sys.time()),
                                        data = jsonlite::toJSON(as.list(row), auto_unbox = T)
                                    ),
                                    credentials
                                   )
        } else {
            # DO nothing but log a message saying skipping
        }

    }
}



clessnverse::version()
hubr::check_version()

scriptname <- "r_vd_shiny_medias_prototype"

credentials <- hubr::get_credentials(Sys.getenv("HUB3_URL"), 
                                     Sys.getenv("HUB3_USERNAME"), 
                                     Sys.getenv("HUB3_PASSWORD"))

clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))

dbx_token <- Sys.getenv("DROPBOX_TOKEN")

logger <- clessnverse::loginit(script=scriptname, backend=c("console", "file", "hub"), logpath=".")

clessnverse::logit(scriptname = scriptname, "starting script", logger)



###############################################################################
######################  Get Data Sources from Warehouse  ######################
######################              HUB 3.0              ######################
###############################################################################
my_table <- "vd_shiny_medias_prototype"
datamart <- clessnverse::get_datamart_table(my_table, credentials)

clessnverse::logit(scriptname=scriptname, message=paste("clhub_tables_datamart_vd_shiny_medias_prototype dataframe contains", nrow(datamart)), logger=logger)



###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################

# Getting journalists
filter <- clessnhub::create_filter(type="journalist")
df_journalists <- clessnhub::get_items('persons', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Journalists dataframe contains", nrow(df_journalists)), logger=logger)

# Getting tweets from journalists of feb 2022
#filter <- clessnhub::create_filter(metadata = list("personType"="journalist"), data=list("creationDate__gte"="2022-01-24", "creationDate__lte"="2022-01-25"))
#df_tweets <- clessnhub::get_items('tweets', filter)
#clessnverse::logit(scriptname=scriptname, message=paste("Tweets dataframe contains", nrow(df_tweets)), logger=logger)
# filter
filter = list(
    type="tweet",
    metadata__personType="journalist",
    data__creationDate__gte="2022-02-01",
    data__creationDate__lte="2022-02-28"
    # most field lookups here should work
    # https://docs.djangoproject.com/en/4.0/ref/models/querysets/#field-lookups
)

# return a list of elements structured as named lists
data <- extract_data(
    "tweets", # the table name, can be tweets, agoraplus_press_releases, persons, or any other
    jsonlite::toJSON(filter, auto_unbox = T), # the auto_unbox is really important
    max_pages = -1 # set to a positive number x to extraxt x * 1000 items only
    )
 
# convert to dataframe. Can be quite heavy and will take a long time. Run it on smaller sets if necessary
df_tweets <- tidyjson::spread_all(data)

# Getting agoraplus press conf interventions from journalists of feb 2022
#filter <- clessnhub::create_filter(type="press_conference", metadata=list("location"="CA-QC"), data=list("eventDate__gte"="2022-01-24", "eventDate__lte"="2022-01-25", speakerType="journalist"))
#df_interventions <- clessnhub::get_items('agoraplus_interventions', filter)
#clessnverse::logit(scriptname=scriptname, message=paste("Agoraplus press_conference interventions dataframe contains", nrow(df_interventions)), logger=logger)
# filter
filter = list(
    type="press_conference",
    metadata__location="CA-QC",
    data__speakerType="journalist",
    data__eventDate__gte="2022-02-01",
    data__eventDate__lte="2022-02-28"
    # most field lookups here should work
    # https://docs.djangoproject.com/en/4.0/ref/models/querysets/#field-lookups
)

# return a list of elements structured as named lists
data <- extract_data(
    "agoraplus_interventions", # the table name, can be tweets, agoraplus_press_releases, persons, or any other
    jsonlite::toJSON(filter, auto_unbox = T), # the auto_unbox is really important
    max_pages = -1 # set to a positive number x to extraxt x * 1000 items only
    )
 
# convert to dataframe. Can be quite heavy and will take a long time. Run it on smaller sets if necessary
df_interventions <- tidyjson::spread_all(data)


###############################################################################
#################   Get Data Sources from Files in Hub 3.0   ##################
###############################################################################
# Getting the lexicoder of stakes of the vitrine democratique

hubr::list_files()

data <- hubr::filter_lake_items(credentials, list(key = "dict_sentiments"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
sentiment_dictionary <- read.csv2(url)
dict_list <- list()
for (i in unique(sentiment_dictionary$categorie)) {
    dict_list[[i]] <- sentiment_dictionary$item[sentiment_dictionary$categorie == i]
}
sentiment_dictionary <- quanteda::dictionary(dict_list)

#file_info <- hubr::retrieve_file("dictionnaire_LexicoderFR-enjeux", credentials)
#stakes_dictionary_fr <- read.csv(file_info$file)
data <- hubr::filter_lake_items(credentials, list(key = "dict_enjeux_fr"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
stakes_dictionary_fr <- read.csv(url)
stakes_dictionary_fr$X <- NULL
dict_list <- list()
for (i in unique(stakes_dictionary_fr$categorie)) {
    dict_list[[i]] <- stakes_dictionary_fr$item[stakes_dictionary_fr$categorie == i]
}
stakes_dictionary_fr <- quanteda::dictionary(dict_list)


#file_info <- hubr::retrieve_file("dictionnaire_LexicoderEN-enjeux", credentials)
#stakes_dictionary_en <- read.csv(file_info$file)
data <- hubr::filter_lake_items(credentials, list(key = "dict_enjeux_en"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
stakes_dictionary_en <- read.csv(url)
stakes_dictionary_en$X <- NULL
dict_list <- list()
for (i in unique(stakes_dictionary_en$categorie)) {
    dict_list[[i]] <- stakes_dictionary_en$item[stakes_dictionary_en$categorie == i]
}
stakes_dictionary_en <- quanteda::dictionary(dict_list)

stakes_dictionary <- c(stakes_dictionary_fr, stakes_dictionary_en)
# remove dups due to en et fr same roots
for (i in names(stakes_dictionary)) {
    stakes_dictionary[[i]] <- unique(stakes_dictionary[[i]])
}


# Getting the political parties dictionary
#file_info <- hubr::retrieve_file("dictionnaire_politiqueCAN", credentials)
#polparties_dictionary_can <- read.csv2(file_info$file)
data <- hubr::filter_lake_items(credentials, list(key = "dict_political_parties_can"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
polparties_dictionary_can <- read.csv2(url)

polparties_dictionary_can$X <- NULL
dict_list <- list()
for (i in unique(polparties_dictionary_can$categorie)) {
    dict_list[[i]] <- polparties_dictionary_can$item[polparties_dictionary_can$categorie == i]
}
polparties_dictionary_can <- quanteda::dictionary(dict_list)


#file_info <- hubr::retrieve_file("dictionnaire_politiqueQC", credentials)
#polparties_dictionary_qc <- read.csv(file_info$file)
data <- hubr::filter_lake_items(credentials, list(key = "dict_political_parties_qc"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
polparties_dictionary_qc <- read.csv(url)

polparties_dictionary_qc$X <- NULL
dict_list <- list()
for (name in names(polparties_dictionary_qc)) {
    for (i in 1:length(polparties_dictionary_qc[[name]])) {
        if (polparties_dictionary_qc[[name]][i] != "") dict_list[[name]][i] <- polparties_dictionary_qc[[name]][i]
    }
}
polparties_dictionary_qc <- quanteda::dictionary(dict_list)

polparties_dictionary <- c(polparties_dictionary_qc, polparties_dictionary_can)
# remove dups due to en et fr same roots
for (i in names(polparties_dictionary)) {
    polparties_dictionary[[i]] <- unique(polparties_dictionary[[i]])
}



data <- hubr::filter_lake_items(credentials, list(key = "dict_covid"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
covid_dictionary <- read.csv(url)
names(covid_dictionary) <- "covid"
covid_dictionary <- quanteda::dictionary(as.list(covid_dictionary))

data <- hubr::filter_lake_items(credentials, list(key = "dict_ai"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
ai_dictionary <- read.csv2(url)
ai_dictionary$X <- NULL
ai_dictionary$X.1 <- NULL
ai_dictionary$X.2 <- NULL
ai_dictionary$X.3 <- NULL
ai_dictionary <- quanteda::dictionary(as.list(ai_dictionary))


# Getting the radar+ csv file from february
data <- hubr::filter_lake_items(credentials, list(key = "radar-feb2022"))
df <- tidyjson::spread_all(data$results)
url <- df$..JSON[[1]]$file
df_radarplus <- read.csv2(url)



###############################################################################
########################               Main              ######################
##                                                                           ##
## On combine les interventions des journalistes et des médias dans les      ##
## sources de données provenant                                              ##
## - De Twitter,                                                             ##
## - Des interventions en conf de presse (assnat et youtube)                 ##
## - Des unes des médias                                                     ##
## Et on calcule des index basée sur des dictionnaires                       ##
##                                                                           ##
## Les jeux de données sont filtrés pour la période de fév 2022 seulement    ##
###############################################################################

# Structure de la table
df <- data.frame(
    source = character(), author_id = character(), author_name = character(), author_gender = character(),
    author_media = character(), content = character(), content_date = character(),
    LoiCrime_words_count = double(), LoiCrime_pos_neg_diff = double(),
    CultNation_words_count = double(), CultNation_pos_neg_diff = double(),
    TerresPubAgri_words_count = double(), TerresPubAgri_pos_neg_diff = double(),
    GvntGvnc_words_count = double(), GvntGvnc_pos_neg_diff = double(),
    Immigration_words_count = double(), Immigration_pos_neg_diff = double(),
    DroitsEtc_words_count = double(), DroitsEtc_pos_neg_diff = double(),
    SanteServSoc_words_count = double(), SanteServSoc_pos_neg_diff = double(),
    EconomieTravail_words_count = double(), EconomieTravail_pos_neg_diff = double(),
    Education_words_count = double(), Education_pos_neg_diff = double(),
    EnviroEnergie_words_count = double(), EnviroEnergie_pos_neg_diff = double(),
    AffIntlDefense_words_count = double(), AffIntlDefense_pos_neg_diff = double(),
    qc_caq_words_count = double(), qc_caq_pos_neg_diff = double(),
    qc_pq_words_count = double(), qc_pq_pos_neg_diff = double(),
    qc_plq_words_count = double(), qc_plq_pos_neg_diff = double(),
    qc_qs_words_count = double(), qc_qs_pos_neg_diff = double(),
    can_lpc_words_count = double(), can_lpc_pos_neg_diff = double(),
    can_cpc_words_count = double(), can_cpc_pos_neg_diff = double(),
    can_npd_words_count = double(), can_npd_pos_neg_diff = double(),
    can_bq_words_count = double(), can_bq_pos_neg_diff = double(),
    can_gpc_words_count = double(), can_gpc_pos_neg_diff = double(),
    can_ppc_words_count = double(), can_ppc_pos_neg_diff = double(),
    headline_mins_duration = double(),
    content_sentences_count = double(),
    content_words_count = double(),
    stringsAsFactors = F
)

# Agoraplus 

for (i in 1:nrow(df_interventions)) {
    person <- NULL

    # get the journalist attributes
    if (!is.na(df_interventions$data.speakerFullName[i])) {
        first_name <- strsplit(df_interventions$data.speakerFullName[i], ",")[[1]][2]
        last_name <- strsplit(df_interventions$data.speakerFullName[i], ",")[[1]][1]

        first_name <- trimws(first_name)
        last_name <- trimws(last_name)

        person_filter <- clessnhub::create_filter(data=list("firstName" = first_name, "lastName" = last_name))
        #person <- clessnhub::get_items('persons', person_filter) 
        person <- df_journalists[which(df_journalists$data.firstName == first_name & df_journalists$data.lastName == last_name),]



        if (nrow(person) == 0) {
            first_name <- strsplit(df_interventions$data.speakerFullName[i], " ")[[1]][1]
            last_name <- strsplit(df_interventions$data.speakerFullName[i], " ")[[1]][2]

            first_name <- trimws(first_name)
            last_name <- trimws(last_name)

            person_filter <- clessnhub::create_filter(data=list("firstName" = first_name, "lastName" = last_name))
            #person <- clessnhub::get_items('persons', person_filter) 
            person <- df_journalists[which(df_journalists$data.firstName == first_name & df_journalists$data.lastName == last_name),]
        }

        if (nrow(person) == 0) {
            person_filter <- clessnhub::create_filter(data=list("fullName" = df_interventions$data.speakerFullName[i]))
            #person <- clessnhub::get_items('persons', person_filter) 
            person <- df_journalists[which(df_journalists$data.fullName == df_interventions$data.speakerFullName[i]),]
        }
    } 
    
    if (is.null(person) || nrow(person) == 0) {
        person <- data.frame(key=NA, type="journalist", data.fullName=df_interventions$data.speakerFullName[i], 
                             data.isFemale=df_interventions$data.speakerGender[i], data.currentMedia=df_interventions$data.speakerMedia[i])
    }

    df_stakes_score <- compute_relevance_score(df_interventions$data.interventionText[i], stakes_dictionary)
    df_polparties_score <- compute_relevance_score(df_interventions$data.interventionText[i], polparties_dictionary)
    df_sentiments <- compute_catergory_sentiment_score(df_interventions$data.interventionText[i], c(stakes_dictionary, polparties_dictionary), sentiment_dictionary)

    row <- data.frame(source=df_interventions$type[i], author_id = person$key, 
                        author_name = person$data.fullName, author_gender = if (!is.na(person$data.isFemale) && person$data.isFemale == 1) "F" else "M",
                        author_media = person$data.currentMedia, content = df_interventions$data.interventionText[i], 
                        content_date = df_interventions$data.eventDate[i],
                        LoiCrime_words_count = df_stakes_score$crime,
                        LoiCrime_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "crime"],
                        CultNation_words_count = df_stakes_score$culture,
                        CultNation_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "culture"],
                        TerresPubAgri_words_count = df_stakes_score$agriculture + df_stakes_score$'land-water-management' + df_stakes_score$fisheries + df_stakes_score$forestry,
                        TerresPubAgri_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
                        GvntGvnc_words_count = df_stakes_score$government_ops + df_stakes_score$prov_local + df_stakes_score$intergovernmental + df_stakes_score$constitutional_natl_unity,
                        GvntGvnc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
                        Immigration_words_count = df_stakes_score$immigration,
                        Immigration_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "immigration"],
                        DroitsEtc_words_count = df_stakes_score$civil_rights + df_stakes_score$religion + df_stakes_score$aboriginal,
                        DroitsEtc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
                        SanteServSoc_words_count = df_stakes_score$healthcare + df_stakes_score$social_welfare,
                        SanteServSoc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
                        EconomieTravail_words_count = df_stakes_score$macroeconomics + df_stakes_score$labour + df_stakes_score$foreign_trade + df_stakes_score$sstc + df_stakes_score$finance + df_stakes_score$housing + df_stakes_score$transportation,
                        EconomieTravail_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
                        Education_words_count = df_stakes_score$education,
                        Education_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "education"],
                        EnviroEnergie_words_count = df_stakes_score$environment + df_stakes_score$energy,
                        EnviroEnergie_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
                        AffIntlDefense_words_count = df_stakes_score$defence + df_stakes_score$intl_affairs,
                        AffIntlDefense_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
                        qc_caq_words_count = df_polparties_score$caq,
                        qc_caq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "caq"],
                        qc_pq_words_count = df_polparties_score$pq,
                        qc_pq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "pq"],
                        qc_plq_words_count = df_polparties_score$plq,
                        qc_plq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "plq"],
                        qc_qs_words_count = df_polparties_score$qs,
                        qc_qs_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "qs"],
                        can_lpc_words_count = df_polparties_score$lpc,
                        can_lpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "lpc"],
                        can_cpc_words_count = df_polparties_score$cpc,
                        can_cpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "cpc"],
                        can_npd_words_count = df_polparties_score$ndp,
                        can_npd_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ndp"],
                        can_bq_words_count = df_polparties_score$bq,
                        can_bq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "bq"],
                        can_gpc_words_count = df_polparties_score$gpc,
                        can_gpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "gpc"],
                        can_ppc_words_count = df_polparties_score$ppc,
                        can_ppc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ppc"],
                        headline_mins_duration = NA,
                        content_sentences_count = compute_nb_sentences(df_interventions$data.interventionText[i]),
                        content_words_count = compute_nb_words(df_interventions$data.interventionText[i]),
                        stringsAsFactors = F
                        )

    df <- df %>% rbind(row)

    cat('committing', df_interventions$key[i], '\n')                  
    commit_hub_row(my_table, df_interventions$key[i], row, "refresh", credentials)
}


# Twitter

for (i in 1:nrow(df_tweets)) {
    person <- NULL

    #person <- clessnhub::get_item('persons', df_tweets$data.personKey[i]) 
    person <- df_journalists[which(df_journalists$key == df_tweets$data.personKey[i]),]

    df_stakes_score <- compute_relevance_score(df_tweets$data.text[i], stakes_dictionary)
    df_polparties_score <- compute_relevance_score(df_tweets$data.text[i], polparties_dictionary)
    df_sentiments <- compute_catergory_sentiment_score(df_tweets$data.text[i], c(stakes_dictionary, polparties_dictionary), sentiment_dictionary)

    row <- (data.frame(
                    source=df_tweets$type[i], author_id = df_tweets$data.personKey[i], 
                    author_name = person$data.fullName, author_gender = if (!is.na(person$data.isFemale) && person$data.isFemale == 1) "F" else "M",
                    author_media = person$data.currentMedia, content = df_tweets$data.text[i], 
                    content_date = df_tweets$data.creationDate[i],
                    LoiCrime_words_count = df_stakes_score$crime,
                    LoiCrime_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "crime"],
                    CultNation_words_count = df_stakes_score$culture,
                    CultNation_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "culture"],
                    TerresPubAgri_words_count = df_stakes_score$agriculture + df_stakes_score$'land-water-management' + df_stakes_score$fisheries + df_stakes_score$forestry,
                    TerresPubAgri_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
                    GvntGvnc_words_count = df_stakes_score$government_ops + df_stakes_score$prov_local + df_stakes_score$intergovernmental + df_stakes_score$constitutional_natl_unity,
                    GvntGvnc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
                    Immigration_words_count = df_stakes_score$immigration,
                    Immigration_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "immigration"],
                    DroitsEtc_words_count = df_stakes_score$civil_rights + df_stakes_score$religion + df_stakes_score$aboriginal,
                    DroitsEtc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
                    SanteServSoc_words_count = df_stakes_score$healthcare + df_stakes_score$social_welfare,
                    SanteServSoc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
                    EconomieTravail_words_count = df_stakes_score$macroeconomics + df_stakes_score$labour + df_stakes_score$foreign_trade + df_stakes_score$sstc + df_stakes_score$finance + df_stakes_score$housing + df_stakes_score$transportation,
                    EconomieTravail_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
                    Education_words_count = df_stakes_score$education,
                    Education_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "education"],
                    EnviroEnergie_words_count = df_stakes_score$environment + df_stakes_score$energy,
                    EnviroEnergie_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
                    AffIntlDefense_words_count = df_stakes_score$defence + df_stakes_score$intl_affairs,
                    AffIntlDefense_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
                    qc_caq_words_count = df_polparties_score$caq,
                    qc_caq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "caq"],
                    qc_pq_words_count = df_polparties_score$pq,
                    qc_pq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "pq"],
                    qc_plq_words_count = df_polparties_score$plq,
                    qc_plq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "plq"],
                    qc_qs_words_count = df_polparties_score$qs,
                    qc_qs_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "qs"],
                    can_lpc_words_count = df_polparties_score$lpc,
                    can_lpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "lpc"],
                    can_cpc_words_count = df_polparties_score$cpc,
                    can_cpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "cpc"],
                    can_npd_words_count = df_polparties_score$ndp,
                    can_npd_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ndp"],
                    can_bq_words_count = df_polparties_score$bq,
                    can_bq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "bq"],
                    can_gpc_words_count = df_polparties_score$gpc,
                    can_gpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "gpc"],
                    can_ppc_words_count = df_polparties_score$ppc,
                    can_ppc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ppc"],
                    headline_mins_duration = NA,
                    content_sentences_count = compute_nb_sentences(df_tweets$data.text[i]),
                    content_words_count = compute_nb_words(df_tweets$data.text[i]),
                    stringsAsFactors = F
                )
                )

    df <- df %>% rbind(row)

    cat('committing', df_tweets$key[i], '\n')
    commit_hub_row(my_table, df_tweets$key[i], row, "refresh", credentials)

}


# Radar plus

for (i in 1:nrow(df_radarplus)){

    if (is.na(df_radarplus$content[i]) || df_radarplus$content[i] == "") next

    person <- NULL

    # get the journalist attributes
    if (!is.na(df_radarplus$author[i])) {
        df_radarplus$author[i] <- gsub(":", "", df_radarplus$author[i])
        df_radarplus$author[i] <- gsub("Texte", "", df_radarplus$author[i])
        df_radarplus$author[i] <- trimws(df_radarplus$author[i])
        
        person_filter <- clessnhub::create_filter(data=list("fullName" = df_radarplus$author[i]))
        #person <- clessnhub::get_items('persons', person_filter) 
        person <- df_journalists[which(df_journalists$data.fullName == df_radarplus$author[i]),]

        

        if (nrow(person) == 0) {
            first_name <- strsplit(df_radarplus$author[i], " ")[[1]][1]
            last_name <- strsplit(df_radarplus$author[i], " ")[[1]][2]
            person_filter <- clessnhub::create_filter(data=list("firstName" = first_name, "lastName" = last_name))
            #person <- clessnhub::get_items('persons', person_filter) 
            person <- df_journalists[which(df_journalists$data.firstName == first_name & df_journalists$data.lastName == last_name),]
        }

        if (nrow(person) == 0) {
            person_filter <- clessnhub::create_filter(data=list("twitterName" = df_radarplus$author[i]))
            #person <- clessnhub::get_items('persons', person_filter) 
            person <- df_journalists[which(df_journalists$data.twitterName == df_radarplus$author[i]),]
        }
    } 
    
    if (is.null(person) || nrow(person) == 0) {
        person <- data.frame(key=NA, type=NA, data.fullName=NA, data.isFemale=NA, data.currentMedia=df_radarplus$media[i])
    }

    df_stakes_score <- compute_relevance_score(df_radarplus$content[i], stakes_dictionary)
    df_polparties_score <- compute_relevance_score(df_radarplus$content[i], polparties_dictionary)
    df_sentiments <- compute_catergory_sentiment_score(df_radarplus$content[i], c(stakes_dictionary, polparties_dictionary), sentiment_dictionary)

    row <- data.frame(
                source=if (!is.na(person$type) && person$type == "media") person$data.fullName else person$data.currentMedia,
                author_id = person$key,
                author_name = if (!is.na(person$data.fullName)) person$data.fullName else person$data.currentMedia,
                author_gender = if ("data.isFemale" %in% names(person) && !is.na(person$data.isFemale) && person$data.isFemale == 1) {"F"} else {if ("data.isFemale" %in% names(person)) "M" else NA},
                author_media = if (!is.na(person$type) && person$type == "media") person$data.fullName else person$data.currentMedia,
                content = df_radarplus$content[i],
                content_date = df_radarplus$begin_date[i],
                LoiCrime_words_count = df_stakes_score$crime,
                LoiCrime_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "crime"],
                CultNation_words_count = df_stakes_score$culture,
                CultNation_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "culture"],
                TerresPubAgri_words_count = df_stakes_score$agriculture + df_stakes_score$'land-water-management' + df_stakes_score$fisheries + df_stakes_score$forestry,
                TerresPubAgri_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
                GvntGvnc_words_count = df_stakes_score$government_ops + df_stakes_score$prov_local + df_stakes_score$intergovernmental + df_stakes_score$constitutional_natl_unity,
                GvntGvnc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
                Immigration_words_count = df_stakes_score$immigration,
                Immigration_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "immigration"],
                DroitsEtc_words_count = df_stakes_score$civil_rights + df_stakes_score$religion + df_stakes_score$aboriginal,
                DroitsEtc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
                SanteServSoc_words_count = df_stakes_score$healthcare + df_stakes_score$social_welfare,
                SanteServSoc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
                EconomieTravail_words_count = df_stakes_score$macroeconomics + df_stakes_score$labour + df_stakes_score$foreign_trade + df_stakes_score$sstc + df_stakes_score$finance + df_stakes_score$housing + df_stakes_score$transportation,
                EconomieTravail_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
                Education_words_count = df_stakes_score$education,
                Education_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "education"],
                EnviroEnergie_words_count = df_stakes_score$environment + df_stakes_score$energy,
                EnviroEnergie_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
                AffIntlDefense_words_count = df_stakes_score$defence + df_stakes_score$intl_affairs,
                AffIntlDefense_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
                qc_caq_words_count = df_polparties_score$caq,
                qc_caq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "caq"],
                qc_pq_words_count = df_polparties_score$pq,
                qc_pq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "pq"],
                qc_plq_words_count = df_polparties_score$plq,
                qc_plq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "plq"],
                qc_qs_words_count = df_polparties_score$qs,
                qc_qs_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "qs"],
                can_lpc_words_count = df_polparties_score$lpc,
                can_lpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "lpc"],
                can_cpc_words_count = df_polparties_score$cpc,
                can_cpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "cpc"],
                can_npd_words_count = df_polparties_score$ndp,
                can_npd_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ndp"],
                can_bq_words_count = df_polparties_score$bq,
                can_bq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "bq"],
                can_gpc_words_count = df_polparties_score$gpc,
                can_gpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "gpc"],
                can_ppc_words_count = df_polparties_score$ppc,
                can_ppc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ppc"],
                headline_mins_duration = as.numeric(difftime(as.POSIXct(df_radarplus$end_date[i]), as.POSIXct(df_radarplus$begin_date[i]), units="mins")),
                content_sentences_count = compute_nb_sentences(df_radarplus$content[i]),
                content_words_count = compute_nb_words(df_radarplus$content[i]),
                stringsAsFactors = F
            )

    df <- df %>% rbind(row)

    cat('committing', digest::digest(df_radarplus$liens[i]), '\n')
    commit_hub_row(my_table, digest::digest(df_radarplus$liens[i]), row, "refresh", credentials)

}
 



