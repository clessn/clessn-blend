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

    filter <- list(key__exact = key)
    item <- hubr::filter_table_items(table, credentials, filter)

    if(is.null(item)) {
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
        # l'item existe déjè dans hublot
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
my_table <- "clhub_tables_datamart_vd_shiny_medias_prototype"
hubr::count_table_items(my_table, credentials) 


page <- hubr::list_table_items(my_table, credentials) 
data <- list() 
repeat {
    data <- c(data, page$results)
    page <- hubr::list_next(page, credentials)
    if (is.null(page)) {
        break
    }
}
datamart <- tidyjson::spread_all(data) # on convertir maintenant les données en tibble

clessnverse::logit(scriptname=scriptname, message=paste("clhub_tables_datamart_vd_shiny_medias_prototype dataframe contains", nrow(datamart)), logger=logger)



###############################################################################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################

# Getting journalists
filter <- clessnhub::create_filter(type="journalist")
df_journalists <- clessnhub::get_items('persons', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Journalists dataframe contains", nrow(df_journalists)), logger=logger)

# Getting tweets from journalists of feb 2022
filter <- clessnhub::create_filter(metadata = list("personType"="journalist"), data=list("creationDate__gte"="2022-01-24", "creationDate__lte"="2022-01-25"))
df_tweets <- clessnhub::get_items('tweets', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Tweets dataframe contains", nrow(df_tweets)), logger=logger)

# Getting agoraplus press conf interventions from journalists of feb 2022
filter <- clessnhub::create_filter(type="press_conference", metadata=list("location"="CA-QC"), data=list("eventDate__gte"="2022-01-24", "eventDate__lte"="2022-01-25", speakerType="journalist"))
df_interventions <- clessnhub::get_items('agoraplus_interventions', filter)
clessnverse::logit(scriptname=scriptname, message=paste("Agoraplus press_conference interventions dataframe contains", nrow(df_interventions)), logger=logger)



###############################################################################
#################   Get Data Sources from Files in Hub 3.0   ##################
###############################################################################
# Getting the lexicoder of stakes of the vitrine democratique
file_info <- hubr::retrieve_file("dictionnaire_LexicoderEN-FR_sentiment", credentials)
sentiment_dictionary <- read.csv2(file_info$file)
dict_list <- list()
for (i in unique(sentiment_dictionary$categorie)) {
    dict_list[[i]] <- sentiment_dictionary$item[sentiment_dictionary$categorie == i]
}
sentiment_dictionary <- quanteda::dictionary(dict_list)


file_info <- hubr::retrieve_file("dictionnaire_LexicoderFR-enjeux", credentials)
stakes_dictionary_fr <- read.csv(file_info$file)
stakes_dictionary_fr$X <- NULL
dict_list <- list()
for (i in unique(stakes_dictionary_fr$categorie)) {
    dict_list[[i]] <- stakes_dictionary_fr$item[stakes_dictionary_fr$categorie == i]
}
stakes_dictionary_fr <- quanteda::dictionary(dict_list)


file_info <- hubr::retrieve_file("dictionnaire_LexicoderEN-enjeux", credentials)
stakes_dictionary_en <- read.csv(file_info$file)
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
file_info <- hubr::retrieve_file("dictionnaire_politiqueCAN", credentials)
polparties_dictionary_can <- read.csv2(file_info$file)
polparties_dictionary_can$X <- NULL
dict_list <- list()
for (i in unique(polparties_dictionary_can$categorie)) {
    dict_list[[i]] <- polparties_dictionary_can$item[polparties_dictionary_can$categorie == i]
}
polparties_dictionary_can <- quanteda::dictionary(dict_list)


file_info <- hubr::retrieve_file("dictionnaire_politiqueQC", credentials)
polparties_dictionary_qc <- read.csv(file_info$file)
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
    LoiCrime_relevance = double(), LoiCrime_sentiment = double(),
    CultNation_relevance = double(), CultNation_sentiment = double(),
    TerresPubAgri_relevance = double(), TerresPubAgri_sentiment = double(),
    GvntGvnc_relevance = double(), GvntGvnc_sentiment = double(),
    Immigration_relevance = double(), Immigration_sentiment = double(),
    DroitsEtc_relevance = double(), DroitsEtc_sentiment = double(),
    SanteServSoc_relevance = double(), SanteServSoc_sentiment = double(),
    EconomieTravail_relevance = double(), EconomieTravail_sentiment = double(),
    Education_relevance = double(), Education_sentiment = double(),
    EnviroEnergie_relevance = double(), EnviroEnergie_sentiment = double(),
    AffIntlDefense_relevance = double(), AffIntlDefense_sentiment = double(),
    qc_caq_relevance = double(), qc_caq_sentiment = double(),
    qc_pq_relevance = double(), qc_pq_sentiment = double(),
    qc_plq_relevance = double(), qc_plq_sentiment = double(),
    qc_qs_relevance = double(), qc_qs_sentiment = double(),
    can_lpc_relevance = double(), can_lpc_sentiment = double(),
    can_cpc_relevance = double(), can_cpc_sentiment = double(),
    can_npd_relevance = double(), can_npd_sentiment = double(),
    can_bq_relevance = double(), can_bq_sentiment = double(),
    can_gpc_relevance = double(), can_gpc_sentiment = double(),
    can_ppc_relevance = double(), can_ppc_sentiment = double(),
    headline_mins_duration = double(),
    nb_sentences = double(),
    nb_words = double(),
    stringsAsFactors = F
)

# Agoraplus 

for (i in 1:nrow(df_interventions)) {
    # get the journalist attributes
    if (!is.na(df_interventions$data.speakerFullName[i])) {
        first_name <- strsplit(df_interventions$data.speakerFullName[i], ",")[[1]][2]
        last_name <- strsplit(df_interventions$data.speakerFullName[i], ",")[[1]][1]

        first_name <- trimws(first_name)
        last_name <- trimws(last_name)

        person_filter <- clessnhub::create_filter(data=list("firstName" = first_name, "lastName" = last_name))
        person <- clessnhub::get_items('persons', person_filter) 


        if (is.null(person)) {
            first_name <- strsplit(df_interventions$data.speakerFullName[i], " ")[[1]][1]
            last_name <- strsplit(df_interventions$data.speakerFullName[i], " ")[[1]][2]

            first_name <- trimws(first_name)
            last_name <- trimws(last_name)

            person_filter <- clessnhub::create_filter(data=list("firstName" = first_name, "lastName" = last_name))
            person <- clessnhub::get_items('persons', person_filter) 
        }

        if (is.null(person)) {
            person_filter <- clessnhub::create_filter(data=list("fullName" = df_interventions$data.speakerFullName[i]))
            person <- clessnhub::get_items('persons', person_filter) 
        }
    } 
    
    if (is.null(person)) {
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
                        LoiCrime_relevance = df_stakes_score$crime,
                        LoiCrime_sentiment = df_sentiments$sentiment[df_sentiments$category == "crime"],
                        CultNation_relevance = df_stakes_score$culture,
                        CultNation_sentiment = df_sentiments$sentiment[df_sentiments$category == "culture"],
                        TerresPubAgri_relevance = df_stakes_score$agriculture + df_stakes_score$'land-water-management' + df_stakes_score$fisheries + df_stakes_score$forestry,
                        TerresPubAgri_sentiment = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
                        GvntGvnc_relevance = df_stakes_score$government_ops + df_stakes_score$prov_local + df_stakes_score$intergovernmental + df_stakes_score$constitutional_natl_unity,
                        GvntGvnc_sentiment = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
                        Immigration_relevance = df_stakes_score$immigration,
                        Immigration_sentiment = df_sentiments$sentiment[df_sentiments$category == "immigration"],
                        DroitsEtc_relevance = df_stakes_score$civil_rights + df_stakes_score$religion + df_stakes_score$aboriginal,
                        DroitsEtc_sentiment = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
                        SanteServSoc_relevance = df_stakes_score$healthcare + df_stakes_score$social_welfare,
                        SanteServSoc_sentiment = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
                        EconomieTravail_relevance = df_stakes_score$macroeconomics + df_stakes_score$labour + df_stakes_score$foreign_trade + df_stakes_score$sstc + df_stakes_score$finance + df_stakes_score$housing + df_stakes_score$transportation,
                        EconomieTravail_sentiment = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
                        Education_relevance = df_stakes_score$education,
                        Education_sentiment = df_sentiments$sentiment[df_sentiments$category == "education"],
                        EnviroEnergie_relevance = df_stakes_score$environment + df_stakes_score$energy,
                        EnviroEnergie_sentiment = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
                        AffIntlDefense_relevance = df_stakes_score$defence + df_stakes_score$intl_affairs,
                        AffIntlDefense_sentiment = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
                        qc_caq_relevance = df_polparties_score$caq,
                        qc_caq_sentiment = df_sentiments$sentiment[df_sentiments$category == "caq"],
                        qc_pq_relevance = df_polparties_score$pq,
                        qc_pq_sentiment = df_sentiments$sentiment[df_sentiments$category == "pq"],
                        qc_plq_relevance = df_polparties_score$plq,
                        qc_plq_sentiment = df_sentiments$sentiment[df_sentiments$category == "plq"],
                        qc_qs_relevance = df_polparties_score$qs,
                        qc_qs_sentiment = df_sentiments$sentiment[df_sentiments$category == "qs"],
                        can_lpc_relevance = df_polparties_score$lpc,
                        can_lpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "lpc"],
                        can_cpc_relevance = df_polparties_score$cpc,
                        can_cpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "cpc"],
                        can_npd_relevance = df_polparties_score$ndp,
                        can_npd_sentiment = df_sentiments$sentiment[df_sentiments$category == "ndp"],
                        can_bq_relevance = df_polparties_score$bq,
                        can_bq_sentiment = df_sentiments$sentiment[df_sentiments$category == "bq"],
                        can_gpc_relevance = df_polparties_score$gpc,
                        can_gpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "gpc"],
                        can_ppc_relevance = df_polparties_score$ppc,
                        can_ppc_sentiment = df_sentiments$sentiment[df_sentiments$category == "ppc"],
                        headline_mins_duration = NA,
                        nb_sentences = compute_nb_sentences(df_interventions$data.interventionText[i]),
                        nb_words = compute_nb_words(df_interventions$data.interventionText[i]),
                        stringsAsFactors = F
                        )

    df <- df %>% rbind(row)
                      
   commit_hub_row(my_table, df_interventions$key[i], row, "refresh", credentials)
}


# Twitter

#for (i in 1:nrow(df_tweets)) {
for (i in 1:100) {
    person <- clessnhub::get_item('persons', df_tweets$data.personKey[i]) 

    df_stakes_score <- compute_relevance_score(df_tweets$data.text[i], stakes_dictionary)
    df_polparties_score <- compute_relevance_score(df_tweets$data.text[i], polparties_dictionary)
    df_sentiments <- compute_catergory_sentiment_score(df_tweets$data.text[i], c(stakes_dictionary, polparties_dictionary), sentiment_dictionary)

    row <- (data.frame(
                    source=df_tweets$type[i], author_id = df_tweets$data.personKey[i], 
                    author_name = person$data$fullName, author_gender = if (!is.na(person$data$isFemale) && person$data$isFemale == 1) "F" else "M",
                    author_media = person$data$currentMedia, content = df_tweets$data.text[i], 
                    content_date = df_tweets$data.creationDate[i],
                    LoiCrime_relevance = df_stakes_score$crime,
                    LoiCrime_sentiment = df_sentiments$sentiment[df_sentiments$category == "crime"],
                    CultNation_relevance = df_stakes_score$culture,
                    CultNation_sentiment = df_sentiments$sentiment[df_sentiments$category == "culture"],
                    TerresPubAgri_relevance = df_stakes_score$agriculture + df_stakes_score$'land-water-management' + df_stakes_score$fisheries + df_stakes_score$forestry,
                    TerresPubAgri_sentiment = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
                    GvntGvnc_relevance = df_stakes_score$government_ops + df_stakes_score$prov_local + df_stakes_score$intergovernmental + df_stakes_score$constitutional_natl_unity,
                    GvntGvnc_sentiment = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
                    Immigration_relevance = df_stakes_score$immigration,
                    Immigration_sentiment = df_sentiments$sentiment[df_sentiments$category == "immigration"],
                    DroitsEtc_relevance = df_stakes_score$civil_rights + df_stakes_score$religion + df_stakes_score$aboriginal,
                    DroitsEtc_sentiment = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
                    SanteServSoc_relevance = df_stakes_score$healthcare + df_stakes_score$social_welfare,
                    SanteServSoc_sentiment = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
                    EconomieTravail_relevance = df_stakes_score$macroeconomics + df_stakes_score$labour + df_stakes_score$foreign_trade + df_stakes_score$sstc + df_stakes_score$finance + df_stakes_score$housing + df_stakes_score$transportation,
                    EconomieTravail_sentiment = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
                    Education_relevance = df_stakes_score$education,
                    Education_sentiment = df_sentiments$sentiment[df_sentiments$category == "education"],
                    EnviroEnergie_relevance = df_stakes_score$environment + df_stakes_score$energy,
                    EnviroEnergie_sentiment = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
                    AffIntlDefense_relevance = df_stakes_score$defence + df_stakes_score$intl_affairs,
                    AffIntlDefense_sentiment = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
                    qc_caq_relevance = df_polparties_score$caq,
                    qc_caq_sentiment = df_sentiments$sentiment[df_sentiments$category == "caq"],
                    qc_pq_relevance = df_polparties_score$pq,
                    qc_pq_sentiment = df_sentiments$sentiment[df_sentiments$category == "pq"],
                    qc_plq_relevance = df_polparties_score$plq,
                    qc_plq_sentiment = df_sentiments$sentiment[df_sentiments$category == "plq"],
                    qc_qs_relevance = df_polparties_score$qs,
                    qc_qs_sentiment = df_sentiments$sentiment[df_sentiments$category == "qs"],
                    can_lpc_relevance = df_polparties_score$lpc,
                    can_lpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "lpc"],
                    can_cpc_relevance = df_polparties_score$cpc,
                    can_cpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "cpc"],
                    can_npd_relevance = df_polparties_score$ndp,
                    can_npd_sentiment = df_sentiments$sentiment[df_sentiments$category == "ndp"],
                    can_bq_relevance = df_polparties_score$bq,
                    can_bq_sentiment = df_sentiments$sentiment[df_sentiments$category == "bq"],
                    can_gpc_relevance = df_polparties_score$gpc,
                    can_gpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "gpc"],
                    can_ppc_relevance = df_polparties_score$ppc,
                    can_ppc_sentiment = df_sentiments$sentiment[df_sentiments$category == "ppc"],
                    headline_mins_duration = NA,
                    nb_sentences = compute_nb_sentences(df_tweets$data.text[i]),
                    nb_words = compute_nb_words(df_tweets$data.text[i]),
                    stringsAsFactors = F
                )
                )

    df <- df %>% rbind(row)

  
    commit_hub_row(my_table, df_tweets$key[i], row, "refresh", credentials)

}


# Radar plus

#for (i in 1:nrow(df_radarplus)){
for (i in 1:100){

    # get the journalist attributes
    if (!is.na(df_radarplus$author[i])) {
        person_filter <- clessnhub::create_filter(data=list("fullName" = df_radarplus$author[i]))
        person <- clessnhub::get_items('persons', person_filter) 

        if (is.null(person)) {
            first_name <- strsplit(df_radarplus$author[i], " ")[[1]][1]
            last_name <- strsplit(df_radarplus$author[i], " ")[[1]][2]
            person_filter <- clessnhub::create_filter(data=list("firstName" = first_name, "lastName" = last_name))
            person <- clessnhub::get_items('persons', person_filter) 
        }

        if (is.null(person)) {
            person_filter <- clessnhub::create_filter(data=list("twitterName" = df_radarplus$author[i]))
            person <- clessnhub::get_items('persons', person_filter) 
        }
    } 
    
    if (is.null(person)) {
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
                LoiCrime_relevance = df_stakes_score$crime,
                LoiCrime_sentiment = df_sentiments$sentiment[df_sentiments$category == "crime"],
                CultNation_relevance = df_stakes_score$culture,
                CultNation_sentiment = df_sentiments$sentiment[df_sentiments$category == "culture"],
                TerresPubAgri_relevance = df_stakes_score$agriculture + df_stakes_score$'land-water-management' + df_stakes_score$fisheries + df_stakes_score$forestry,
                TerresPubAgri_sentiment = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
                GvntGvnc_relevance = df_stakes_score$government_ops + df_stakes_score$prov_local + df_stakes_score$intergovernmental + df_stakes_score$constitutional_natl_unity,
                GvntGvnc_sentiment = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
                Immigration_relevance = df_stakes_score$immigration,
                Immigration_sentiment = df_sentiments$sentiment[df_sentiments$category == "immigration"],
                DroitsEtc_relevance = df_stakes_score$civil_rights + df_stakes_score$religion + df_stakes_score$aboriginal,
                DroitsEtc_sentiment = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
                SanteServSoc_relevance = df_stakes_score$healthcare + df_stakes_score$social_welfare,
                SanteServSoc_sentiment = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
                EconomieTravail_relevance = df_stakes_score$macroeconomics + df_stakes_score$labour + df_stakes_score$foreign_trade + df_stakes_score$sstc + df_stakes_score$finance + df_stakes_score$housing + df_stakes_score$transportation,
                EconomieTravail_sentiment = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
                Education_relevance = df_stakes_score$education,
                Education_sentiment = df_sentiments$sentiment[df_sentiments$category == "education"],
                EnviroEnergie_relevance = df_stakes_score$environment + df_stakes_score$energy,
                EnviroEnergie_sentiment = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
                AffIntlDefense_relevance = df_stakes_score$defence + df_stakes_score$intl_affairs,
                AffIntlDefense_sentiment = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
                qc_caq_relevance = df_polparties_score$caq,
                qc_caq_sentiment = df_sentiments$sentiment[df_sentiments$category == "caq"],
                qc_pq_relevance = df_polparties_score$pq,
                qc_pq_sentiment = df_sentiments$sentiment[df_sentiments$category == "pq"],
                qc_plq_relevance = df_polparties_score$plq,
                qc_plq_sentiment = df_sentiments$sentiment[df_sentiments$category == "plq"],
                qc_qs_relevance = df_polparties_score$qs,
                qc_qs_sentiment = df_sentiments$sentiment[df_sentiments$category == "qs"],
                can_lpc_relevance = df_polparties_score$lpc,
                can_lpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "lpc"],
                can_cpc_relevance = df_polparties_score$cpc,
                can_cpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "cpc"],
                can_npd_relevance = df_polparties_score$ndp,
                can_npd_sentiment = df_sentiments$sentiment[df_sentiments$category == "ndp"],
                can_bq_relevance = df_polparties_score$bq,
                can_bq_sentiment = df_sentiments$sentiment[df_sentiments$category == "bq"],
                can_gpc_relevance = df_polparties_score$gpc,
                can_gpc_sentiment = df_sentiments$sentiment[df_sentiments$category == "gpc"],
                can_ppc_relevance = df_polparties_score$ppc,
                can_ppc_sentiment = df_sentiments$sentiment[df_sentiments$category == "ppc"],
                headline_mins_duration = as.numeric(difftime(as.POSIXct(df_radarplus$end_date[i]), as.POSIXct(df_radarplus$begin_date[i]), units="mins")),
                nb_sentences = compute_nb_sentences(df_radarplus$content[i]),
                nb_words = compute_nb_words(df_radarplus$content[i]),
                stringsAsFactors = F
            )

    df <- df %>% rbind(row)

    
    commit_hub_row(my_table, digest::digest(df_radarplus$liens[i]), row, "refresh", credentials)

}




