###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                    <r_medias_issues_parties_sentiment_scores>               #
#                                                                             #
# This script extracts the tweets from journalists, the interventions from    #
# journalists in the Quebec National Assembly press conferences and the       #
# headlines from the main canadian medias and calculates the number of
# words relating to each issue (as per CLESSN dictionary) as well as the 
# number of words in the pos, neg, neg_pos and neg_neg categories of the 
# sentiment lexicoder
#
# Currently, the tweets and quebec assembly press conferences are in hub 2.0
# and the media headlines are in a csv file
#
# Eventually, they will all come from hublot
#                                                                             #
###############################################################################


###############################################################################
########################           Functions            ######################
###############################################################################


###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################


get_radarplus_headlines <- function() {
  data <- hublot::filter_lake_items(credentials, list(key = "dataradar_20210101_20220623_fr"))
  df <- tidyjson::spread_all(data$results)
  url <- df$..JSON[[1]]$file
  df <- read.csv2(url)

  clessnverse::logit(scriptname=scriptname, message=paste("Radarplus headlines dataframe contains", nrow(df)), logger=logger)


  return(df)
}




###############################################################################
######################            Functions to           ######################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################

get_journalists <- function() {
  #filter <- clessnhub::create_filter(type="journalist")
  #df <- clessnverse::get_items('persons', filter)

  filter <- list(type="journalist")
  df <- clessnverse::get_hub2_table(
    'persons', 
    data_filter = filter,
    max_pages = -1,
    hub_conf = hub_config
  )
  clessnverse::logit(scriptname=scriptname, message=paste("Journalists dataframe contains", nrow(df)), logger=logger)

  return(df)
}


get_confpress_journalists_interventions <- function() {
  filter <- list(
    type = "press_conference",
    metadata__location = "CA-QC",
    data__speakerType = "journalist",
    data__eventDate__gte = "2022-08-28",
    data__eventDate__lte = "2022-09-08"
  )

  #df <- clessnhub::get_items('agoraplus_interventions', filter)
  df <- clessnverse::get_hub2_table(
    table_name = 'agoraplus_interventions', 
    data_filter = filter, 
    max_pages = -1, 
    hub_conf = hub_config)

  clessnverse::logit(scriptname=scriptname, message=paste("agoraplus interventions dataframe contains", nrow(df)), logger=logger)


  return(df)
}


get_journalists_tweets <- function() {
    filter = list(
    metadata__personType="journalist",
    data__creationDate__gte="2022-08-28",
    data__creationDate__lte="2022-09-08"
    # most field lookups here should work
    # https://docs.djangoproject.com/en/4.0/ref/models/querysets/#field-lookups
  )

  df <- clessnverse::get_hub2_table(
    table_name = 'tweets', 
    data_filter = filter, 
    max_pages = -1, 
    hub_conf = hub_config)

  # filter = clessnhub::create_filter(
  #   metadata = list(personType="journalist"),
  #   data = list(
  #     creationDate__gte = "2021-01-01",
  #     creationDate__lte = "2022-06-23"
  #   )
  # )

  # df <- clessnhub::get_items('tweets', filter)
  #   clessnverse::logit(scriptname=scriptname, message=paste("tweets dataframe contains", nrow(df)), logger=logger)


  return(df)
}




###############################################################################
######################            Functions to          ######################
######################   Get Data Sources from Dropbox   ######################
###############################################################################




###############################################################################
########################               Main              ######################
## This function is the core of your script. It can use global R objects and ##
## variables that you can define in the tryCatch section of the script below ##
###############################################################################

main <- function() {
  ###########################################################################
  # Define local objects of your core algorithm here
  # Ex: parties_list <- list("CAQ", "PLQ", "QS", "PCQ", "PQ")


  ###########################################################################
  # Start your main script here using the best practices in activity logging
  #
  # Ex: warehouse_items_list <- clessnverse::get_warehouse_table(warehouse_table, credentials, nbrows = 0)
  #     clessnverse::logit(scriptname, "Getting political parties press releases from the datalake", logger) 
  #     lakes_items_list <- get_lake_press_releases(parties_list)
  #      
  
  # Get all dictionaries
  issues_dict <- clessnverse::get_dictionary('issues', c("en","fr"), credentials)
  sentiment_dict <- clessnverse::get_dictionary('sentiments', c("en","fr"), credentials)
  parties_can_dict <- clessnverse::get_dictionary('political_parties_can', c("en","fr"), credentials)
  parties_qc_dict <- clessnverse::get_dictionary('political_parties_qc', c("en","fr"), credentials)
  parties_dict <- c(parties_can_dict,parties_qc_dict)

  # Extract our data

  df_journalists <- get_journalists()

  df_interventions <- get_confpress_journalists_interventions()
  
  df_tweets <- get_journalists_tweets()

  # # return a list of elements structured as named lists
  # data <- extract_data(
  #   "tweets", # the table name, can be tweets, agoraplus_press_releases, persons, or any other
  #   jsonlite::toJSON(filter, auto_unbox = T), # the auto_unbox is really important
  #   max_pages = -1 # set to a positive number x to extraxt x * 1000 items only
  # )

  # df_tweets <-  tidyjson::spread_all(data)

  df_headlines <- get_radarplus_headlines()

  datamart  <- clessnverse::get_mart_table(datamart_table_name, credentials)

  # Decleare our dataframe with the proper columns names
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


  # Enrich and load journalists interventions into datamart
  for (i in 1:nrow(df_interventions)) {
    # get the journalist attributes
    person <- NULL

    if (!is.na(df_interventions$speakerFullName[i])) {
        first_name <- strsplit(df_interventions$speakerFullName[i], ",")[[1]][2]
        last_name <- strsplit(df_interventions$speakerFullName[i], ",")[[1]][1]

        first_name <- trimws(first_name)
        last_name <- trimws(last_name)

        person <- df_journalists[which(df_journalists$firstName == first_name & df_journalists$lastName == last_name),]

        if (nrow(person) == 0) {
            first_name <- strsplit(df_interventions$speakerFullName[i], " ")[[1]][1]
            last_name <- strsplit(df_interventions$speakerFullName[i], " ")[[1]][2]

            first_name <- trimws(first_name)
            last_name <- trimws(last_name)

            person <- df_journalists[which(df_journalists$firstName == first_name & df_journalists$lastName == last_name),]
        }

        if (nrow(person) == 0) {
            person_filter <- clessnhub::create_filter(data=list("fullName" = df_interventions$speakerFullName[i]))
            person <- df_journalists[which(df_journalists$fullName == df_interventions$speakerFullName[i]),]
        }
    }

    if (is.null(person) || nrow(person) == 0) {
        person <- data.frame(hub.key=NA, hub.type="journalist", fullName=df_interventions$speakerFullName[i], 
                             isFemale=df_interventions$speakerGender[i], currentMedia=df_interventions$speakerMedia[i])
    }

    person <- person[1,]

    #df_issues_score <- clessnverse::compute_relevance_score(df_interventions$data.interventionText[i], issues_dict)
    #df_parties_score <- clessnverse::compute_relevance_score(df_interventions$data.interventionText[i], parties_dict)
    df_issues_score <- clessnverse::run_dictionary(data.frame(text = df_interventions$interventionText[i]), text, issues_dict)
    df_parties_score <- clessnverse::run_dictionary(data.frame(text = df_interventions$interventionText[i]), text, parties_dict)
    df_sentiments <- clessnverse::compute_category_sentiment_score(df_interventions$interventionText[i], c(issues_dict, parties_dict), sentiment_dict)

    row <- data.frame(
      source=df_interventions$type[i], author_id = person$hub.key, 
      author_name = person$fullName, author_gender = if (!is.na(person$isFemale) && person$isFemale == 1) "F" else "M",
      author_media = person$currentMedia, content = df_interventions$interventionText[i], 
      content_date = df_interventions$eventDate[i],
      LoiCrime_words_count = df_issues_score$crime,
      LoiCrime_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "crime"],
      CultNation_words_count = df_issues_score$culture,
      CultNation_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "culture"],
      TerresPubAgri_words_count = df_issues_score$agriculture + df_issues_score$'land-water-management' + df_issues_score$fisheries + df_issues_score$forestry,
      TerresPubAgri_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
      GvntGvnc_words_count = df_issues_score$government_ops + df_issues_score$prov_local + df_issues_score$intergovernmental + df_issues_score$constitutional_natl_unity,
      GvntGvnc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
      Immigration_words_count = df_issues_score$immigration,
      Immigration_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "immigration"],
      DroitsEtc_words_count = df_issues_score$civil_rights + df_issues_score$religion + df_issues_score$aboriginal,
      DroitsEtc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
      SanteServSoc_words_count = df_issues_score$healthcare + df_issues_score$social_welfare,
      SanteServSoc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
      EconomieTravail_words_count = df_issues_score$macroeconomics + df_issues_score$labour + df_issues_score$foreign_trade + df_issues_score$sstc + df_issues_score$finance + df_issues_score$housing + df_issues_score$transportation,
      EconomieTravail_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
      Education_words_count = df_issues_score$education,
      Education_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "education"],
      EnviroEnergie_words_count = df_issues_score$environment + df_issues_score$energy,
      EnviroEnergie_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
      AffIntlDefense_words_count = df_issues_score$defence + df_issues_score$intl_affairs,
      AffIntlDefense_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
      qc_caq_words_count = df_parties_score$caq,
      qc_caq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "caq"],
      qc_pq_words_count = df_parties_score$pq,
      qc_pq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "pq"],
      qc_plq_words_count = df_parties_score$plq,
      qc_plq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "plq"],
      qc_qs_words_count = df_parties_score$qs,
      qc_qs_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "qs"],
      can_lpc_words_count = df_parties_score$lpc,
      can_lpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "lpc"],
      can_cpc_words_count = df_parties_score$cpc,
      can_cpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "cpc"],
      can_npd_words_count = df_parties_score$ndp,
      can_npd_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ndp"],
      can_bq_words_count = df_parties_score$bq,
      can_bq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "bq"],
      can_gpc_words_count = df_parties_score$gpc,
      can_gpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "gpc"],
      can_ppc_words_count = df_parties_score$ppc,
      can_ppc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ppc"],
      headline_mins_duration = NA,
      content_sentences_count = clessnverse::compute_nb_sentences(df_interventions$interventionText[i]),
      content_words_count = clessnverse::compute_nb_words(df_interventions$interventionText[i]),
      stringsAsFactors = F
      )

    df <- df %>% rbind(row)

    clessnverse::logit(scriptname, paste('committing', df_interventions$key[i], '\n'), logger)
    clessnverse::commit_mart_row(datamart_table_name, df_interventions$key[i], as.list(row), opt$hub_mode, credentials)

  }

  # Enrich and load tweets into datamart
  for (i in 612034:nrow(df_tweets)) {
    person <- NULL

    person <- df_journalists[which(df_journalists$hub.key == df_tweets$personKey[i]),]

    if (nrow(person) == 0) {
      next
    }

    #df_issues_score <- clessnverse::compute_relevance_score(df_tweets$data.text[i], issues_dict)
    #df_parties_score <- clessnverse::compute_relevance_score(df_tweets$data.text[i], parties_dict)
    df_issues_score <- clessnverse::run_dictionary(data.frame(text=df_tweets$text[i]), text, issues_dict, verbose = F)
    df_parties_score <- clessnverse::run_dictionary(data.frame(text=df_tweets$text[i]), text, parties_dict, verbose = F)
    df_sentiments <- clessnverse::compute_category_sentiment_score(df_tweets$text[i], c(issues_dict, parties_dict), sentiment_dict)

    row <- data.frame(
      source=df_tweets$hub.type[i], author_id = df_tweets$personKey[i], 
          author_name = person$fullName, author_gender = if (!is.na(person$isFemale) && person$isFemale == 1) "F" else "M",
          author_media = person$currentMedia, content = df_tweets$text[i], 
          content_date = df_tweets$creationDate[i],
          LoiCrime_words_count = df_issues_score$crime,
          LoiCrime_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "crime"],
          CultNation_words_count = df_issues_score$culture,
          CultNation_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "culture"],
          TerresPubAgri_words_count = df_issues_score$agriculture + df_issues_score$'land-water-management' + df_issues_score$fisheries + df_issues_score$forestry,
          TerresPubAgri_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
          GvntGvnc_words_count = df_issues_score$government_ops + df_issues_score$prov_local + df_issues_score$intergovernmental + df_issues_score$constitutional_natl_unity,
          GvntGvnc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
          Immigration_words_count = df_issues_score$immigration,
          Immigration_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "immigration"],
          DroitsEtc_words_count = df_issues_score$civil_rights + df_issues_score$religion + df_issues_score$aboriginal,
          DroitsEtc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
          SanteServSoc_words_count = df_issues_score$healthcare + df_issues_score$social_welfare,
          SanteServSoc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
          EconomieTravail_words_count = df_issues_score$macroeconomics + df_issues_score$labour + df_issues_score$foreign_trade + df_issues_score$sstc + df_issues_score$finance + df_issues_score$housing + df_issues_score$transportation,
          EconomieTravail_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
          Education_words_count = df_issues_score$education,
          Education_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "education"],
          EnviroEnergie_words_count = df_issues_score$environment + df_issues_score$energy,
          EnviroEnergie_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
          AffIntlDefense_words_count = df_issues_score$defence + df_issues_score$intl_affairs,
          AffIntlDefense_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
          qc_caq_words_count = df_parties_score$caq,
          qc_caq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "caq"],
          qc_pq_words_count = df_parties_score$pq,
          qc_pq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "pq"],
          qc_plq_words_count = df_parties_score$plq,
          qc_plq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "plq"],
          qc_qs_words_count = df_parties_score$qs,
          qc_qs_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "qs"],
          can_lpc_words_count = df_parties_score$lpc,
          can_lpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "lpc"],
          can_cpc_words_count = df_parties_score$cpc,
          can_cpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "cpc"],
          can_npd_words_count = df_parties_score$ndp,
          can_npd_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ndp"],
          can_bq_words_count = df_parties_score$bq,
          can_bq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "bq"],
          can_gpc_words_count = df_parties_score$gpc,
          can_gpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "gpc"],
          can_ppc_words_count = df_parties_score$ppc,
          can_ppc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ppc"],
          headline_mins_duration = NA,
          content_sentences_count = clessnverse::compute_nb_sentences(df_tweets$text[i]),
          content_words_count = clessnverse::compute_nb_words(df_tweets$text[i]),
          stringsAsFactors = F
      )

    df <- df %>% rbind(row)

    clessnverse::logit(scriptname, paste('committing', df_tweets$hub.key[i]), logger)
    clessnverse::commit_mart_row(datamart_table_name, df_tweets$hub.key[i], as.list(row), opt$hub_mode, credentials)
  }

  # Enrich and load headlines into datamart
  for (i in 1:nrow(df_headlines)) {
    if (is.na(df_headlines$content[i]) || df_headlines$content[i] == "") next

    person <- NULL

    # get the journalist attributes
    if (!is.na(df_headlines$author[i])) {
        df_headlines$author[i] <- gsub(":", "", df_headlines$author[i])
        df_headlines$author[i] <- gsub("Texte", "", df_headlines$author[i])
        df_headlines$author[i] <- trimws(df_headlines$author[i])
        
        person <- df_journalists[which(df_journalists$fullName == df_headlines$author[i]),]

        if (nrow(person) == 0) {
            first_name <- strsplit(df_headlines$author[i], " ")[[1]][1]
            last_name <- strsplit(df_headlines$author[i], " ")[[1]][2]
            person <- df_journalists[which(df_journalists$firstName == first_name & df_journalists$lastName == last_name),]
        }

        if (nrow(person) == 0) {
            person <- df_journalists[which(df_journalists$twitterName == df_headlines$author[i]),]
        }
    } 
    
    if (is.null(person) || nrow(person) == 0) {
        person <- data.frame(hub.key=NA, hub.type=NA, fullName=NA, isFemale=NA, currentMedia=df_headlines$media[i])
    }

    person <- person[1,]

    df_issues_score <- clessnverse::compute_relevance_score(data.frame(text=df_headlines$content[i]), text, issues_dict)
    df_parties_score <- clessnverse::compute_relevance_score(data.frame(text=df_headlines$content[i]), text, df_headlines$content[i], parties_dict)
    df_sentiments <- clessnverse::compute_category_sentiment_score(df_headlines$content[i], c(issues_dict, parties_dict), sentiment_dict)

    row <- data.frame(
      source=if (!is.na(person$type) && person$type == "media") person$fullName else person$currentMedia,
          author_id = person$hub.key,
          author_name = if (!is.na(person$fullName)) person$fullName else person$currentMedia,
          author_gender = if ("data.isFemale" %in% names(person) && !is.na(person$isFemale) && person$isFemale == 1) {"F"} else {if ("data.isFemale" %in% names(person)) "M" else NA},
          author_media = if (!is.na(person$type) && person$type == "media") person$fullName else person$currentMedia,
          content = df_headlines$content[i],
          content_date = df_headlines$begin_date[i],
          LoiCrime_words_count = df_issues_score$crime,
          LoiCrime_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "crime"],
          CultNation_words_count = df_issues_score$culture,
          CultNation_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "culture"],
          TerresPubAgri_words_count = df_issues_score$agriculture + df_issues_score$'land-water-management' + df_issues_score$fisheries + df_issues_score$forestry,
          TerresPubAgri_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "agriculture"] + df_sentiments$sentiment[df_sentiments$category == "land-water-management"] + df_sentiments$sentiment[df_sentiments$category == "fisheries"] + df_sentiments$sentiment[df_sentiments$category == "forestry"],
          GvntGvnc_words_count = df_issues_score$government_ops + df_issues_score$prov_local + df_issues_score$intergovernmental + df_issues_score$constitutional_natl_unity,
          GvntGvnc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "government_ops"] + df_sentiments$sentiment[df_sentiments$category == "prov_local"] + df_sentiments$sentiment[df_sentiments$category == "intergovernmental"] + df_sentiments$sentiment[df_sentiments$category == "constitutional_natl_unity"],
          Immigration_words_count = df_issues_score$immigration,
          Immigration_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "immigration"],
          DroitsEtc_words_count = df_issues_score$civil_rights + df_issues_score$religion + df_issues_score$aboriginal,
          DroitsEtc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "civil_rights"] + df_sentiments$sentiment[df_sentiments$category == "religion"] + df_sentiments$sentiment[df_sentiments$category == "aboriginal"],
          SanteServSoc_words_count = df_issues_score$healthcare + df_issues_score$social_welfare,
          SanteServSoc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "healthcare"] + df_sentiments$sentiment[df_sentiments$category == "social_welfare"],
          EconomieTravail_words_count = df_issues_score$macroeconomics + df_issues_score$labour + df_issues_score$foreign_trade + df_issues_score$sstc + df_issues_score$finance + df_issues_score$housing + df_issues_score$transportation,
          EconomieTravail_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "macroeconomics"] + df_sentiments$sentiment[df_sentiments$category == "labour"] + df_sentiments$sentiment[df_sentiments$category == "foreign_trade"] + df_sentiments$sentiment[df_sentiments$category == "sstc"] + df_sentiments$sentiment[df_sentiments$category == "finance"] + df_sentiments$sentiment[df_sentiments$category == "housing"] + df_sentiments$sentiment[df_sentiments$category == "transportation"],
          Education_words_count = df_issues_score$education,
          Education_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "education"],
          EnviroEnergie_words_count = df_issues_score$environment + df_issues_score$energy,
          EnviroEnergie_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "environment"] + df_sentiments$sentiment[df_sentiments$category == "energy"],
          AffIntlDefense_words_count = df_issues_score$defence + df_issues_score$intl_affairs,
          AffIntlDefense_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "defence"] + df_sentiments$sentiment[df_sentiments$category == "intl_affairs"],
          qc_caq_words_count = df_parties_score$caq,
          qc_caq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "caq"],
          qc_pq_words_count = df_parties_score$pq,
          qc_pq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "pq"],
          qc_plq_words_count = df_parties_score$plq,
          qc_plq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "plq"],
          qc_qs_words_count = df_parties_score$qs,
          qc_qs_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "qs"],
          can_lpc_words_count = df_parties_score$lpc,
          can_lpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "lpc"],
          can_cpc_words_count = df_parties_score$cpc,
          can_cpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "cpc"],
          can_npd_words_count = df_parties_score$ndp,
          can_npd_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ndp"],
          can_bq_words_count = df_parties_score$bq,
          can_bq_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "bq"],
          can_gpc_words_count = df_parties_score$gpc,
          can_gpc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "gpc"],
          can_ppc_words_count = df_parties_score$ppc,
          can_ppc_pos_neg_diff = df_sentiments$sentiment[df_sentiments$category == "ppc"],
          headline_mins_duration = as.numeric(difftime(as.POSIXct(df_headlines$end_date[i]), as.POSIXct(df_headlines$begin_date[i]), units="mins")),
          content_sentences_count = clessnverse::compute_nb_sentences(df_headlines$content[i]),
          content_words_count = clessnverse::compute_nb_words(df_headlines$content[i]),
          stringsAsFactors = F
      )

    df <- df %>% rbind(row)

    clessnverse::logit(scriptname, paste('committing', digest::digest(df_headlines$liens[i])), logger)
    clessnverse::commit_mart_row(datamart_table_name, digest::digest(df_headlines$liens[i]), as.list(row), opt$hub_mode, credentials)
  }

}


###############################################################################
########################  Error handling wrapper of the   #####################
########################   main script, allowing to log   #####################
######################## the error and warnings in case   #####################
######################## Something goes wrong during the  #####################
######################## automated execution of this code #####################
###############################################################################

tryCatch( 
  withCallingHandlers(
  {
    # Package dplyr for the %>% 
    # All other packages must be invoked by specifying its name
    # in front ot the function to be called
    library(dplyr) 

    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to 
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #
  datamart_table_name <- "medias_issues_parties_sentiment_scores"


    #########################################################################
    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"




    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally") 
    if (!exists("scriptname")) scriptname <<- "r_medias_issues_parties_sentiment_scores"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    opt <- list(dataframe_mode = "refresh",  hub_mode = "refresh", log_output = c("file", "console"), download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"), 
        Sys.getenv("HUB3_USERNAME"), 
        Sys.getenv("HUB3_PASSWORD"))
    
    # if your script uses hub 2.0 uncomment the line below
    # clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    # or 
    # use this one
    # clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))
    clessnhub::connect_with_token(Sys.getenv('HUB_TOKEN'))
    
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)

    status <<- 0

    # Call main script    
    main()
  },

  warning = function(w) {
      clessnverse::logit(scriptname, paste(w, collapse=' '), logger)
      print(w)
      status <<- 2
  }),
  
  # Handle an error or a call to stop function in the code
  error = function(e) {
    clessnverse::logit(scriptname, paste(e, collapse=' '), logger)
    print(e)
    status <<- 1
  },
  
  # Terminate gracefully whether error or not
  finally={
    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::log_close(logger)

    # Cleanup
    closeAllConnections()
    clessnverse::log_close(logger)
    
    #quit(status = status)
  }
)

