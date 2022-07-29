###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             #
#                                                                             #
#                        <e|l|r_name_of_the_etl_script>                       #
#                                                                             #
#  Describe what the script does.  You can copy/paste part of the README.md   #
#  of the folder which this script resides in                                 #
#                                                                             #
###############################################################################

# login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    credentials <- hublot::get_credentials(
        Sys.getenv("HUB3_URL"),
        Sys.getenv("HUB3_USERNAME"),
        Sys.getenv("HUB3_PASSWORD"))

 ###############################################################################
########################           Functions            ######################
###############################################################################

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

    normalize_variable <- function(vector) {
      max <- max(vector, na.rm = T)
      min <- min(vector, na.rm = T)
      value <- (vector - min)/(max-min)
      return(value)
    }

    reduce_outliers <- function(vector) {
      q1 <- stats::quantile(vector, 0.25)
      q3 <- stats::quantile(vector, 0.75)
      eiq <- q3-q1
      lim_max <- q3 + 1.5*eiq
      lim_min <- q1 - 1.5*eiq
      vector[vector > lim_max] <- lim_max
      vector[vector < lim_min] <- lim_min

      return(vector)
    }


###############################################################################
######################            Functions to           ######################
######################  Get Data Sources from DataLake   ######################
######################              HUB 3.0              ######################
###############################################################################



###############################################################################
######################            Functions to           ######################
######################   Get Data Sources from HUB 2.0   ######################
###############################################################################

# Se connecter au système de Radar+ pour aller chercher les données.
auth <- quorum::createRadarplusAuth(Sys.getenv("RADARPLUS_USERNAME"), Sys.getenv("RADARPLUS_PASSWORD"))

load_radarplus_data <- function(){

  # on crée une requête avec les paramètres des articles qu'on veut
  # Date de début des données désirées
  begin_date <- quorum::createDate(20, 07, 2022, 00,00,00)

  # # Créer la date de fin (aujourd'hui)
  day2 <- as.numeric(format(Sys.time(), "%d"))
  month2 <- as.numeric(format(Sys.time(), "%m"))
  year2 <- as.numeric(format(Sys.time(), "%Y"))

  # Date de fin des données désirées
  end_date <- quorum::createDate(day2, month2, year2, 00, 00, 00)

  # Obtenir les données en français
  queryFR <- quorum::createRadarplusQuery(begin_date=begin_date,                # les articles dont la fin de la une est après cette date
                                          end_date=end_date,                      # les articles dont la fin de la une est avant cette date
                                          # title_contains=c('covid', 'legault'),  # dont le titre contient covid ET legault (case ignorée)
                                          # text_contains=c('covid', 'québec'),    # dont le titre contient covid ET québec (case ignorée)
                                          tags=c('quorum', 'french'),             # dont la source possède les tags quorum ET french
                                          type='text')                            # choix: slug (info minime), text (texte seulement) ou html (html seulement)

  # Obtenir les données en anglais
  queryEN <- quorum::createRadarplusQuery(begin_date=begin_date,
                                          end_date=end_date,
                                          # title_contains=c('covid', 'legault'),
                                          # text_contains=c('covid', 'québec'),
                                          tags=c('quorum', 'english'),
                                          type='text')

  # On télécharge les données dans un data.table "result"
  ResultFR <- quorum::loadRadarplusData(queryFR, auth)

  # On télécharge les données dans un data.table "result"
  ResultEN <- quorum::loadRadarplusData(queryEN, auth)

  # Mettre les données FR et celles EN ensemble
  ResultTot <- ResultFR %>%
    bind_rows(ResultEN)

  # Nettoyer les dates
  ResultTot$cleanDate <- NA
  ResultTot$cleanDate <- substr(ResultTot$begin_date, 1, nchar(ResultTot$begin_date)) # cette ligne pour AAAA/MM/JJ
  ResultTot$cleanDate2 <- substr(ResultTot$begin_date, 1, nchar(ResultTot$begin_date) - 3) # cette ligne pour AAAA/MM

  # On complète le csv avec des données de langue et de région!
  DataRadar <- ResultTot %>%
    mutate(language = ifelse(source %in% c("le-devoir", "la-presse", "tva-nouvelles", "journal-de-montreal", "radio-canada"),
                             "french", "english")) %>%
    mutate(area = ifelse(source %in% c("le-devoir", "la-presse", "tva-nouvelles", "journal-de-montreal", "radio-canada", "montreal-gazette"),
                         "quebec",
                         ifelse(source %in% c("new-york-times", "cnn", "fox-news", "wall-street-journal"),
                                "usa", "canada"))) %>%
    filter(area %in% c("canada", "quebec")) %>%
    mutate(headline_time = as.numeric(end_date - begin_date))

  return(DataRadar)
}

get_journalists_tweets <- function(){
  myfilter <- clessnhub::create_filter(
    metadata = list(personType__regex="journalist|media"),
    data = list(creationDate__gte="2022-07-20",
                creationDate__lte=as.character(as.Date(Sys.time()))))
  Tweets <- clessnhub::get_items('tweets', myfilter, download_data = T) %>%
    mutate(data.likeCount = as.numeric(data.likeCount),
           data.retweetCount = as.numeric(data.retweetCount))
  return(Tweets)
}

get_journalists_hub2 <- function(){
  filter <- clessnhub::create_filter(type = "journalist")
  Persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE) %>%
    select(handle = data.twitterHandle, fullName = data.fullName, female = data.isFemale, media = data.currentMedia)
  return(Persons)
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
  issue <-  clessnverse::get_dictionary("issues", lang = 'fr', credentials)
  sentiment <- clessnverse::get_dictionary("sentiments", lang = "fr", credentials)
  radarplus_data <- load_radarplus_data()
  journalistes_tweets <- get_journalists_tweets()
  journalistes <- get_journalists_hub2()

  journalistes_tweets2 <- journalistes_tweets %>%
    select(date = data.creationDate, text = data.text, handle = metadata.twitterHandle)


tweets <- left_join(journalistes_tweets2, journalistes, by = "handle") %>%
  mutate(source = "twitter") %>%
  select(date, text, id = handle, media, source) %>%
  filter(media %in% c("TVA Nouvelles", "Radio-Canada", "Radio Canada", "La Presse", "Le Devoir")) %>%
  mutate(media = ifelse(media == "La Presse", "la-presse", media),
         media = ifelse(media == "Radio-Canada", "radio-canada", media),
         media = ifelse(media == "Radio Canada", "radio-canada", media),
         media = ifelse(media == "TVA Nouvelles", "tva-nouvelles", media),
         media = ifelse(media == "Le Devoir", "le-devoir", media))

radarplus_data2 <- radarplus_data %>%
  mutate(id = source, media = source, source = "headlines") %>%
  select(date = cleanDate, text, id, media, source) %>%
  filter(media %in% c("la-presse", "le-devoir", "radio-canada", "tva-nouvelles"))

data <- rbind(tweets, radarplus_data2)

example <- compute_relevance_score(data$text[1], issue)

df <- data.frame(matrix(nrow = 0, ncol = length(names(example))))

names(df) <- names(example)

nbwords <- c()

for (i in 1:nrow(data)) {
  row <- compute_relevance_score(data$text[i], issue)
  nbwords[i] <- clessnverse::compute_nb_words(data$text[i])
  df <- rbind(df, row)
  print(i)
}

df$nbwords <- nbwords

relevance_long <- df %>%
  mutate(doc_id = 1:nrow(.)) %>%
  tidyr::pivot_longer(., cols = 1:(length(colnames(.))-2),
                      names_to = "issue", values_to = "nbwords_issue") %>%
  mutate(prop = ifelse((nbwords == 0 & nbwords_issue == 0),
                        0,
                       (nbwords_issue/nbwords)*100),
         logprop = log(prop),
         logprop = ifelse(is.infinite(logprop),
                       NA,
                       logprop),
         relevance = normalize_variable(logprop))

relevance_wide <- relevance_long %>%
  tidyr::replace_na(list(relevance = 0)) %>%
  tidyr::pivot_wider(., "doc_id",
                     names_from = "issue",
                     values_from = "relevance")
library(ggplot2)
ggplot(relevance_long, aes(x = relevance)) +
  geom_histogram() +
  facet_wrap(~issue)

ggplot(relevance_long, aes(x = relevance)) +
  geom_point() +
  geom_smooth() +
  facet_grid(rows = vars(issue))


library(ggcorrplot)
corr_df <- relevance_wide %>%
  select(-doc_id)
corr <- round(cor(corr_df), 1)
ggcorrplot(corr)

df2$propnorm <- normalize_variable(df2$prop)

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
    clessnhub::login(Sys.getenv("HUB2_USERNAME"), Sys.getenv("HUB2_PASSWORD"))

    # Globals
    # Here you must define all objects (variables, arrays, vectors etc that you
    # want to make global to this entire code and that will be accessible to
    # functions you define above.  Defining globals avoids having to pass them
    # as arguments across your functions in thei code
    #
    # The objects scriptname, opt, logger and credentials *must* be set and
    # used throught your code.
    #



    #########################################################################
    # Define your global variables here
    # Ex: lake_path <- "political_party_press_releases"
    #     lake_items_selection_metadata <- list(metadata__province_or_state="QC", metadata__country="CAN", metadata__storage_class="lake")
    #     warehouse_table <- "political_parties_press_releases"




    # scriptname, opt, logger, credentials are mandatory global objects
    # for them we use the <<- assignment so that they are available in
    # all the tryCatch context ("error", "warning", "finally")
    if (!exists("scriptname")) scriptname <<- "l_agoraplus-pressreleases-qc"

    # Uncomment the line below to hardcode the command line option passed to this script when it runs
    # This is particularly useful while developping your script but it's wiser to use real command-
    # line options when puting your script in production in an automated container.
    # opt <- list(dataframe_mode = "refresh", log_output = c("file", "console"), hub_mode = "refresh", download_data = FALSE, translate=FALSE)

    if (!exists("opt")) {
        opt <- clessnverse::processCommandLineOptions()
    }

    if (!exists("logger") || is.null(logger) || logger == 0) logger <<- clessnverse::loginit(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))

    # if your script uses hub 2.0 uncomment the line below
    # clessnhub::connect_with_token(Sys.getenv("HUB_TOKEN"))
    # or
    # use this one
    # clessnhub::login(
    #    Sys.getenv("HUB_USERNAME"),
    #    Sys.getenv("HUB_PASSWORD"),
    #    Sys.getenv("HUB_URL"))

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
    rm(logger)

    quit(status = status)
  }
)

