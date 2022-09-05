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

# package
library(tidyverse)
library(lubridate)

# login to hublot
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

    sentiment_nbwords <- function(text_bloc, sentiment_dictionary) {
      toks <- quanteda::tokens(text_bloc)
      dfm_corpus <- quanteda::dfm(toks)
      lookup <- quanteda::dfm_lookup(dfm_corpus, dictionary = sentiment_dictionary, valuetype = "glob")
      df <- quanteda::convert(lookup, to="data.frame")
      return(df)
    }

    inv <- function(x){
      output <- ((x - max(x)) * -1) + min(x)
      return(output)
    }

    ggsave_twitter <- function(filename, plot = ggplot2::last_plot(),
                               device = NULL, path = NULL, scale = 1,
                               width = 1600, height = 900, units = "px",
                               dpi = 300, limitsize = TRUE, bg = NULL, ...) {
      ggplot2::ggsave(filename, plot = ggplot2::last_plot(),
                      device = device, path = path, scale = scale,
                      width = width, height = height, units = units,
                      dpi = dpi, limitsize = limitsize, bg = bg, ...)
    }

    # Graphs

  issue_names <-  c("troisieme_lien" = "Troisième lien",
                   "tramway_quebec" = "Tramway de Québec",
                   "chsld" = "CHSLD",
                   "racisme" = "Enjeux raciaux",
                   "enjeux autochtone" = "Premières Nations",
                   "groupes" = "Groupes minoritaires",
                   "bien_etre" = "Santé et bien-être",
                   "langue_fran" = "Langue française",
                   "inflation" = "Inflation",
                   "prix_essence" = "Prix de l'essence",
                   "salaire_minimum" = "Salaire minimum",
                   "polarisation" = "Polarisation")

  issue_color <-  c("troisieme_lien" = "#C1E3FE",
                    "tramway_quebec" = "#CFB6E5",
                    "chsld" = "#FFD9E0",
                    "racisme" = "#F1EECD",
                    "enjeux autochtone" = "#C9DECE",
                    "groupes" = "#B6CAD6",
                    "bien_etre" = "#9A98C3",
                    "langue_fran" = "#EADA9A",
                    "inflation" = "#C5CB68",
                    "prix_essence" = "#FF6961",
                    "salaire_minimum" = "#988270",
                    "polarisation" = "red")

  media_color <- c("la-presse" = "#ADF2B5",
                   "le-devoir" = "#FFD9C0",
                   "tva-nouvelles" = "#8CC0DE",
                   "radio-canada" = "#F4BFBF")

  media_names <- c("la-presse" = "La Presse",
                   "le-devoir" = "Le Devoir",
                   "tva-nouvelles" = "TVA Nouvelles",
                   "radio-canada" = "Radio-Canada")

  issue_color2 <- c("#FF624D", # red
  "#88ADFF", # blue
  "#BA8FFF", # purple
  "#FEEC20") # yellow

  # thermometre <- c("red",
  #                  "white",
  #                  "green")


  # Graph par enjeux

  graph_issue <- function(dataframe, graph_issue) {
      title_name <- issue_names[graph_issue]
      graph <- dataframe %>%
      filter(relevance != 0) %>%
      group_by(media, issue) %>%
      summarise(mean_relevance = mean(relevance),
                mean_sentiment = mean(sentiment, na.rm = T)) %>%
      filter(issue == graph_issue) %>%
      ggplot(., aes(x = mean_relevance, y = mean_sentiment, label = media, color = media)) +
      # pour modifier la ligne de l'axe horizontal
      geom_segment(y = 0, x = 0.10, yend = 0, xend = 0.87, color = "light grey") +
      # pour modifier la ligne de l'axe vertical
      geom_segment(y = -1.02, x = 0.5, yend = 1.03, xend = 0.5, color = "light grey") +
      # Ajouter les points sur lignes
      scale_color_manual(values = media_color) +
      scale_fill_manual(values = media_color) +
      geom_point() +
      ggrepel::geom_label_repel(aes(fill = media, label = media_names[media]), color = "black") +
      # Ajouter les labels sur les points
      # geom_text() +
      # Limites du graphique
      xlim(0,1) +
      ylim(-1,1) +
      # Ajouter les titres des extrémités des axes
      annotate("text", x = -Inf, y = 0, label = "Peu couvert", hjust = 0, vjust = 0.4) +
      annotate("text", x = Inf, y = 0, label = "Très couvert    ", hjust = 1, vjust = 0.4) +
      annotate("text", x = 0.5, y = Inf, label = "Ton positif", hjust = 0.5, vjust = 1) +
      annotate("text", x = 0.5, y = -Inf, label = "Ton négatif", hjust = 0.5, vjust = -0.5) +
      # Enlever le titre des axes
      xlab("") +
      ylab("") +
    #  theme_classic() +
      # Enlever les ticks et l'arrière-plan
      theme(axis.text = element_blank(),
            axis.ticks = element_blank(),
            panel.grid.major = element_blank(),
            panel.grid.minor = element_blank(),
            panel.background = element_blank(),
            text = element_text(family="Helvetica"),
            plot.title = element_text(size = 20, face="bold"),
            plot.subtitle = element_text(size = 14),
            legend.position = "none") +
      labs(title = title_name,
           subtitle = paste0("Couverture et ton des enjeux saillants depuis le ", format(as.Date(min(dataframe$date)), "%d/%m/%Y"),"\n"))
  return(graph)
    }

  # Graph par média

  graph_media <- function(dataframe, graph_media) {
    title_name <- media_names[graph_media]
    graph <- dataframe %>%
      filter(relevance != 0 &    # Ici à réviser. Contrôler pour le nb d'articles qui ne parlent pas de l'enjeu, plutôt que de les éliminer
               !(issue %in% c("racisme", "groupes", "bien_etre"))) %>%
      group_by(issue, media) %>%
      summarise(mean_relevance = mean(relevance),
                mean_sentiment = mean(sentiment, na.rm = T)) %>%
      filter(media == graph_media) %>%
      ggplot(., aes(x = mean_relevance, y = mean_sentiment, label = issue, color = issue)) +
      # pour modifier la ligne de l'axe horizontal
      geom_segment(y = 0, x = 0.10, yend = 0, xend = 0.87, color = " light grey") +
      # pour modifier la ligne de l'axe vertical
      geom_segment(y = -1.02, x = 0.5, yend = 1.03, xend = 0.5, color = " light grey") +
      # Ajouter les points sur lignes
      scale_color_manual(values = issue_color) +
      scale_fill_manual(values = issue_color) +
      geom_point() +
      ggrepel::geom_label_repel(aes(fill = issue, label = issue_names[issue]), color = "black", nudge_y = -0.05, nudge_x = 0.05) +
      # Ajouter les labels sur les points
      # geom_text() +
      # Limites du graphique
      xlim(0,1) +
      ylim(-1,1) +
      # Ajouter les titres des extrémités des axes
      annotate("text", x = -Inf, y = 0, label = "Peu couvert", hjust = 0, vjust = 0.4) +
      annotate("text", x = Inf, y = 0, label = "Très couvert    ", hjust = 1, vjust = 0.4) +
      annotate("text", x = 0.5, y = Inf, label = "Ton positif", hjust = 0.5, vjust = 1) +
      annotate("text", x = 0.5, y = -Inf, label = "Ton négatif", hjust = 0.5, vjust = -0.5) +
      # Enlever le titre des axes
      xlab("") +
      ylab("") +
      #  theme_classic() +
      # Enlever les ticks et l'arrière-plan
      theme(axis.text = element_blank(),
            axis.ticks = element_blank(),
            panel.grid.major = element_blank(),
            panel.grid.minor = element_blank(),
            panel.background = element_blank(),
            text = element_text(family="Helvetica"),
            plot.title = element_text(size = 20, face="bold"),
            plot.subtitle = element_text(size = 14),
            legend.position = "none") +
      labs(title = title_name,
           subtitle = paste0("Couverture et ton des enjeux saillants depuis le ", format(as.Date(min(dataframe$date)), "%d/%m/%Y"),"\n"))
    return(graph)
  }



  # # Graph par enjeux #2
  # graph_issue2 <- function(dataframe, graph_issue) {
  #   title_name <- issue_names[graph_issue]
  #   graph <- dataframe %>%
  #     filter(relevance != 0) %>%
  #     group_by(media, issue) %>%
  #     summarise(mean_relevance = mean(relevance),
  #               mean_sentiment = mean(sentiment, na.rm = T)) %>%
  #     filter(issue == graph_issue)
  #
  #     output <- ggplot(graph, aes(x = mean_sentiment, y = media, size = mean_relevance, fill = mean_sentiment)) +
  #     geom_point(shape = 21, stroke = 1) +
  #     scale_size(range = c(10, 30),
  #                  guide="none") +
  #     scale_fill_gradientn(colors = thermometre,
  #                            breaks=c(min(graph$mean_sentiment), mean(graph$mean_sentiment), max(graph$mean_sentiment)),
  #                            labels=c("Négative",  "Couverture neutre","Positive"),
  #                            limits=c(min(graph$mean_sentiment), max(graph$mean_sentiment)),
  #                            name = "") +
  #     scale_y_discrete(labels = c("TVA Nouvelles", "Radio-Canada", "Le Devoir", "La Presse"),
  #                      breaks = c("tva-nouvelles", "radio-canada", "le-devoir", "la-presse")) +
  #     # Enlever le titre des axes
  #     xlab("") +
  #     ylab("") +
  #     ggthemes::theme_hc() +
  #     # Enlever les ticks et l'arrière-plan
  #     theme(axis.text.x = element_blank(),
  #           axis.text.y = element_text(size = 12),
  #           axis.ticks = element_blank(),
  #           panel.grid.major = element_blank(),
  #           panel.grid.minor = element_blank(),
  #           text = element_text(family="Helvetica"),
  #           plot.title = element_text(size = 20, face="bold"),
  #           plot.subtitle = element_text(size = 14),
  #           legend.position = "bottom",
  #           legend.key.width = unit(5, "cm"),
  #           legend.text = element_text(size = 12)) +
  #     labs(title = title_name,
  #          subtitle = paste0("Intensité de la couverture et ton dans les médias depuis le ", format(as.Date(min(datamart_df$date)), "%d/%m/%Y"),"\n"),
  #          caption = "\nNote: La grosseur des points est indicatif de l'intensité de la couverture médiatique")
  #     return(output)
  # }

  # Graph par média

  data_ends <- relevanceProp %>%
    group_by(issue) %>%
    filter(n == max(n))

  sysfonts::font_add_google("Roboto", "roboto")
  showtext_auto()

  ggplot(relevanceProp, aes(x = date, y = n)) +
    geom_line(aes(group = issue, color = issue, alpha = polarisation),
              show.legend = F, size = 1) +
    geom_point(aes(group = issue, color = issue, alpha = polarisation), show.legend = F) +
    # geom_vline(xintercept = as.Date("2022-08-27"),
    #            linetype="dashed", color = "grey") +
    clessnverse::theme_clean_dark(base_size = 13) +
    theme(text = element_text(family = "roboto")) +
    # Ajouter les points sur lignes
    scale_color_manual(values = issue_color2) +
    scale_fill_manual(values = issue_color2) +
    scale_alpha_continuous(range = c(0.3, 1)) +
    scale_x_datetime("",
                     date_labels = "%d/%m",
                     date_breaks =  "1 day") +
    ggrepel::geom_label_repel(aes(fill = issue, label = issue_names[issue], alpha = polarisation), data = data_ends, color = "black", nudge_y = 0.5, nudge_x = 0.05, show.legend = F) +
    ylab("Nombre d'articles par jour à la Une\n") +
    labs(title = "Nombre d'articles abordant la polarisation des idéologies",
         subtitle = "À la Une des grands médias québécois\n")

  ggsave_twitter("/Users/adrien/Dropbox/Travail/Universite_Laval/CLESSN/elxn-qc2022/_SharedFolder_elxn-qc2022/_graphs/2022-09-05/radar.png")

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

  #Créer la date de début
  # Depuis 2 semaine
  day <- as.numeric(format(lubridate::today() - days(14), "%d"))
  month <- as.numeric(format(lubridate::today() - days(14), "%m"))
  year <- as.numeric(format(lubridate::today() - days(14), "%Y"))

  # on crée une requête avec les paramètres des articles qu'on veut
  # Date de début des données désirées
  begin_date <- quorum::createDate(day, month, year, 00,00,00)

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
    mutate(headline_time = as.numeric(end_date - begin_date)) %>%
    filter(text != "")

  return(DataRadar)
}

# get_journalists_tweets <- function(){
#   myfilter <- clessnhub::create_filter(
#     metadata = list(personType__regex="journalist|media"),
#     data = list(creationDate__gte="2022-07-20",
#                 creationDate__lte=as.character(as.Date(Sys.time()))))
#   Tweets <- clessnhub::get_items('tweets', myfilter, download_data = T) %>%
#     mutate(data.likeCount = as.numeric(data.likeCount),
#            data.retweetCount = as.numeric(data.retweetCount))
#   return(Tweets)
# }
#
# get_journalists_hub2 <- function(){
#   filter <- clessnhub::create_filter(type = "journalist")
#   Persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE) %>%
#     select(handle = data.twitterHandle, fullName = data.fullName, female = data.isFemale, media = data.currentMedia)
#   return(Persons)
# }

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
  issue <-  clessnverse::get_dictionary("subcategories", lang = 'fr', credentials)
  sentiment <- clessnverse::get_dictionary("sentiments", lang = "fr", credentials)
  radarplus_data <- load_radarplus_data()
#  journalists_tweets <- get_journalists_tweets()
 # journalists <- get_journalists_hub2()

  # journalists_tweets2 <- journalists_tweets %>%
  #   select(date = data.creationDate, text = data.text, handle = metadata.twitterHandle)

# tweets <- left_join(journalists_tweets2, journalists, by = "handle") %>%
#   mutate(source = "twitter") %>%
#   select(date, text, id = handle, media, source) %>%
#   filter(media %in% c("TVA Nouvelles", "Radio-Canada", "Radio Canada", "La Presse", "Le Devoir")) %>%
#   mutate(media = ifelse(media == "La Presse", "la-presse", media),
#          media = ifelse(media == "Radio-Canada", "radio-canada", media),
#          media = ifelse(media == "Radio Canada", "radio-canada", media),
#          media = ifelse(media == "TVA Nouvelles", "tva-nouvelles", media),
#          media = ifelse(media == "Le Devoir", "le-devoir", media))

radarplus_data2 <- radarplus_data %>%
  mutate(id = source, media = source, source = "headlines") %>%
  select(date = cleanDate, text, id, media, source) %>%
  filter(media %in% c("la-presse", "le-devoir", "radio-canada", "tva-nouvelles")) %>%
  mutate(doc_id = 1:nrow(.))

data <- rbind(radarplus_data2)

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

names(df) <- paste0("relevance_", names(df))

relevance_long <- df %>%
  mutate(nbwords = nbwords,
         doc_id = 1:nrow(.)) %>%
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

#Pour graph de n enjeux
relevanceProp <- relevance_long %>%
  select(-c(logprop, relevance, prop)) %>%
  mutate(talking = ifelse(nbwords_issue > 1, 1, 0)) %>%
  left_join(., radarplus_data2, by="doc_id") %>%
  group_by(date, issue) %>%
  summarise(n = sum(talking)) %>%
  mutate(issue = gsub("relevance_", "", issue),
         polarisation = ifelse(issue == "polarisation", 1, 0)) %>%
  filter(issue %in% c("inflation", "polarisation", "troisieme_lien", "tramway_quebec")) %>%
  mutate(date = as.POSIXct(date))

df_sentiment <- data.frame(negative = as.numeric(), positive = as.numeric())

for (i in 1:nrow(data)) {
  row <- sentiment_nbwords(data$text[i], sentiment_dictionary = sentiment) %>%
    select(-doc_id)
  df_sentiment <- rbind(df_sentiment, row)
  print(i)
}

data2 <- cbind(data, df_sentiment, relevance_wide) %>%
  mutate(nbwords = nbwords,
         neutralWords = (nbwords - negative - positive),
         ratioPos = positive / (negative + positive),
         ratioPos = ifelse(is.nan(ratioPos), 0.5, ratioPos),
         pos = ifelse(ratioPos > 0.5, 1, 0),
         neg = ifelse(ratioPos < 0.5, 1, 0))

positive <- data2 %>%
  filter(pos == 1) %>%
  mutate(norm_ratio = normalize_variable(ratioPos))

negative <- data2 %>%
  filter(neg == 1) %>%
  mutate(invratio = inv(ratioPos),
         norm_ratio = normalize_variable(invratio)) %>%
  select(-invratio)

neutral <- data2 %>% filter(ratioPos == 0.5) %>%
  mutate(norm_ratio = 0)


datamart_df <- rbind(positive, negative, neutral) %>%
  mutate(connonated_words = nbwords - neutralWords,
         prop_connwords = connonated_words / nbwords,
         adj_ratio = log(norm_ratio * prop_connwords),
         adj_ratio = ifelse(is.infinite(adj_ratio),
                            NA,
                            adj_ratio),
         adj_ratio = normalize_variable(adj_ratio),
         raw_ratio = 0,
         raw_ratio = ifelse(neg == 1, adj_ratio*-1, raw_ratio),
         sentiment = ifelse(pos == 1, adj_ratio, raw_ratio)) %>%
  select(-c(id, negative, positive, doc_id, nbwords, neutralWords,
            ratioPos, pos, neg, norm_ratio, connonated_words,
            prop_connwords, adj_ratio, raw_ratio)) %>%
  mutate(doc_id = 1:nrow(.)) %>%
  pivot_longer(cols = starts_with("relevance"),
               names_to = "issue",
               values_to = "relevance",
               names_prefix = "relevance_") %>%
  mutate(key = 1:nrow(.)) %>%
  select(key, doc_id, date, media, source, sentiment, issue, relevance)

clessnverse::commit_mart_table(datamart_table_name, datamart_df, "key", "refresh", credentials)

## Enregistrement Graph 1

for (i in names(issue_names)) {
  graph_issue(datamart_df, i)
  ggsave(paste0('_SharedFolder_elxn-qc2022/presse_canadienne/2022_08_05/',
                format(Sys.time(), "%d-%m-%Y"),"_enjeux_", i, ".png"),
         width = 18, height = 18, units = c("cm"))
}

## Enregistrement Graph 2

for (i in names(media_names)) {
  graph_media(datamart_df, i)
  ggsave(paste0('_SharedFolder_elxn-qc2022/presse_canadienne/2022_08_05/',
                format(Sys.time(), "%d-%m-%Y"),"_media_", i, ".png"),
         width = 18, height = 18, units = c("cm"))
}

## Enregistrement Graph 3

# for (i in names(media_names)) {
#   graph_issue2(datamart_df, i)
#   ggsave(paste0('_SharedFolder_elxn-qc2022/presse_canadienne/2022_08_05/',
#                 format(Sys.time(), "%d-%m-%Y"),"_media_", i, ".png"),
#          width = 26, height = 14, units = c("cm"))
# }

# Enregistrement du csv du graph 2

write_csv(graph, paste0("_SharedFolder_elxn-qc2022/presse_canadienne/2022_08_05/",
                        format(Sys.time(), "%d-%m-%Y"),"_media_issue", ".csv"))

write_csv(datamart_df, paste0("_SharedFolder_elxn-qc2022/presse_canadienne/2022_08_05/",
                        format(Sys.time(), "%d-%m-%Y"),"_datamart", ".csv"))


# library(ggplot2)
# ggplot(relevance_long, aes(x = relevance)) +
#   geom_histogram() +
#   facet_wrap(~issue)
#
# library(ggcorrplot)
# corr_df <- relevance_wide %>%
#   select(-doc_id)
# corr <- round(cor(corr_df), 1)
# ggcorrplot(corr)




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

    datamart_table_name  <- "media_sentiment_relevance_compass"




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

