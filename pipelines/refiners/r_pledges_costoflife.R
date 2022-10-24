credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))

#### Cost of life pledges by legislature ####
PledgeLabelsHistorical <- clessnverse::get_warehouse_table(
  "pledge_labels_historical", credentials) |>
  dplyr::filter(province_or_state == "QC") # load Quebec government pledge labels
PledgeLabelsHistorical$promise_tolower <-
  PledgeLabelsHistorical$french_label |>
  tolower() |> # prepare pledge labels for dictionary analysis (lowercase, no punctuation)
  stringr::str_remove_all("\\[|\\]") |>
  stringr::str_replace_all("[[:punct:]]", "  ")
Dictionaries <- clessnverse::get_dictionary("subcategories", lang = "fr",
                                            credentials = credentials) # get cost of life dictionary
Dictionaries$cost_life <- Dictionaries$cost_life |>
  stringr::str_replace_all("'", "*") # adapt dictionary for mistakes
PledgeLabelsDictionaries <- clessnverse::run_dictionary(
  PledgeLabelsHistorical, promise_tolower, Dictionaries) # calculate number of cost of life pledges
PledgeLabelsHistorical$cost_life <- PledgeLabelsDictionaries$cost_life
InflationPledgesByLegislature <- PledgeLabelsHistorical |>
  dplyr::group_by(legislature) |> # group pledge totals by mandate
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T))
InflationPledgesByLegislature$years <- c(
  "Parizeau/Bouchard\n1994-1998 (n = 97)",
  "Bouchard/Landry\n1998-2003 (n = 127)",
  "Charest\n2003-2007 (n = 106)", # give labels to each mandate
  "Charest\n2007-2008 (n = 98)",
  "Charest\n2008-2012 (n = 62)",
  "Marois\n2012-2014 (n = 113)",
  "Couillard\n2014-2018 (n = 158)",
  "Legault\n2018-2022 (n = 251)")
sysfonts::font_add_google("Roboto", "roboto") # font for graphs
showtext::showtext_auto()
ggplot2::ggplot(InflationPledgesByLegislature, ggplot2::aes(
  x = legislature, y = cost_life)) +
  ggplot2::geom_line(ggplot2::aes(group = 1)) +
  ggplot2::scale_x_discrete("",
                            labels = InflationPledgesByLegislature$years) +
  ggplot2::ylab("Nombre de promesses sur le coût de la vie") +
  clessnverse::theme_clean_dark(base_size = 30) +
  ggplot2::ggtitle("Un enjeu autrefois absent?",
                   subtitle = "Le coût de la vie dans les promesses du parti au pouvoir au Québec depuis 1994") +
  ggplot2::theme(axis.text.x = ggplot2::element_text(hjust = 1, angle = 90,
                                                     lineheight = 0.35))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/", # in this Excel file, count manually number of pledges in each paragraph in a new column
                       "presse_canadienne/2022_09_16/CoutdelaviePromesses",
                       ".png"), width = 8, height = 5.5)

#### Cost of life pledges by 2022 manifesto ####
PartyPlatforms2022 <- clessnverse::get_warehouse_table(
  "political_parties_manifestos_qc2022", credentials) # get 2022 manifestos
PartyPlatforms2022$paragraph_tolower <- PartyPlatforms2022$paragraph |>
  tolower() |> # prepare manifestos for analysis (lowercase, remove punctuation)
  stringr::str_replace_all("[[:punct:]]", "  ")
PartyPlatforms2022Dictionaries <- clessnverse::run_dictionary(
  PartyPlatforms2022, paragraph_tolower, Dictionaries) # calculate number of cost of life mentions
PartyPlatforms2022$cost_life <- PartyPlatforms2022Dictionaries$cost_life
openxlsx::write.xlsx(PartyPlatforms2022,
                     paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                            "presse_canadienne/2022_09_16/",
                            "CoutdelaviePromesses.xlsx"))
InflationPledgesByParty <- PartyPlatforms2022 |>
  dplyr::group_by(political_party) |> # group cost of life mentions by party
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T))

#### Economy pledges by legislature ####
PolimeterHistorical <- clessnverse::get_warehouse_table(
  "polimeter_historical", credentials) |>
  dplyr::filter(province_or_state == "QC") # load Quebec government pledge totals
EconomicVerdictsData <- PolimeterHistorical |>
  dplyr::filter(policy_domain == "Économie et employabilité") |>
  dplyr::group_by(legislature, verdict) |> # group number of economic pledges by fulfillment status by mandate
  dplyr::summarise(number_pledges = sum(as.numeric(number_pledges)),
                   verdict = factor(verdict, levels = c(
                     "kept", "partially_kept", "broken")))
EconomicVerdictsDataShort <- EconomicVerdictsData |>
  dplyr::group_by(legislature) |> # group number of economic pledges by mandate
  dplyr::summarise(number_pledges = sum(number_pledges, na.rm = T))
EconomicVerdictsDataYears <- c(
  paste0("Parizeau/Bouchard\n1994-1998 (n = ",
         EconomicVerdictsDataShort$number_pledges[1], ")"),
  paste0("Bouchard/Landry\n1998-2003 (n = ",
         EconomicVerdictsDataShort$number_pledges[2], ")"),
  paste0("Charest\n2003-2007 (n = ",
         EconomicVerdictsDataShort$number_pledges[3], ")"),
  paste0("Charest\n2007-2008 (n = ",
         EconomicVerdictsDataShort$number_pledges[4], ")"),
  paste0("Charest\n2008-2012 (n = ",
         EconomicVerdictsDataShort$number_pledges[5], ")"),
  paste0("Marois\n2012-2014 (n = ",
         EconomicVerdictsDataShort$number_pledges[6], ")"),
  paste0("Couillard\n2014-2018 (n = ",
         EconomicVerdictsDataShort$number_pledges[7], ")"),
  paste0("Legault\n2018-2022 (n = ",
         EconomicVerdictsDataShort$number_pledges[8], ")"))
sysfonts::font_add_google("Roboto", "roboto")
showtext::showtext_auto()
ggplot2::ggplot(EconomicVerdictsData, ggplot2::aes(
  x = legislature, y = number_pledges, group = verdict,
  fill = as.factor(verdict))) +
  clessnverse::theme_clean_dark(base_size = 30) +
  ggplot2::geom_col(position = "fill") +
  ggplot2::scale_x_discrete("", labels = EconomicVerdictsDataYears) +
  ggplot2::scale_y_continuous(
    "% des promesses en économie et employabilité",
    breaks = c(0, 0.2, 0.4, 0.6, 0.8, 1),
    labels = c(0, 20, 40, 60, 80, 100)) +
  ggplot2::scale_fill_manual("Verdict",
    values = c("#228B22", "#F3C349", "#AE0101"),
    labels = c("Réalisée", "Partiellement réalisée", "Rompue")) +
  ggplot2::theme(plot.title = ggplot2::element_text(lineheight = 0.35),
                 axis.text.x = ggplot2::element_text(
    hjust = 0.5, vjust = 0.5, angle = 90, lineheight = 0.35)) +
  ggplot2::ggtitle(paste("Les gouvernements québécois respectent-ils\nleurs",
                         "promesses sur l'économie?"),
                   subtitle = paste("État de réalisation des promesses en",
                                    "économie et employabilité depuis 1994"))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "presse_canadienne/2022_09_16/CoutdelaviePromesses2",
                       ".png"), width = 8, height = 5.5)

#### Cost of life pledges by press release ####
PressReleases <- clessnverse::get_warehouse_table(
  "political_parties_press_releases", credentials) |>
  dplyr::filter(date >= "2022-08-28") # get political party press releases since the campaign launch
PressReleasesSplit <- strsplit(PressReleases$body, "\n\n+|\t\t+|\n \n+")
PressReleasesSplitDF <- data.frame( # rearrange press releases to have 1 observation par paragraph (not 1 per press release)
  hub.id = rep(PressReleases$hub.id, lapply(PressReleasesSplit, length)),
  date = rep(PressReleases$date, lapply(PressReleasesSplit, length)),
  title = rep(PressReleases$title, lapply(PressReleasesSplit, length)),
  country = rep(PressReleases$country, lapply(PressReleasesSplit, length)),
  political_party = rep(PressReleases$political_party,
                        lapply(PressReleasesSplit, length)),
  province_or_state = rep(PressReleases$province_or_state,
                          lapply(PressReleasesSplit, length)),
  body = unlist(PressReleasesSplit))
PressReleasesSplitDF$body_tolower <- PressReleasesSplitDF$body |>
  tolower() |> # prepare press releases for analysis (lowercase, remove punctuation)
  stringr::str_replace_all("[[:punct:]]", "  ")
PressReleasesDictionaries <- clessnverse::run_dictionary(
  PressReleasesSplitDF, body_tolower, Dictionaries) # calculate number of cost of life mentions
PressReleasesSplitDF$cost_life <- PressReleasesDictionaries$cost_life
openxlsx::write.xlsx(PressReleasesSplitDF, # in this Excel file, count manually number of pledges in each paragraph in a new column
                     paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                            "presse_canadienne/2022_09_16/",
                            "CoutdelaviePromesses2.xlsx"))
CostoflifePressReleases <- PressReleasesSplitDF |>
  dplyr::group_by(political_party) |> # group cost of life mentions by party
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T))

#### Graph 5 ####
NumberPledgesCostoflife <- openxlsx::read.xlsx(paste0(
  "../elxn-qc2022/_SharedFolder_elxn-qc2022/presse_canadienne/2022_09_16/",
  "CoutdelaviePromesses-Modified.xlsx"))
NumberPledgesCostoflifeLong <- NumberPledgesCostoflife |>
  dplyr::group_by(political_party) |> # group number of pledges by party to calculate party totals
  dplyr::summarise(number_pledges = sum(number_pledges, na.rm = T))
sysfonts::font_add_google("Roboto", "roboto")
showtext::showtext_auto()
ggplot2::ggplot(NumberPledgesCostoflifeLong,
                ggplot2::aes(x = political_party, y = number_pledges,
                             fill = political_party)) +
  clessnverse::theme_clean_dark(base_size = 30) +
  ggplot2::geom_bar(stat = "identity") +
  ggplot2::scale_x_discrete("") +
  ggplot2::scale_y_continuous("Nombre de promesses sur le coût de la vie") +
  ggplot2::scale_fill_manual(values = c("#00B0F0", "#3D5889", "#E61B2E",
                                        "#ED8528"), guide = "none") +
  ggplot2::theme(plot.title = ggplot2::element_text(lineheight = 0.35)) +
  ggplot2::ggtitle(paste0("Le coût de la vie dans les plateformes des\n",
                          "grands partis"),
                   subtitle = paste("Comparaison des principaux partis",
                                    "provinciaux québécois qui ont dévoilé",
                                    "publiquement leur plateforme 2022"))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "presse_canadienne/2022_09_16/CoutdelaviePromesses3",
                       ".png"), width = 8, height = 5.5)
