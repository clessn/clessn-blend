credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))

#### Education pledges by legislature ####
PledgeLabelsHistorical <- clessnverse::get_warehouse_table(
  "pledge_labels_historical", credentials) |>
  dplyr::filter(province_or_state == "QC") # load Quebec government pledge labels
PledgeLabelsHistorical$promise_tolower <-
  PledgeLabelsHistorical$french_label |>
  tolower() |> # prepare pledge labels for dictionary analysis (lowercase, no punctuation)
  stringr::str_remove_all("\\[|\\]") |>
  stringr::str_replace_all("[[:punct:]]", "  ")
Dictionaries <- clessnverse::get_dictionary("subcategories", lang = "fr",
                                            credentials = credentials) # get education dictionary
PledgeLabelsDictionaries <- clessnverse::run_dictionary(
  PledgeLabelsHistorical, promise_tolower, Dictionaries["education"]) # calculate number of education pledges
PledgeLabelsHistorical$education <- PledgeLabelsDictionaries$education
EducationPledgesByLegislature <- PledgeLabelsHistorical |>
  dplyr::group_by(legislature) |> # group pledge totals by mandate
  dplyr::summarise(education = sum(education, na.rm = T))
EducationPledgesByLegislature$years <- c( # give labels to each mandate
  paste0("Parizeau/Bouchard\n1994-1998 (n = ",
         EducationPledgesByLegislature$education[1], ")"),
  paste0("Bouchard/Landry\n1998-2003 (n = ",
         EducationPledgesByLegislature$education[2], ")"),
  paste0("Charest\n2003-2007 (n = ",
         EducationPledgesByLegislature$education[3], ")"),
  paste0("Charest\n2007-2008 (n = ",
         EducationPledgesByLegislature$education[4], ")"),
  paste0("Charest\n2008-2012 (n = ",
         EducationPledgesByLegislature$education[5], ")"),
  paste0("Marois\n2012-2014 (n = ",
         EducationPledgesByLegislature$education[6], ")"),
  paste0("Couillard\n2014-2018 (n = ",
         EducationPledgesByLegislature$education[7], ")"),
  paste0("Legault\n2018-2022 (n = ",
         EducationPledgesByLegislature$education[8], ")"))
EducationPledgesByLegislature$number_pledges <- c(97, 127, 106, 98, 62, 113,
                                                  158, 251)
EducationPledgesByLegislature$percent <- 100 *
  EducationPledgesByLegislature$education /
  EducationPledgesByLegislature$number_pledges
EducationPledgesByLegislature$party = c("PQ", "PQ", "PLQ", "PLQ", "PLQ",
                                        "PQ", "PLQ", "CAQ")
EducationPledgesByLegislature$party <- factor(
  EducationPledgesByLegislature$party, levels = c("PLQ", "PQ", "CAQ"))
sysfonts::font_add_google("Roboto", "roboto") # font for graphs
showtext::showtext_auto()
ggplot2::ggplot(EducationPledgesByLegislature, ggplot2::aes(
  x = legislature, y = percent)) +
  ggplot2::geom_col(ggplot2::aes(fill = party)) +
  ggplot2::geom_text(ggplot2::aes(label = round(percent, 2)),
                     position = ggplot2::position_fill(vjust = 20)) +
  ggplot2::scale_fill_manual("Gouvernement", values = c("#E61B2E", "#3D5889",
                                                        "#00B0F0")) +
  ggplot2::scale_x_discrete("",
                            labels = EducationPledgesByLegislature$years) +
  ggplot2::scale_y_continuous("% de promesses sur l'éducation") +
  clessnverse::theme_clean_dark(base_size = 15) +
  ggplot2::labs(caption = paste0("Source: Polimètre du Québec.\n",
                                 "Note: Analyse textuelle automatisée à",
                                 " partir de mots liés à l'éducation.\nPour",
                                 " plus d'informations: polimetre.org")) +
  ggplot2::ggtitle("L'éducation, priorité des partis ou enjeu négligé?",
                   subtitle = "Présence de l'éducation dans les promesses du parti au pouvoir au Québec depuis 1994") +
  ggplot2::theme(axis.text.x = ggplot2::element_text(vjust = 0.5, angle = 45,
                                                     lineheight = 0.35),
                 plot.title = ggplot2::element_text(
                   lineheight = 0.35, margin = ggplot2::margin(0, 0, 0, 0)),
                 plot.subtitle = ggplot2::element_text(
                   lineheight = 0.35, margin = ggplot2::margin(2.5, 0, 0, 0)),
                 plot.caption = ggplot2::element_text(lineheight = 0.35),
                 legend.key.size = ggplot2::unit(0.2, "lines"),
                 legend.text = ggplot2::element_text(lineheight = 0.35),
                 legend.position = "top",
                 legend.box.margin = ggplot2::margin(0, 0, -15, 0))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/", # in this Excel file, count manually number of pledges in each paragraph in a new column
                       "education/PromessesEducationHistoriques",
                       ".png"), width = 1200, height = 675, units = "px")

#### Education pledges by legislature ####
PolimeterHistorical <- clessnverse::get_warehouse_table(
  "polimeter_historical", credentials) |>
  dplyr::filter(province_or_state == "QC") # load Quebec government pledge totals
EducationVerdictsData <- PolimeterHistorical |>
  dplyr::filter(policy_domain == "Éducation et recherche") |>
  dplyr::group_by(legislature, verdict) |> # group number of education pledges by fulfillment status by mandate
  dplyr::summarise(number_pledges = sum(as.numeric(number_pledges)),
                   verdict = factor(verdict, levels = c(
                     "kept", "partially_kept", "broken")))
EducationVerdictsDataShort <- EducationVerdictsData |>
  dplyr::group_by(legislature) |> # group number of education pledges by mandate
  dplyr::summarise(number_pledges = sum(number_pledges, na.rm = T))
EducationVerdictsDataYears <- c(
  paste0("Parizeau/Bouchard\n1994-1998 (n = ",
         EducationVerdictsDataShort$number_pledges[1], ")"),
  paste0("Bouchard/Landry\n1998-2003 (n = ",
         EducationVerdictsDataShort$number_pledges[2], ")"),
  paste0("Charest\n2003-2007 (n = ",
         EducationVerdictsDataShort$number_pledges[3], ")"),
  paste0("Charest\n2007-2008 (n = ",
         EducationVerdictsDataShort$number_pledges[4], ")"),
  paste0("Charest\n2008-2012 (n = ",
         EducationVerdictsDataShort$number_pledges[5], ")"),
  paste0("Marois\n2012-2014 (n = ",
         EducationVerdictsDataShort$number_pledges[6], ")"),
  paste0("Couillard\n2014-2018 (n = ",
         EducationVerdictsDataShort$number_pledges[7], ")"),
  paste0("Legault\n2018-2022 (n = ",
         EducationVerdictsDataShort$number_pledges[8], ")"))
EducationVerdictsData$percent_pledges <- NA
for(i in labels(table(EducationVerdictsData$legislature))[[1]]){
  EducationVerdictsData$percent_pledges[
    EducationVerdictsData$legislature == i] <-
    100 * EducationVerdictsData$number_pledges[
      EducationVerdictsData$legislature == i] /
    sum(EducationVerdictsData$number_pledges[
      EducationVerdictsData$legislature == i], na.rm = T)
}
sysfonts::font_add_google("Roboto", "roboto")
showtext::showtext_auto()
ggplot2::ggplot(EducationVerdictsData, ggplot2::aes(
  x = legislature, y = percent_pledges, group = verdict,
  fill = as.factor(verdict))) +
  clessnverse::theme_clean_dark(base_size = 15) +
  ggplot2::geom_bar(stat = "identity", position = "stack") +
  ggplot2::geom_text(ggplot2::aes(label = round(percent_pledges, 2)),
                     position = ggplot2::position_stack(vjust = 0.5)) +
  ggplot2::scale_x_discrete("", labels = EducationVerdictsDataYears) +
  ggplot2::scale_y_continuous("% des promesses en éducation et recherche") +
  ggplot2::labs(caption = paste0("Source: Polimètre du Québec.\n",
                                 "Note: Codage manuel de l'état de ",
                                 "réalisation des promesses.\nPour",
                                 " plus d'informations: polimetre.org")) +
  ggplot2::scale_fill_manual("Verdict",
                             values = c("#228B22", "#F3C349", "#AE0101"),
                             labels = c("Réalisée", "Partiellement réalisée",
                                        "Rompue")) +
  ggplot2::theme(
    axis.text.x = ggplot2::element_text(angle = 45, vjust = 0.5,
                                        lineheight = 0.35),
    legend.key.size = ggplot2::unit(0.2, "lines"),
    plot.title = ggplot2::element_text(
      lineheight = 0.35, margin = ggplot2::margin(0, -20, 0, 0)),
    plot.subtitle = ggplot2::element_text(
      lineheight = 0.35, margin = ggplot2::margin(2.5, 0, 0, 0)),
    plot.caption = ggplot2::element_text(lineheight = 0.35),
    legend.text = ggplot2::element_text(lineheight = 0.35),
    legend.position = "top",
    legend.box.margin = ggplot2::margin(0, 0, -15, 0)) +
  ggplot2::ggtitle("Des promesses surtout réalisées en éducation",
                   subtitle = paste("État de réalisation des promesses en",
                                    "éducation et recherche depuis 1994"))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "education/VerdictsPolimetreEducation.png"),
                width = 1200, height = 675, units = "px")

#### Education mentions by manifesto ####
PartyPlatforms2022 <- clessnverse::get_warehouse_table(
  "political_parties_manifestos_qc2022", credentials) # get 2022 manifestos
PartyPlatforms2022$paragraph_tolower <- PartyPlatforms2022$paragraph |>
  tolower() |> # prepare manifestos for analysis (lowercase, remove punctuation)
  stringr::str_replace_all("[[:punct:]]", "  ")
PartyPlatforms2022Dictionaries <- clessnverse::run_dictionary(
  PartyPlatforms2022, paragraph_tolower, Dictionaries["education"]) # calculate number of education mentions
PartyPlatforms2022$education <- PartyPlatforms2022Dictionaries$education
openxlsx::write.xlsx(PartyPlatforms2022,
                     paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                            "education/PlateformesEducation.xlsx"))
EducationPledgesByParty <- PartyPlatforms2022 |>
  dplyr::group_by(political_party) |> # group education mentions by party
  dplyr::summarise(education = sum(education, na.rm = T))

#### Education pledges by manifesto ####
ManifestoPledgesEducation <- openxlsx::read.xlsx(paste0(
  "../elxn-qc2022/_SharedFolder_elxn-qc2022/education/",
  "PlateformesEducation-Promesses.xlsx"))
ManifestoPledgesEducationLong <- ManifestoPledgesEducation |>
  dplyr::group_by(political_party) |> # group number of pledges by party to calculate party totals
  dplyr::summarise(number_pledges = sum(number_pledges, na.rm = T))
sysfonts::font_add_google("Roboto", "roboto")
showtext::showtext_auto()
ggplot2::ggplot(ManifestoPledgesEducationLong,
                ggplot2::aes(x = political_party, y = number_pledges,
                             fill = political_party)) +
  clessnverse::theme_clean_dark(base_size = 15) +
  ggplot2::geom_bar(stat = "identity") +
  ggplot2::geom_text(ggplot2::aes(label = number_pledges), size = 10,
                     position = ggplot2::position_fill(vjust = 10)) +
  ggplot2::scale_x_discrete("") +
  ggplot2::scale_y_continuous("Nombre de promesses sur l'éducation") +
  ggplot2::labs(caption = paste0("Source: Plateformes électorales 2022 des",
                                 " partis politiques québécois.\n",
                                 "Note: Analyse textuelle automatisée à",
                                 " partir de mots liés à l'éducation.\nPour",
                                 " plus d'informations: info@clessn.com")) +
  ggplot2::scale_fill_manual(values = c("#00B0F0", "#3D5889", "#E61B2E",
                                        "#ED8528"), guide = "none") +
  ggplot2::theme(plot.title = ggplot2::element_text(lineheight = 0.35),
                 plot.caption = ggplot2::element_text(lineheight = 0.35)) +
  ggplot2::ggtitle(paste("L'éducation dans les plateformes des",
                         "grands partis"),
                   subtitle = paste("Comparaison des principaux partis",
                                    "provinciaux québécois qui ont dévoilé",
                                    "publiquement leur plateforme 2022"))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "education/PromessesEducation.png"),
                width = 1200, height = 675, units = "px")

#### Education mentions by press release ####
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
  PressReleasesSplitDF, body_tolower, Dictionaries) # calculate number of education mentions
PressReleasesSplitDF$education <- PressReleasesDictionaries$education
openxlsx::write.xlsx(PressReleasesSplitDF, # in this Excel file, count manually number of pledges in each paragraph in a new column
                     paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                            "education/CommuniquesDePresseEducation.xlsx"))
EducationPressReleases <- PressReleasesSplitDF |>
  dplyr::group_by(political_party) |> # group education mentions by party
  dplyr::summarise(education = sum(education, na.rm = T))

#### Education pledges by press release ####
PressReleasePledgesEducation <- openxlsx::read.xlsx(paste0(
  "../elxn-qc2022/_SharedFolder_elxn-qc2022/education/",
  "CommuniquesDePresseEducation-Promesses.xlsx"))
PressReleasePledgesEducationLong <- PressReleasePledgesEducation |>
  dplyr::group_by(political_party) |> # group number of pledges by party to calculate party totals
  dplyr::summarise(number_pledges = sum(number_pledges, na.rm = T))
sysfonts::font_add_google("Roboto", "roboto")
showtext::showtext_auto()
ggplot2::ggplot(PressReleasePledgesEducationLong,
                ggplot2::aes(x = political_party, y = number_pledges,
                             fill = political_party)) +
  clessnverse::theme_clean_dark(base_size = 15) +
  ggplot2::geom_bar(stat = "identity") +
  ggplot2::scale_x_discrete("") +
  ggplot2::scale_y_continuous("Nombre de promesses sur l'éducation") +
  ggplot2::scale_fill_manual(values = c("#00B0F0", "#3D5889", "#E61B2E",
                                        "#ED8528"), guide = "none") +
  ggplot2::theme(plot.title = ggplot2::element_text(lineheight = 0.35)) +
  ggplot2::ggtitle(paste("L'éducation dans les communiqués de presse des",
                         "grands partis"),
                   subtitle = paste("Comparaison des principaux partis",
                                    "provinciaux québécois"))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "education/CommuniquesDePresseEducation.png"),
                width = 1200, height = 675, units = "px")
