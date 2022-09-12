credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))
PledgeLabelsHistorical <- clessnverse::get_warehouse_table(
  "pledge_labels_historical", credentials) |>
  dplyr::filter(province_or_state == "QC")
PledgeLabelsHistorical$french_label <- PledgeLabelsHistorical$french_label |>
  tolower() |>
  stringr::str_remove_all("\\[|\\]") |>
  stringr::str_replace_all("[[:punct:]]", "  ")
Dictionaries <- clessnverse::get_dictionary("subcategories", lang = "fr",
                                            credentials = credentials)
Dictionaries$cost_life <- Dictionaries$cost_life |>
  stringr::str_replace_all("'", "*")
PledgeLabelsDictionaries <- clessnverse::run_dictionary(
  PledgeLabelsHistorical, french_label, Dictionaries)
PledgeLabelsHistorical$cost_life <- PledgeLabelsDictionaries$cost_life
InflationPledgesByLegislature <- PledgeLabelsHistorical |>
  dplyr::group_by(legislature) |>
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T))
InflationPledgesByLegislature$years <- c(
  "Parizeau/Bouchard 1994-1998 (n = 97)",
  "Bouchard/Landry 1998-2003 (n = 127)",
  "Charest 2003-2007 (n = 106)",
  "Charest 2007-2008 (n = 98)",
  "Charest 2008-2012 (n = 62)",
  "Marois 2012-2014 (n = 113)",
  "Couillard 2014-2018 (n = 158)",
  "Legault 2018-2022 (n = 251)")
ggplot2::ggplot(InflationPledgesByLegislature, ggplot2::aes(
  x = legislature, y = cost_life)) +
  ggplot2::geom_line(ggplot2::aes(group = 1)) +
  ggplot2::scale_x_discrete("",
                            labels = InflationPledgesByLegislature$years) +
  ggplot2::ylab("Nombre de promesses abordant le coût de la vie") +
  clessnverse::theme_clean_dark() +
  ggplot2::theme(axis.text.x = ggplot2::element_text(hjust = 1, angle = 90))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "presse_canadienne/2022_09_16/CoutdelaviePromesses",
                       ".png"))

PartyPlatforms2022 <- clessnverse::get_warehouse_table(
  "political_parties_manifestos_qc2022", credentials)
PartyPlatforms2022$paragraph <- PartyPlatforms2022$paragraph |>
  tolower() |>
  stringr::str_replace_all("[[:punct:]]", "  ")
PartyPlatforms2022Dictionaries <- clessnverse::run_dictionary(
  PartyPlatforms2022, paragraph, Dictionaries)
PartyPlatforms2022$cost_life <- PartyPlatforms2022Dictionaries$cost_life
InflationPledgesByParty <- PartyPlatforms2022 |>
  dplyr::group_by(political_party) |>
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T))

PolimeterHistorical <- clessnverse::get_warehouse_table(
  "polimeter_historical", credentials) |>
  dplyr::filter(province_or_state == "QC")
EconomicVerdictsData <- PolimeterHistorical |>
  dplyr::filter(policy_domain == "Économie et employabilité") |>
  dplyr::group_by(legislature, verdict) |>
  dplyr::summarise(number_pledges = sum(as.numeric(number_pledges)),
                   verdict = factor(verdict, levels = c(
                     "kept", "partially_kept", "broken")))
EconomicVerdictsDataShort <- EconomicVerdictsData |>
  dplyr::group_by(legislature) |>
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
ggplot2::ggplot(EconomicVerdictsData, ggplot2::aes(
  x = legislature, y = number_pledges, group = verdict,
  fill = as.factor(verdict))) +
  clessnverse::theme_clean_dark() +
  ggplot2::geom_col(position = "fill") +
  ggplot2::scale_x_discrete("", labels = EconomicVerdictsDataYears) +
  ggplot2::scale_y_continuous(
    "% des promesses en économie et employabilité",
    breaks = c(0, 0.2, 0.4, 0.6, 0.8, 1),
    labels = c(0, 20, 40, 60, 80, 100)) +
  ggplot2::scale_fill_manual("Verdict",
    values = c("#228B22", "#F3C349", "#AE0101"),
    labels = c("Réalisée", "Partiellement réalisée", "Rompue")) +
  ggplot2::theme(axis.text.x = ggplot2::element_text(hjust = 0.5, vjust = 0.5,
                                                     angle = 90))
ggplot2::ggsave(paste0("../elxn-qc2022/_SharedFolder_elxn-qc2022/",
                       "presse_canadienne/2022_09_16/CoutdelaviePromesses2",
                       ".png"))
