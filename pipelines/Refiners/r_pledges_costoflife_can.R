credentials <- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))

#### Cost of life pledges by legislature ####
PledgeLabelsHistorical <- clessnverse::get_warehouse_table(
  "pledge_labels_historical", credentials) |>
  dplyr::filter(province_or_state == "CAN") # load Canadian government pledge labels
PledgeLabels2019 <- openxlsx::read.xlsx(paste(
  "../polimetre-dev/_SharedFolder_polimetre-fonctionnement/5. Polimètres archivés/6.",
  "Polimètre Fédéral (Trudeau-43)/polimetre_trudeau-43.xlsm"), 2)
PledgeLabels <- dplyr::full_join(PledgeLabelsHistorical, PledgeLabels2019, by = c(
  "french_label" = "Libellé.FR",
  "english_label" = "Libellé.ENG",
  "pledge_number" = "#",
  "policy_domain" = "Domaine",
  "verdict" = "Verdict.dernière.MaJ"))
PledgeLabels$legislature[is.na(PledgeLabels$legislature)] <- 43
PledgeLabels2021 <- openxlsx::read.xlsx(paste(
  "../polimetre-dev/_SharedFolder_polimetre-fonctionnement/6. Polimètre Fédéral",
  "(Trudeau-44)/polimetre_trudeau-44.xlsm"), 2)
PledgeLabels <- dplyr::full_join(PledgeLabels, PledgeLabels2021, by = c(
  "french_label" = "Libellé.FR",
  "english_label" = "Libellé.ENG",
  "pledge_number" = "#",
  "policy_domain" = "Domaine",
  "verdict" = "Verdict.dernière.MaJ"))
PledgeLabels$legislature[is.na(PledgeLabels$legislature)] <- 44
PledgeLabels$political_party[is.na(PledgeLabels$political_party)] <- "PLC"
PledgeLabels$province_or_state[is.na(PledgeLabels$province_or_state)] <- "CAN"
PledgeLabels$country[is.na(PledgeLabels$country)] <- "CAN"
PledgeLabels$promise_tolower_fr <-
  PledgeLabels$french_label |>
  tolower() |> # prepare pledge labels for dictionary analysis (lowercase, no punctuation)
  stringr::str_remove_all("\\[|\\]") |>
  stringr::str_replace_all("[[:punct:]]", "  ")
PledgeLabels$promise_tolower_en <-
  PledgeLabels$english_label |>
  tolower() |> # prepare pledge labels for dictionary analysis (lowercase, no punctuation)
  stringr::str_remove_all("\\[|\\]") |>
  stringr::str_replace_all("[[:punct:]]", "  ")
DictionariesFR <- clessnverse::get_dictionary( # get cost of life dictionary
  "subcategories", lang = "fr", credentials = credentials)
DictionariesEN <- clessnverse::get_dictionary( # get cost of life dictionary
  "subcategories", lang = "en", credentials = credentials)
DictionariesFR$cost_life <- DictionariesFR$cost_life |>
  stringr::str_replace_all("'", "*") # adapt dictionary for mistakes
PledgeLabelsDictionariesFR <- clessnverse::run_dictionary(
  PledgeLabels, promise_tolower_fr, DictionariesFR) # calculate number of cost of life pledges
PledgeLabelsDictionariesEN <- clessnverse::run_dictionary(
  PledgeLabels, promise_tolower_en, DictionariesEN) # calculate number of cost of life pledges
PledgeLabels$cost_life <- PledgeLabelsDictionariesFR$cost_life +
  PledgeLabelsDictionariesEN$cost_life
PledgeLabels$cost_life[PledgeLabels$cost_life > 1] <- 1
PledgeLabelsCostLife <- dplyr::filter(PledgeLabels, cost_life == 1)
write.csv(PledgeLabelsCostLife,
          "../article_inflation/Data/19932021PledgesCostOfLiving.csv")
InflationPledgesByLegislature <- PledgeLabels |>
  dplyr::group_by(legislature) |> # group pledge totals by mandate
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T),
                   number_pledges = dplyr::n())
InflationPledgesByLegislature$years <- c(
  paste0("Chrétien\n\n1993-1997 (n = ", InflationPledgesByLegislature$number_pledges[1], ")"),
  paste0("Chrétien\n\n1997-2000 (n = ", InflationPledgesByLegislature$number_pledges[2], ")"),
  paste0("Chrétien/Martin\n\n2000-2004 (n = ", InflationPledgesByLegislature$number_pledges[3], ")"), # give labels to each mandate
  paste0("Martin\n\n2004-2006 (n = ", InflationPledgesByLegislature$number_pledges[4], ")"),
  paste0("Harper\n\n2006-2008 (n = ", InflationPledgesByLegislature$number_pledges[5], ")"),
  paste0("Harper\n\n2008-2011 (n = ", InflationPledgesByLegislature$number_pledges[6], ")"),
  paste0("Harper\n\n2011-2015 (n = ", InflationPledgesByLegislature$number_pledges[7], ")"),
  paste0("Trudeau\n\n2015-2019 (n = ", InflationPledgesByLegislature$number_pledges[8], ")"),
  paste0("Trudeau\n\n2019-2021 (n = ", InflationPledgesByLegislature$number_pledges[9], ")"),
  paste0("Trudeau\n\n2021-... (n = ", InflationPledgesByLegislature$number_pledges[10], ")"))
ggplot2::ggplot(InflationPledgesByLegislature, ggplot2::aes(
  x = legislature, y = cost_life)) +
  ggplot2::geom_line(ggplot2::aes(group = 1)) +
  ggplot2::scale_x_discrete("", labels = InflationPledgesByLegislature$years) +
  ggplot2::ylab("Number of government pledges on cost of living") +
  ggplot2::theme(axis.text.x = ggplot2::element_text(hjust = 1, angle = 90,
                                                     lineheight = 0.35))
ggplot2::ggsave("../article_inflation/Graphs/CostOfLivingPledgesPerYear.png",
                width = 8, height = 5.5)
# in this Excel file, count manually number of pledges in each paragraph in a new column

#### Cost of life pledges by 2021 manifesto ####
PartyPlatforms2021 <- clessnverse::get_warehouse_table(
  "manifestos_can2021", credentials) # get 2022 manifestos
PartyPlatforms2021$paragraph_tolower <- PartyPlatforms2021$paragraph |>
  tolower() |> # prepare manifestos for analysis (lowercase, remove punctuation)
  stringr::str_replace_all("[[:punct:]]", "  ")
PartyPlatforms2021FR <- dplyr::filter(PartyPlatforms2021, language == "FR")
PartyPlatforms2021EN <- dplyr::filter(PartyPlatforms2021, language == "EN")
PartyPlatforms2021DictionariesFR <- clessnverse::run_dictionary(
  PartyPlatforms2021FR, paragraph_tolower, DictionariesFR) # calculate number of cost of life mentions
PartyPlatforms2021DictionariesEN <- clessnverse::run_dictionary(
  PartyPlatforms2021EN, paragraph_tolower, DictionariesEN) # calculate number of cost of life mentions
PartyPlatforms2021FR$cost_life <- PartyPlatforms2021DictionariesFR$cost_life
PartyPlatforms2021EN$cost_life <- PartyPlatforms2021DictionariesEN$cost_life
PartyPlatforms2021Merged <- rbind(PartyPlatforms2021FR, PartyPlatforms2021EN) |>
  dplyr::filter(cost_life == 1)
write.csv(PartyPlatforms2021Merged,
          "../article_inflation/Data/2021ManifestoStatementsCostOfLiving.csv")
InflationPledgesByParty <- PartyPlatforms2021Merged |>
  dplyr::group_by(political_party) |> # group cost of life mentions by party
  dplyr::summarise(cost_life = sum(cost_life, na.rm = T))
InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "PLC"] <-
  InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "PLC"
  ] - 4
InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "PCC"] <-
  InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "PCC"
  ] - 4
InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "NPD"] <-
  InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "NPD"
  ] - 7
InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "PPC"] <-
  InflationPledgesByParty$cost_life[InflationPledgesByParty$political_party == "PPC"
  ] - 1

#### Economy pledges by legislature ####
PolimeterHistorical <- clessnverse::get_warehouse_table(
  "polimeter_historical", credentials) |>
  dplyr::filter(province_or_state == "CAN") # load Canadian government pledge totals
Polimeter2019 <- openxlsx::read.xlsx(paste(
  "../polimetre-dev/_SharedFolder_polimetre-fonctionnement/5. Polimètres archivés/6.",
  "Polimètre Fédéral (Trudeau-43)/polimetre_trudeau-43.xlsm"), 1)
Polimeter <- dplyr::full_join(PolimeterHistorical, Polimeter2019, by = c(
  "number_pledges" = "Réalisée",
  "policy_domain" = "Domaines.de.politique"))
Polimeter$verdict[is.na(Polimeter$verdict)] <- "kept"
Polimeter$number_pledges <- as.numeric(Polimeter$number_pledges)
Polimeter <- dplyr::full_join(Polimeter, Polimeter2019, by = c(
  "number_pledges" = "Partiellement.réalisée",
  "policy_domain" = "Domaines.de.politique"))
Polimeter$verdict[is.na(Polimeter$verdict)] <- "partially_kept"
Polimeter <- dplyr::full_join(PolimeterHistorical, Polimeter2019, by = c(
  "number_pledges" = "Rompue",
  "policy_domain" = "Domaines.de.politique"))
Polimeter$verdict[is.na(Polimeter$verdict)] <- "broken"
Polimeter$legislature[is.na(Polimeter$legislature)] <- 43
Polimeter$province_or_state[is.na(Polimeter$province_or_state)] <- "CAN"
Polimeter$country[is.na(Polimeter$country)] <- "CAN"
Polimeter$first_minister[is.na(Polimeter$first_minister)] <- "Trudeau"
Polimeter$government_type[is.na(Polimeter$government_type)] <- "Min"
EconomicVerdictsData <- Polimeter |>
  dplyr::filter(policy_domain == "Économie et employabilité") |>
  dplyr::group_by(legislature, verdict) |> # group number of economic pledges by fulfillment status by mandate
  dplyr::summarise(number_pledges = sum(as.numeric(number_pledges)),
                   verdict = factor(verdict, levels = c(
                     "kept", "partially_kept", "broken")))
EconomicVerdictsDataShort <- EconomicVerdictsData |>
  dplyr::group_by(legislature) |> # group number of economic pledges by mandate
  dplyr::summarise(number_pledges = sum(number_pledges, na.rm = T))
EconomicVerdictsDataYears <- c(
  paste0("Chrétien\n\n1993-1997 (n = ",
         EconomicVerdictsDataShort$number_pledges[1], ")"),
  paste0("Chrétien\n\n1997-2000 (n = ",
         EconomicVerdictsDataShort$number_pledges[2], ")"),
  paste0("Chrétien/Martin\n\n2000-2004 (n = ",
         EconomicVerdictsDataShort$number_pledges[3], ")"),
  paste0("Martin\n\n2004-2006 (n = ",
         EconomicVerdictsDataShort$number_pledges[4], ")"),
  paste0("Harper\n\n2006-2008 (n = ",
         EconomicVerdictsDataShort$number_pledges[5], ")"),
  paste0("Harper\n\n2008-2011 (n = ",
         EconomicVerdictsDataShort$number_pledges[6], ")"),
  paste0("Harper\n\n2011-2015 (n = ",
         EconomicVerdictsDataShort$number_pledges[7], ")"),
  paste0("Trudeau\n\n2015-2019 (n = ",
         EconomicVerdictsDataShort$number_pledges[8], ")"),
  paste0("Trudeau\n\n2019-2021 (n = ",
         EconomicVerdictsDataShort$number_pledges[9], ")"))
ggplot2::ggplot(EconomicVerdictsData, ggplot2::aes(
  x = legislature, y = number_pledges, group = verdict,
  fill = as.factor(verdict))) +
  ggplot2::geom_col(position = "fill") +
  ggplot2::scale_x_discrete("", labels = EconomicVerdictsDataYears) +
  ggplot2::scale_y_continuous(
    "% des promesses en économie et employabilité",
    breaks = c(0, 0.2, 0.4, 0.6, 0.8, 1),
    labels = c(0, 20, 40, 60, 80, 100)) +
  ggplot2::scale_fill_manual("Verdict",
                             values = c("#228B22", "#F3C349", "#AE0101"),
                             labels = c("Réalisée", "Partiellement réalisée",
                                        "Rompue")) +
  ggplot2::theme(plot.title = ggplot2::element_text(lineheight = 0.35),
                 axis.text.x = ggplot2::element_text(
                   hjust = 0.5, vjust = 0.5, angle = 90, lineheight = 0.35)) +
  ggplot2::ggtitle(paste("Les gouvernements canadiens respectent-ils\n\nleurs",
                         "promesses sur l'économie?"),
                   subtitle = paste("État de réalisation des promesses en",
                                    "économie et employabilité depuis 1994"))
ggplot2::ggsave("../article_inflation/Graphs/CoutdelaviePromesses2.png",
                width = 8, height = 5.5)
