# connect to hublot
# see https://github.com/clessn/Renviron_tutorial
my_credentials <- hublot::get_credentials(
  hub_url = Sys.getenv('HUB3_URL'),
  username = Sys.getenv('HUB3_USERNAME'),
  password = Sys.getenv('HUB3_PASSWORD')
)


# Choisir la table dasns l'entrepôt de données qui a été générée par un chargeur 
# Simplement enlever le commentaire devant la table qu'on veut charger
#my_table <- "agoraplus_quebec_national_assembly"
#my_table <- "agoraplus_canada_house_of_commons"
#my_table <- "agoraplus_qc_press_conferences"
#my_table <- "agoraplus_qcvintage_national_assembly"
#my_table <- "agoraplus_european_parliament"

# On peut charger toute la table
my_table <- "agoraplus_qc_press_conferences"

# Pour cela on crée un filtre vide
my_filter <- list()

df <- clessnverse::get_warehouse_table(
    table_name = my_table,
    data_filter = my_filter,
    credentials = my_credentials,
)

# Ou spécifier un filtre pour charger seulement les données dont on a besoin
# Pour cela il faut connaitre les champs de la table
# Pour voir un enregistrement dans la table et en connaitre les champs,
# soit passer par l'admin de hublot: 
# soit charger juste quelques (genre 1000) rows de la table sans filtre.
# Pour voir les types de filtres, voir
my_table <- "agoraplus_qcvintage_national_assembly"

my_filter <- list(data__event_date__lt = "1981-12-07")

# Charger la table
df <- clessnverse::get_warehouse_table(
    table_name = my_table,
    data_filter = my_filter,
    credentials = my_credentials,
)

# Si des datasets sont trop gros (comme QC Vintage), ça peut être nécessaire de les spliter
# avec des filtres sinon R crash ou hublot timeout pour prendre toutes les données.
# Par exemple en addition à l'exemple ci-dessus:
my_filter <- list(data__event_date__gte = "1981-12-07")


# Charger la table
df1 <- clessnverse::get_warehouse_table(
    table_name = my_table,
    data_filter = my_filter,
    credentials = my_credentials,
)

# On combine les deux dataframe
final_df <- df %>% dplyr::bind_rows(df1)
