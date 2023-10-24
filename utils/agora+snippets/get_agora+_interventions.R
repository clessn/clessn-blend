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
my_table <- "agoraplus_european_parliament"

# On peut charger toute la table : Pour cela on crée un filtre vide
my_filter <- list()
my_filter <- list(data__event_date = "2016-05-09")


df <- clessnverse::get_warehouse_table(
    table_name = my_table,
    data_filter = my_filter,
    credentials = my_credentials,
    nbrows = 0
)

# Ou spécifier un filtre pour charger seulement les données dont on a besoin
# Pour cela il faut connaitre les champs de la table
# Pour voir un enregistrement dans la table et en connaitre les champs,
# 1. soit passer par l'admin de hublot: https://clhub.clessn.cloud/admin/
#    et aller naviguer dans la table en question (ex: warehouse_agoraplus_quebec_national_assembly)
#    ouvrir une observation et regarder sa structure.
# 2. soit charger juste quelques (genre 1000) rows de la table sans filtre ici dans R
#    et regarder la structure du dataframe.  Pour seulement charger quelques lignes, simplement
#    donner une valeur à nbrows dans l'appel à la fonction clessnverse::get_warehouse_table
#    par exemple nbrows = 10 chargera les 10 premières observations de la table dans un dataframe
# 
# Pour voir les types de filtres possibles (my_filter), 
# voir https://github.com/clessn/hublotr#t%C3%A9l%C3%A9charger-un-subset-des-donn%C3%A9es-gr%C3%A2ce-au-filtrage
#
my_table <- "agoraplus_qcvintage_national_assembly"

my_filter <- list(data__event_date__lt = "1981-12-07")

# Charger la table
df <- clessnverse::get_warehouse_table(
    table_name = my_table,
    data_filter = my_filter,
    credentials = my_credentials,
    nbrows = 0
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
    nbrows = 0
)

# On combine les deux dataframe
final_df <- df %>% dplyr::bind_rows(df1)
