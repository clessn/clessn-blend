library(dplyr)

`%vc%` <- clessnverse::`%vcontains%`


clntxt <- function(x) {
   x <- tolower(x)
   x <- trimws(x)
   return(x)
}

"%contains%" <- function(vec_y, x) {
   #checks that all the words in x are in a string in the vector of strings vec_y
   for (y in vec_y) {
      if (strsplit(y, " ") %vc% strsplit(x, " ")) return(TRUE)
   }
   return(FALSE)
}

# connect to hublot
credentials <<- hublot::get_credentials(
Sys.getenv("HUB3_URL"), 
Sys.getenv("HUB3_USERNAME"), 
Sys.getenv("HUB3_PASSWORD"))

# Connecting to hub 2.0
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))

scriptname <- "build_eu_mps"
logger <- clessnverse::log_init("build_eu_mps", "console", "~/logs")

# create filter
filter <- clessnhub::create_filter(type="mp", schema="v3", metadata=list(institution="European Parliament"), data=NULL)

# Now get the data
df_persons <- clessnhub::get_items(table = 'persons', filter = filter, download_data = TRUE)
#df_persons <- df_persons %>% filter(data.fullName != data.fullNameNative)

###############################################################################
#  CLEAN HUB 2 PERSONS
#
# Get eu country codes
# df_country <- clessnverse::get_warehouse_table(
#       table_name = "countries",
#       data_filter = list(),
#       credentials = credentials,
#       nbrows = 0
#     )

# # Cleaned data
# clessnverse::dbxDownloadFile(
#    filename = "/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-europe/DataPersons_Clean.csv",
#    local_path = ".",
#    token = Sys.getenv("DROPBOX_TOKEN")
# )
# df_clean <- read.csv2("./DataPersons_Clean.csv")
# #df_clean <- df_clean %>% filter(!is.na(data.speakerParty) & !is.na(data.speakerPolGroup))
# if (file.exists("./DataPersons_Clean.csv")) file.remove("./DataPersons_Clean.csv")
# df_clean$data.speakerPolGroup <- gsub("Group of the Group", "Group of the", df_clean$data.speakerPolGroup)
# df_clean$data.speakerPolGroup <- gsub("of the of the", "of the", df_clean$data.speakerPolGroup)

# # Official EU Pol Groups
# clessnverse::dbxDownloadFile(
#    filename = "/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-europe/pol_groups.csv",
#    local_path = ".",
#    token = Sys.getenv("DROPBOX_TOKEN")
# )
# df_pol_groups <- read.csv2("./pol_groups.csv")
# if (file.exists("./pol_groups.csv")) file.remove("./pol_groups.csv")
# #clessnverse::dbxUploadFile("pol_groups.csv", "/clessn-blend/_SharedFolder_clessn-blend/data/agoraplus-europe/", Sys.getenv("DROPBOX_TOKEN"), overwrite = T)

# df_new_people <- data.frame(
#    id = character(),
#    full_name = character(),
#    first_name = character(),
#    last_name = character(),
#    full_name_native = character(),
#    pol_group = character(),
#    party = character(),
#    country = character(),
#    gender = character()
# )

# df_exceptions <- data.frame(
#    data.speakerCountry = character(),
#    data.speakerFullName = character(),
#    data.speakerFullNameClean = character(),
#    data.speakerParty = character(),
#    data.speakerPolGroup = character()
# )

# for (i in 156:nrow(df_persons)){
#    full_name <- NA
#    first_name <- NA
#    last_name <- NA
#    full_name_native <- NA
#    gender <- NA
#    country <- NA
#    pol_group <- NA
#    party <- NA
#    pol_group_en <- NA
#    party_en <- NA

#    df1_row <- df_persons[i,]
#    df2_row <- df_clean[clessnverse::rm_accents(tolower(df_clean$data.speakerFullNameClean))  == clessnverse::rm_accents(tolower(df1_row$data.fullName)),]
#    df2_row <- df2_row[!is.na(df2_row$data.speakerFullName),]

#    if (nrow(df2_row) == 0) {
#       df2_row <- df_clean[clessnverse::rm_accents(tolower(df_clean$data.speakerFullNameClean))  == clessnverse::rm_accents(tolower(df1_row$data.fullNameNative)),]
#       df2_row <- df2_row[!is.na(df2_row$data.speakerFullName),]

#       if (nrow(df2_row) == 0) {
#          df2_row <- df_clean[clessnverse::rm_accents(tolower(df_clean$data.speakerFullName))  == clessnverse::rm_accents(tolower(df1_row$data.fullName)),]
#          df2_row <- df2_row[!is.na(df2_row$data.speakerFullName),]

#          if (nrow(df2_row) == 0) {
#             df2_row <- df_clean[clessnverse::rm_accents(tolower(df_clean$data.speakerFullName))  == clessnverse::rm_accents(tolower(df1_row$data.fullNameNative)),]
#             df2_row <- df2_row[!is.na(df2_row$data.speakerFullName),]
#          }
#       }
#    }


#    if (nrow(df2_row) > 0) {
#       #found entry in the clean csv
#       if (nrow(as.data.frame(unique(as.data.frame(lapply(df2_row, clntxt))))) > 1) {
#          # Take the richest row

#          index <- which.max(rowSums(!is.na(df2_row)))

#          df_exceptions <- df_exceptions %>% 
#             rbind(
#                df2_row[-c(index),]
#             )
#          df2_row <- df2_row[index,]
#       } else {
#          if (nrow(as.data.frame(unique(as.data.frame(lapply(df2_row, clntxt))))) == 1) {
#             df2_row <- df2_row[1,]
#          }
#       }

#       full_name = if (!is.na(df2_row$data.speakerFullNameClean)) df2_row$data.speakerFullNameClean else df1_row$data.fullName
#       full_name_native = df1_row$data.fullNameNative
#       first_name <- strsplit(full_name, " ")[[1]][1]
#       gender_df <- gender::gender(clessnverse::rm_accents(first_name))
#       gender <- if (nrow(gender_df) > 0) {
#                   gender_df$gender
#                } else {
#                   if (df1_row$data.isFemale == 0) {
#                      "female"
#                   } else {
#                      "male"
#                   }
#                }

#       country <- df2_row$data.speakerCountry

#       #if (df2_row$data.speakerPolGroup %in% df_pol_groups$pol_group) {
#       if (tolower(df_pol_groups$pol_group) %contains% tolower(df2_row$data.speakerPolGroup)) {
#          pol_group_native <- df2_row$data.speakerPolGroup
#          pol_group_native <- gsub('\"', '', pol_group_native)
#          #pol_group_lang <- clessnverse::detect_language("deeptranslate", pol_group_native)
#          #pol_group_en   <- clessnverse::translate_text(pol_group_native, "deeptranslate", pol_group_lang, "en", TRUE)
#          pol_group_en <- pol_group_native
#          pol_group <- pol_group_en

#          party_native <-  df2_row$data.speakerParty
#          party_native <- gsub('\"', '', party_native)
#          #party_lang <- clessnverse::detect_language("deeptranslate", party_native)
#          #party_en   <- clessnverse::translate_text(party_native, "deeptranslate", party_lang, "en", TRUE)
#          party_en <- party_native
#          party <- party_en
#       } else {
#          pol_group <- "unknown"
#          party <- "unknown"
#       }

      

#       clessnverse::logit(scriptname, paste("found", full_name), logger)

#       if(is.na(full_name)) stop()

#       df_new_people <- 
#          df_new_people %>% rbind(
#             data.frame(
#                full_name = full_name,
#                first_name = first_name,
#                last_name = trimws(gsub(first_name, "", full_name)),
#                full_name_native = full_name_native,
#                gender = gender,
#                country = country,
#                pol_group = pol_group,
#                party = party
#             )
#          )
   
#       clessnverse::logit(scriptname, paste("got", full_name, "as", gender, "from", pol_group_en, "/", party_en, "in", country), logger)

#    } else {
#       #did not find entry in cleaned csv
#       clessnverse::logit(scriptname, paste("could not find", df1_row$data.fullName), logger)
#       full_name = df1_row$data.fullName
#       full_name_native = df1_row$data.fullNameNative
#       first_name <- strsplit(full_name, " ")[[1]][1]
#       gender_df <- gender::gender(clessnverse::rm_accents(first_name))
#       gender <- if (nrow(gender_df) > 0) {
#                   gender_df$gender
#                 } else {
#                   if (df1_row$data.isFemale == 0) {
#                      "male"
#                   } else {
#                      "female"
#                   }
#                 }
#       country <- df1_row$metadata.country

#       if ( (!is.null(df1_row$data.currentParty) && !is.na(df1_row$data.currentParty) && length(df1_row$data.currentParty) != 0) &&
#            (!is.null(df1_row$data.currentPolGroup) && !is.na(df1_row$data.currentPolGroup) && length(df1_row$data.currentPolGroup) != 0) ) {

#          if (df_pol_groups$pol_group %contains% df1_row$data.currentParty) {
#             pol_group_native <- if (!is.null(df1_row$data.currentParty) && !is.na(df1_row$data.currentParty) && length(df1_row$data.currentParty) != 0 ) df1_row$data.currentParty else "unknown"
#             pol_group_native <- gsub('\"', '', pol_group_native)
#             #pol_group_lang <- clessnverse::detect_language("deeptranslate", pol_group_native)
#             #pol_group_en   <- clessnverse::translate_text(pol_group_native, "deeptranslate", pol_group_lang, "en", TRUE)
#             pol_group_en <- pol_group_native
#             pol_group <- pol_group_en

#             party_native <- if (!is.null(df1_row$data.currentPolGroup) && !is.na(df1_row$data.currentPolGroup) && length(df1_row$data.currentPolGroup) != 0 ) df1_row$data.currentPolGroup else "unknown"
#             party_native <- gsub('\"', '', party_native)
#             #party_lang <- clessnverse::detect_language("deeptranslate", party_native)
#             #party_en   <- clessnverse::translate_text(party_native, "deeptranslate", party_lang, "en", TRUE)
#             party_en <- party_native
#             party <- party_en
#          } else {
#             pol_group <- "unknown"
#             party <- "unknown"
#          }

#          if (df_pol_groups$pol_group %contains% df1_row$data.currentPolGroup) {
#             pol_group_native <- if (!is.null(df1_row$data.currentPolGroup) && !is.na(df1_row$data.currentPolGroup) && length(df1_row$data.currentPolGroup) != 0 ) df1_row$data.currentPolGroup else "unknown"
#             pol_group_native <- gsub('\"', '', pol_group_native)
#             #pol_group_lang <- clessnverse::detect_language("deeptranslate", pol_group_native)
#             #pol_group_en   <- clessnverse::translate_text(pol_group_native, "deeptranslate", pol_group_lang, "en", TRUE)
#             pol_group_en <- pol_group_native
#             pol_group <- pol_group_en

#             party_native <- if (!is.null(df1_row$data.currentParty) && !is.na(df1_row$data.currentParty) && length(df1_row$data.currentParty) != 0 ) df1_row$data.currentParty else "unknown"
#             party_native <- gsub('\"', '', party_native)
#             #party_lang <- clessnverse::detect_language("deeptranslate", party_native)
#             #party_en   <- clessnverse::translate_text(party_native, "deeptranslate", party_lang, "en", TRUE)
#             party_en <- party_native
#             party <- party_en

#          } else {
#             pol_group <- "unknown"
#             party <- "unknown"
#          }
#       }
      
#       df_new_people <- 
#          df_new_people %>% rbind(
#             data.frame(
#                full_name = df1_row$data.fullName,
#                first_name = first_name,
#                last_name = trimws(gsub(first_name, "", full_name)),
#                full_name_native = full_name_native,               
#                pol_group = pol_group,
#                party = party,
#                gender = gender,
#                country = country
#             )
#          )
#    }
# }

# df <- df_new_people %>% unique()

# df <- df %>% 
#    group_by(full_name, first_name, last_name, gender, party, pol_group, country) %>% 
#    dplyr::summarise(other_names=paste(full_name_native, collapse=" | "))

# df$full_name <- stringr::str_to_title(df$full_name)
# df$first_name <- stringr::str_to_title(df$first_name)
# df$last_name <- stringr::str_to_title(df$last_name)

# df <- df[-c(which(duplicated(df$full_name) & is.na(df$party) & is.na(df$pol_group))),]

# clessnverse::detect_language("fastText", full_name)
# clessnverse::translate_text(full_name, "deeptranslate", "hu", "en", TRUE)
