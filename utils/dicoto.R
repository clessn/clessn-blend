library(dplyr)

df <- data.frame(
    survey_respondant = c(
        "joe",
        "mike",
        "rob",
        "mitch",
        "huguette"
    ),
    survey_answer = c(
        "grand,moyen",
        "petit,grand",
        "grand,petit,extra-petit",
        "extra-large,petit,moyen",
        "grand,petit,extra-large,moyen,extra-petit"
    )
)


new_df <- df %>% cbind(textshape::mtabulate(strsplit(df$survey_answer, ',',fixed=TRUE)))

cat(nrow(new_df))