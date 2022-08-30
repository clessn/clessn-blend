library(dplyr)

df <- data.frame(word="", frequency=0)

for (i in 1:50000) {
    words <- data.frame()

    if (grepl(tolower(paste(dict_ai$ai, collapse="|")), tolower(Subset$text[i]))) {

        words <- data.frame(word=attributes(table(stringr::str_match(Subset$text[i], dict_ai$ai)))$dimnames[[1]])

        if (nrow(words) > 0) {
            for (j in 1:nrow(words)) {
                if (nrow(df[df$word == words$word[j],]) > 0) {
                    df$frequency[df$word == words$word[j]] <- df$frequency[df$word == words$word[j]] + 1
                } else {
                    df <- df %>% bind_rows(data.frame(word = words$word[j], frequency = 1))
                }
            }
        }
    }
}

df

ggplot2::ggplot(data = df, aes(x = word, y=frequency)) + 
    ggplot2::geom_bar(stat = 'identity', position="dodge", width=1) +
    ggplot2::theme_minimal() +
    ggplot2::theme(axis.text.x = element_text(angle=90, vjust = 0.5))
