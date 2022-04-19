library(dplyr)

suffix <- 0
df <- data.frame()

for (i in 1:60) {
    suffix <- suffix + 100

    if (i > 1) {
        import <- read.csv2(paste("_SharedFolder_clessn-blend/data/agoraplus-vintage/data/agoraplus-vintage-", suffix, ".csv", sep = ""))
        import$data.interventionID <- as.numeric(import$data.interventionID)
        df <- df %>% bind_rows(import)
    } else {
        df <- read.csv2("_SharedFolder_clessn-blend/data/agoraplus-vintage/data/agoraplus-vintage-100.csv")
    }
}