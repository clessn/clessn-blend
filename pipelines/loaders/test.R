credentials <- hublot::get_credentials(
            Sys.getenv("HUB3_URL"), 
            Sys.getenv("HUB3_USERNAME"), 
            Sys.getenv("HUB3_PASSWORD"))
        


Dictionaries <- clessnverse::get_dictionary("subcategories", lang = "fr",
                                            credentials = credentials)


Data <- data.frame(text = "| valorisation de l  enseignement du français si le français est ce qui nous distingue   il est aussi ce qui nous unit   le français est une source de fierté   une source de savoir   une source de beauté dont il faut en prendre soin    on insiste souvent sur l  importance de maîtriser une seconde langue sur le marché du travail   mais fait moins connu   une mauvaise maîtrise du français a également des conséquences économiques majeures    35    des employeurs auraient rejeté des candidatures pour cause de manque de compétence dans notre langue   les taux d  échec sont alarmants   il est impératif que les élèves soient mieux encadrés pour maîtriser leur langue et assurer leur succès à l  école comme sur le marché du travail   dans un second mandat   un gouvernement de la caq entend mettre l  accent sur l  enseignement du français aux jeunes   en fait   on doit entreprendre une révision en profondeur de l  ensemble des programmes d  enseignement du français   cette réforme permettra aussi d  offrir aux québécois d  expression anglaise les moyens d  améliorer leur français langue seconde")

clessnverse::run_dictionary(Data, text, Dictionaries)

dic1 <- quanteda::dictionary(list(
    #langue_fran = Dictionaries$langue_fran,
    #langue_fran = c("* fran*","loi 96","loi 101","* du français","nation distinct*",
    #                "minorité angl*","langue commune","langue officielle",                       
    #                "charte de la langue française","office québécois de la langue française",
    #                "épreuve uniforme de français","droits linguistiques",
    #                "servi* en français","anglicisation","office de la langue française",
    #                "langue majoritaire","langue minoritaire",
    #                "intégration"),
    langue_fran = c("*fran*","loi 96","loi 101","*du français","nation distinct*",
                    "minorité angl*","langue commune","langue officielle",                       
                    "charte de la langue française","office québécois de la langue française",
                    "épreuve uniforme de français","droits linguistiques",
                    "servi* en français","anglicisation","office de la langue française",
                    "langue majoritaire","langue minoritaire",
                    "intégration"),

    education = c("écol*", "enseign*", "secondaire 1")
    ))
dic2 <- quanteda::dictionary(list(
    langue_fran = c("* fran*", "loi 96"),
    education = c("écol*", "enseign*", "secondaire 1")
    ))

clessnverse::run_dictionary(Data, text, dic1)
clessnverse::run_dictionary(Data, text, dic2)



