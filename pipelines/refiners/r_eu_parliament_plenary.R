###############################################################################
################         Script Definitions and Specs        ##################
###############################################################################
#                                                                             
#                                                                             
#                           r_eu_parliament_plenary                        
#                                                                             
# This script retrieves the interventions from the agoraplus_european_parliament
# warehouse table and crosses it 
#                                                                             
###############################################################################


###############################################################################
########################       Auxiliary functions       ######################
###############################################################################

"%contains_one_of%" <- function(vec_y, x) {
   #checks that at least one the words in x is in a string within the vector of strings vec_y
   for (y in vec_y) {
      if (all(strsplit(y, " ")[[1]] %in% strsplit(x, " ")[[1]])) return(TRUE)
   }
   return(FALSE)
}


which_contains_one_of <- function(vec_y, x) {
  #checks that at least one the words in x is in a string within the vector of strings vec_y
  found <- FALSE
  best <- ""

  for (y in vec_y) {
    if (all(strsplit(y, " ")[[1]] %in% strsplit(x, " ")[[1]])) {
      found <- TRUE
      if (nchar(y) > nchar(best)) best <- y
    }
  }

  if (found) {
    return(best)
  } else {
    return("")
  }
}


detect_president <- function(x) {
  return (  tolower(president) %contains_one_of% tolower(x) ||
            tolower(vicepresident) %contains_one_of% tolower(x) ||
            tolower(presidency) %contains_one_of% tolower(x) ||
            tolower(presidency_of_the_hon) %contains_one_of% tolower(x) )
}


get_speaker <- function(full_name, full_name_native) {
  # Get the speaker data from hub 2.0.  If absent try to get it from the parliament.
  # If parliament successful and not in hub, then write in hub for next time
  df <- data.frame()

  full_name_trimed <- gsub("\\s+", " ", full_name)
  full_name_native_trimed <- gsub("\\s+", " ", full_name_native)
  full_name_nostopword <- tm::removeWords(tolower(full_name), stopwords::stopwords())
  full_name_native_nostopword <- tm::removeWords(tolower(full_name_native), stopwords::stopwords())
  full_name_noaccent <- clessnverse::rm_accents(full_name)
  full_name_native_noaccent <- clessnverse::rm_accents(full_name_native)

  names <- c(
    full_name, full_name_native, 
    full_name_trimed, full_name_native_trimed,
    full_name_nostopword, full_name_native_nostopword,
    full_name_noaccent, full_name_native_noaccent
  )

  for (i in names) {
    df <- df_people[which(tolower(df_people$full_name) == tolower(i)),]
    if (nrow(df) != 0) break

    df <- df_people[which(grepl(tolower(i), tolower(df_people$other_names))),]
    if (nrow(df) != 0) break
  }


  if (nrow(df) == 0) {
    #did not find it in the hub - check the parliament db
    for (i in names) {
      df_parliament <- clessnverse::get_europe_mep_data(i)
      if (!is.na(df_parliament$mepid)) break
    }

    if (!is.na(df_parliament$mepid)) {
      #found it
      fullname <- df_parliament$fullname
      first_name <- trimws(stringr::str_to_title(stringr::str_split(full_name, "\\s")[[1]][[1]]))
      last_name  <- trimws(stringr::str_to_title(stringr::str_match(full_name, paste("^",first_name,"(.*)$",sep=''))[2]))

      df <- data.frame(
        full_name = paste(last_name, first_name, sep=", "),
        pol_group = df_parliament$polgroup,
        party = df_parliament$party,
        country = unique(df_country$short_name_3[df_country$name == df_parliament$country])
      )
    }
  }

  return(df)
}



###############################################################################
###############################################################################
#####################           core functions          #######################
###############################################################################
###############################################################################


strip_and_push_intervention <- function(intervention) {
  speaker_full_name <- NA
  speaker_full_name_native <- NA
  speaker_gender <- NA
  speaker_polgroup <- NA
  speaker_party <- NA
  speaker_country <- NA

  speaker_type <- NA

  header <- intervention$intervention_header_en
  header_native <- intervention$intervention_header
  header1 <- NA
  header2 <- NA

  if (is.na(header)) {
    clessnverse::logit(scriptname, paste("intervention header of ", intervention$key, "is missing"), logger)
    warning(paste("intervention header of ", intervention$key, "is missing"))
    status <<- 2

    return()
  }


  clessnverse::logit(scriptname, paste("\n\n\nstarting header:", header), logger)

  if (length(stringr::str_split(header,",")[[1]]) >= 2) {
    # format of type "full name (POLG), blah blah blah"
    header1 <- stringr::str_split(header,",")[[1]][1]
    header1 <- trimws(header1, "both")
    header1_native <- stringr::str_split(header_native,",")[[1]][1]
    header1_native <- trimws(header1_native, "both")
    
    header2 <- stringr::str_split(header,",")[[1]][2:length(stringr::str_split(header,",")[[1]])]
    header2 <- trimws(header2)
    header2 <- paste(header2, collapse = ', ')
    header2 <- trimws(header2)     
    header2_native <- stringr::str_split(header_native,",")[[1]][2:length(stringr::str_split(header_native,",")[[1]])]
    header2_native <- trimws(header2_native)
    header2_native <- paste(header2_native, collapse = ', ')
    header2_native <- trimws(header2_native)     

    #now figure out what's in header1 and header2 (name, pol_group, intervention type etc.)
    if ( stringr::str_detect(header1, "\\((.*)\\)") ) {
      speaker_full_name <- stringr::str_replace(header1, "\\((.*)\\)", "")
      speaker_full_name <- gsub("\\s+", " ", stringr::str_trim(speaker_full_name))
      speaker_full_name_native <- stringr::str_replace(header1_native, "\\((.*)\\)", "")
      speaker_full_name_native <- gsub("\\s+", " ", stringr::str_trim(speaker_full_name_native))
    } else {
      speaker_full_name <- header1
      speaker_full_name_native <- header1_native
    }

    if ( tolower(dignitary) %contains_one_of% tolower(header2) ) {
      speaker_type <- "dignitary or guest speaker"
    } else {
      if ( type_of_speakers %contains_one_of% header2 ) {
        speaker_type <- header2
      } else {
        if (grepl("behalf", tolower(header2))) {
          speaker_type <- paste("mp", header2)
        } else {
          speaker_type <- which_contains_one_of(
            c(president_of_the_commission, president_of_the_committee, vicepresident, vicepresident), 
            tolower(header)
          )
          
          if (nchar(speaker_type) == 0)  speaker_type <- NA

        }
      }
    }

  } else {

    if (length(stringr::str_split(header,",")[[1]]) == 1) {

      if (detect_president(header)) {
        speaker_full_name <- intervention$president_name
        speaker_full_name_native <- intervention$president_name
        
        if (c(president_of_the_commission, president_of_the_committee, vicepresident, vicepresident, president) %contains_one_of% tolower(header)) {
          speaker_type <- trimws(gsub(speaker_full_name, "", header))
        }
        

        speaker_type <- trimws(gsub(speaker_full_name, "", header))

      } else {
        if ( stringr::str_detect(header, "\\((.*)\\)") ) {
          speaker_full_name <- stringr::str_replace(header, "\\((.*)\\)", "")
          speaker_full_name <- gsub("\\s+", " ", stringr::str_trim(speaker_full_name))
          speaker_full_name_native <- stringr::str_replace(header_native, "\\((.*)\\)", "")
          speaker_full_name_native <- gsub("\\s+", " ", stringr::str_trim(speaker_full_name_native))
          speaker_type <- which_contains_one_of(
            c(president_of_the_commission, president_of_the_committee, vicepresident, vicepresident, president), 
            tolower(header)
          )
          if (speaker_type == "") speaker_type <- "mp"
        }
      }

    } else {
      clessnverse::logit(scriptname, paste("unknown header parsing pattern:", header), logger)
      stop(paste("unknown header parsing pattern:", header))
    }
  }

  speaker_full_name <- stringr::str_to_title(trimws(speaker_full_name))
  speaker_full_name_native <- stringr::str_to_title(trimws(speaker_full_name_native))

  # we must come out of the above if with a full_name or else it's a warning!
  # get the speaker attributes from the hub
  df_speaker <- get_speaker(speaker_full_name, speaker_full_name_native)

  if (nrow(df_speaker) > 0) {
    # take the richest row
    df_speaker <- df_speaker[which.max(rowSums(!is.na(df_speaker))),]

    if (nrow(df_speaker) > 1) df_speaker <- df_speaker[1,]

    speaker_full_name <- df_speaker$full_name
    speaker_gender <- df_speaker$gender
    speaker_polgroup <- df_speaker$pol_group
    speaker_party <- df_speaker$party
    speaker_country <- unique(df_country$name[df_country$short_name_3 == df_speaker$country])
    if (length(speaker_country) == 0) speaker_country <- NA
  } else {
    clessnverse::logit(scriptname, paste("could not find", speaker_full_name, "in the people table in the hub"), logger)
    warning(paste("could not find", speaker_full_name, "in the people table in the hub"))
    status <<- 2
    # we dont want to lose information in out datamart so we'll bring the full intervention header as full_name
    speaker_full_name <- header
  }

  clessnverse::logit(
    scriptname,
    paste(
      "\nheader is ", header, 
      "processing intervention from ", speaker_full_name,
      " (", speaker_full_name_native, "), gender=", speaker_gender,
      ", from country ", speaker_country, ", belonging to party ", speaker_party,
      ", and political group ", speaker_polgroup, 
      ", as speaker type ", speaker_type,
      sep=""
    ),
    logger
  )

  # commit


  intervention <- intervention %>% 
    cbind(data.frame(
      speaker_full_name = speaker_full_name,
      speaker_full_name_native = speaker_full_name_native,
      speaker_gender = speaker_gender,
      speaker_country = speaker_country,
      speaker_party = speaker_party,
      speaker_polgroup = speaker_polgroup,
      speaker_type = speaker_type
      ) 
    )

  if (opt$backend == "hub") {
    nb_attempts <- 0
    write_success <- FALSE
    while (!write_success && nb_attempts < 20) {
      tryCatch(
        {
          r <- clessnverse::commit_mart_row(
            table = mart_table,
            key = intervention$hub.key, 
            row = as.list(intervention[1,c(which(!grepl("hub.",names(intervention))))]),
            refresh_data = T,
            credentials = credentials
          )
          write_success <- TRUE
        },
        error = function(e) {
          clessnverse::logit(scriptname, paste("error rwiting to hub:", e$message, "on attempt", nb_attempts, ". sleeping 30 seconds"), logger)
          sleep(30)
        },
        finally={
          nb_attempts <- nb_attempts + 1
        }

      )
    }
  } else {
    my_df <- my_df %>%
      rbind(intervention)
  }

} #</strip_and_push_intervention>



###############################################################################
########################               Main              ######################
###############################################################################

main <- function() {
    
  clessnverse::logit(scriptname, "starting main function", logger)
  clessnverse::logit(scriptname, paste("retrieving debates interventions from data warhouse ", wh_table, sep=''), logger)

  if (opt$method[[1]] == "date_range") {
    my_filter <- list(
      data__event_date__gte = opt$method[[2]],
      data__event_date__lte = opt$method[[3]]
    )
  }

  if (opt$backend != "hub") my_df <<- data.frame()

  df_interventions <- clessnverse::get_warehouse_table(
    table_name = wh_table,
    data_filter = my_filter,
    credentials = credentials,
    nbrows = 0
  )

  if (nrow(df_interventions) > 0) {
    clessnverse::logit(scriptname, paste("retrieved", nrow(df_interventions), "observations"), logger)

    nb_warehouse_items <<- nrow(df_interventions)

    nb_debates <- 0
    previous_event_id <- ""

    for (i in 1:nrow(df_interventions)) {

      intervention <- df_interventions[i,]

      current_event_id <- intervention$event_id
      if (previous_event_id != current_event_id) nb_debates <- nb_debates + 1

      key <- paste(
        intervention$event_id,"-", 
        intervention$intervention_seq_num, "-", 
        opt$schema, 
        sep=""
      )

      if (!opt$refresh_data) {
        tryCatch(
          {
            clessnverse::logit(scriptname, paste("checking if ",key," already exists", sep=""), logger)
            item_check <- hublot::filter_table_items(
              credentials = credentials,
              table_name = paste("clhub_tables_mart_", mart_table, sep=''), 
              filter = list(key = key)
            )

            if (length(item_check$results) == 1) {
              clessnverse::logit(
                scriptname, 
                paste("item ", key, " already exists, skipping...", sep=""),
                logger)
              next
            }
          },
          error=function(e) {},
          finally = {}
        )
      }

      strip_and_push_intervention(intervention)
      nb_interventions <<- nb_interventions + 1
      previous_event_id <- current_event_id
    } #for (i in 1:nrow(df_interventions))

  } else {
    stop(paste("no intervention were found with options povided"), paste(names(opt), opt, sep = "=", collapse = " "))
  } #if (nrow(df_interventions) > 0)

  clessnverse::logit(scriptname, "ending main function", logger)  
} #</main>



tryCatch( 
  withCallingHandlers(
  {
    `%>%` <- dplyr::`%>%`

    Sys.setlocale("LC_TIME", "fr_CA.utf8")

    wh_table <<- "agoraplus_european_parliament"
    mart_table <<- "agoraplus_european_parliament"
    
    status <<- 0
    final_message <<- ""
    nb_interventions <<- 0
    nb_debates <<- 0
    nb_warehouse_items <<- 0

    president <<- tolower(unique(c(
      "president","президент","predsjednik","başkan","prezident","formand","presidentti","président",
      "präsident","Πρόεδρος","elnök","preside","uachtarán","Presidente","prezidents","prezidentas",
      "presidint","prezydent","presedinte","predsednik","presidentea","presidente","chairman","chair",
      "présidente","Präsident","President", "Preşedinte"
      )))

    vicepresident <<- tolower(unique(c(
      "vice-président","вицепрезидент","dopredsjednik","Başkan Vekili","víceprezident","vicepræsident",
      "asepresident","varapresidentti","Vizepräsident","αντιπρόεδρος","alelnök","Leasuachtarán",
      "vicepresidente","viceprezidents","viceprezidentas","Vizepresident","Viċi President","onderdirecteur",
      "fise-presidint","wiceprezydent","vice-presidente","vice-preşedinte","podpredsedníčka","podpredsednik",
      "lehendakariordea","vicepresident","vice President","vice-president","vice-présidente" 
    )))


    president_of_the_commission <<- tolower(unique(c(
      "president de la commission","президент на комисията","predsjednik komisije",
      "komisyon başkanı","prezident de la Commission","formand for kommissionen",
      "komisjoni president","komission puheenjohtaja","président de la commission",
      "Präsident der Kommission","πρόεδρος της επιτροπής","bizottság elnöke",
      "Uachtarán an choimisiúin","presidente della commissione","komisijas prezidents",
      "komisijos pirmininkas","President vun der Kommissioun","voorzitter van de commissie",
      "foarsitter fan de kommisje","przewodniczący komisji","presidente da comissão",
      "preşedinte de la commission","prezident komisie","predsednik komisije",
      "batzordeko presidentea","president de la comissió","presidente da comisión",
      "presidente de la comisión","ordförande för kommissionen","chairman of the commission",
      "president of the commission",
      "председател на комисията","predsjednik odbora","komite başkanı",
      "předseda výboru","formand for udvalget","komisjoni esimees",
      "komitean puheenjohtaja","présidente de la commission","Vorsitzender des Ausschusses",
      "cathaoirleach an choiste","presidente del comitato","komitejas priekšsēdētājs",
      "komiteto pirmininkas","President vum Comité","president tal-kumitat",
      "preşedintele comitetului","predseda výboru","predsednik odbora",
      "batzordeko lehendakaria","president del comitè","presidente do comité",
      "presidente del comité","nämndens ordförande"    
    )))

    president_of_the_committee <<- tolower(unique(c(
      "komite başkanı","předseda výboru","formand for udvalget",
      "komisjoni esimees","komitean puheenjohtaja","président du comité",
      "vorsitzender des ausschusses","πρόεδρος της επιτροπής","a bizottság elnöke",
      "cathaoirleach an choiste","presidente del comitato","komitejas priekšsēdētājs",
      "komiteto pirmininkas","president vum comité","president tal-kumitat",
      "voorzitter van de commissie","foarsitter fan de kommisje","przewodniczący komitetu",
      "presidente do comitê","președinte al comitetului","predseda výboru",
      "predsednik odbora","batzordeburua","president del comitè",
      "presidente do comité","presidente del comité","utskottets ordförande",
      "chair of the committee","president of the committee", "председател на комисията","predsjednik odbora",
      "komite başkanı","předseda výboru","formand for udvalget",
      "présidente du comité","πρόεδρος της επιτροπής","bizottság elnöke",
      "cathaoirleach an choiste","presidente del comitato","komitejas priekšsēdētājs",
      "komiteto pirmininkas","president tal-kumitat","voorzitter van de commissie",
      "foarsitter fan de kommisje","przewodniczący komisji","presidente da comissão",
      "preşedintele comitetului","predsednik odbora","batzordeko burua",
      "president de la comissió","presidente do comité","presidente del comité",
      "ordförande i kommittén"
    )))

    presidency_of_the_hon <<- tolower(unique(c(
      "presidency of the Hon","председателството на Hon","predsjedništvo č","Cumhurbaşkanlığı","předsednictví Hon",
      "præsidentskab for Hon","presidentuuri au","kunniapuheenjohtaja","présidence de l'honorable","Präsidentschaft des Hon",
      "προεδρία του Ον","elnöksége a Hon","uachtaránacht an Oinigh","presidenza dell'On","prezidentūra god",
      "prezidentu gerb","Présidence vum Hon","presidenza tal-Onor","voorzitterschap van de Hon","presidintskip fan de Hon",
      "prezydentura Hon","presidência do Exmo","președinția Onorului","predsedníctvo Hon","predsedstvo Hon",
      "presidentetza Hon","presidència de l'Excm","presidencia do Excmo","presidencia del Excmo","ordförandeskapet för Hon"
    )))

    presidency <<- tolower(unique(c(
      "presidency","президентство","predsjedništvo","başkanlık","předsednictví","formandskab","eesistumine","puheenjohtajuus","présidence",
      "Präsidentschaft","προεδρία","elnökség","uachtaránacht","presidenza","prezidentūra","prezidentūra","Présidence","presidenza", "vorsitz",
      "voorzitterschap","presidintskip","przewodnictwo","presidência","preşedinţie","predsedníctvo","predsedstvo","lehendakaritza","presidència",
      "presidencia","presidencia","ordförandeskap",
      "presidency of","председателство на","predsjedništvo od","başkanlığı","předsednictví",
      "formandskab for","aasta presidentuur","puheenjohtajakausi","présidence de","Präsidentschaft von",
      "προεδρία του","elnöksége","uachtaránacht na","presidenza di","gada prezidentūra",
      "pirmininkavimas","Présidence vun","presidenza ta","voorzitterschap van","presidintskip fan",
      "prezydencja","presidência de","preşedinţia lui","predsedníctvo","predsedovanje",
      "ren presidentetza","presidència de","presidencia de","presidencia de","ordförandeskapet för",
      "presidency of mrs","председателство на г-жа","predsjedništvo gđe","başkanlığı hanım",
      "předsednictví mrs","formandskab for mrs","presidendiks pr","rouvan puheenjohtajakausi",
      "présidence de mme","präsidentschaft von mrs","προεδρία της κας","elnöksége mrs",
      "uachtaránacht mrs","presidenza della sig","kundzes prezidentūra","prezidentūra p",
      "présidence vun mme","presidenza tas-sinjura","voorzitterschap van mevr","presidintskip fan mrs",
      "prezydentura p","presidência da sra","președinția doamnei","predsedníctvo p",
      "predsedovanje ge","andrearen presidentetza","presidència de la sra","presidencia da sra",
      "presidencia de la sra","presidentskapet för mrs",
      "chair of mr","presidency of mr","председател на mr","stolica gosp","başkanı sn",
      "křeslo mr","formand for mr","juhataja hr","puheenjohtaja mr",
      "présidence de monsieur","vorsitzende von hr","πρόεδρος του κ","elnök úr",
      "cathaoirleach mr","presidente del sig","priekšsēdētājs mr","pirmininkas p",
      "president vum mr","president tas-sur","stoel van dhr","foarsitter fan mr",
      "krzesło p","cadeira do sr","scaunul dlui","predseda p",
      "predsednik g","jaunaren burua","president del sr","presidente do sr",
      "silla del sr","ordförande för mr"   
    )))

    dignitary <- c(
      "chancellor", "president of", "minister", "his\\sholiness", "secretary", "king", "queen"
    )

    type_of_speakers <<- c("rapporteur", "reporter", "auteur", "author", "member of")

    
    if (!exists("scriptname")) scriptname <<- "l_eu_parliament_plenary"

    # valid options for this script are
    #    log_output = c("file","console","hub")
    #    loading_method = c("date_range", "start_date", "end_date") | "all" | c("parliament_num", parl_num) | c("year", year) | "all"
    #    translate = "TRUE" | "FALSE"
    #    refresh_data = "TRUE" | "FALSE"
    #    
    #    you can use log_output = c("console") to debug your script if you want
    #    but set it to c("file") before putting in automated containerized production

    opt <<- list(
      backend = "hub",
      schema = "beta_pipelinev1_202303",
      log_output = c("console"),
      method = c("date_range", "2018-02-28", "2018-02-28"),
      refresh_data = TRUE,
      translate = TRUE
    )

    if (!exists("opt")) {
      opt <<- clessnverse::process_command_line_options()
    }

    if (!exists("logger") || is.null(logger)) {
      logger <<- clessnverse::log_init(scriptname, opt$log_output, Sys.getenv("LOG_PATH"))
    }
    
    # login to hublot
    clessnverse::logit(scriptname, "connecting to hub", logger)

    # connect to hublot
    credentials <<- hublot::get_credentials(
      Sys.getenv("HUB3_URL"), 
      Sys.getenv("HUB3_USERNAME"), 
      Sys.getenv("HUB3_PASSWORD"))
    
    # or connect to hub2
    clessnhub::login(
       Sys.getenv("HUB_USERNAME"),
       Sys.getenv("HUB_PASSWORD"),
       Sys.getenv("HUB_URL"))

    df_people <- clessnverse::get_warehouse_table(
      table_name = 'people', 
      data_filter=list(
        data__union = "EU",
        data__institution = "European Parliament"
      ),
      credentials = credentials,
      nbrows = 0
    )

    df_country <- clessnverse::get_warehouse_table(
      table_name = "countries",
      data_filter = list(),
      credentials = credentials,
      nbrows = 0
    )

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"starting"), logger)
    
    main()
  },

  warning = function(w) {
    print(w) 
    if (final_message == "") {
      final_message <<- w$message
     } else {
      paste(final_message, "\n", w$message, sep="")
     }
     
    status <<- 2
  }),
    
  error = function(e) {
    print(e)
    if (final_message == "") {
      final_message <<- e$message
     } else {
      paste(final_message, "\n", e$message, sep="")
     }

    status <<- 1
  },

  finally={
    clessnverse::logit(scriptname, final_message, logger)

    clessnverse::logit(scriptname, 
      paste(
          nb_warehouse_items, 
          "warehouse items were found",
          nb_debates,
          "debates were loaded in the datamart, totalling",
          nb_interventions
      ),
      logger
    )

    clessnverse::logit(scriptname, paste("Execution of",  scriptname,"program terminated"), logger)
    clessnverse::log_close(logger)
    if (exists("logger")) rm(logger)
    print(paste("exiting with status", status))
    #quit(status = status)
  }
)