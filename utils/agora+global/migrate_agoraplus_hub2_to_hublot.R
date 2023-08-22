#install.packages('RPostgreSQL')

file.remove("R.log")
log_file <- "R.log"
logger::log_appender(logger::appender_tee(log_file),index=1)

credentials <- hublot::get_credentials(
  hub_url = Sys.getenv('HUB3_URL'),
  username = Sys.getenv('HUB3_USERNAME'),
  password = Sys.getenv('HUB3_PASSWORD')
)

dsn_database = "hub"
dsn_hostname = "p2xco.de"  
dsn_port = "5432"                # Specify your port number. e.g. 98939
dsn_uid = "postgres"         # Specify your username. e.g. "admin"
dsn_pwd = "mypgdbpass"        # Specify your password. e.g. "xxx"

tryCatch({  
  drv <- RPostgreSQL::postgresqlInitDriver()
  logger::log_info("Connecting to Database…")
  connec <- RPostgreSQL::dbConnect(drv, 
                dbname = dsn_database,
                host = dsn_hostname, 
                port = dsn_port,
                user = dsn_uid, 
                password = dsn_pwd)
  logger::log_info("Database Connected!")
  },
  error=function(e) {
          logger::log_error("Unable to connect to Database.")
          logger::log_error(e$message)
  })

batch_size <- 100000
#s <- 2189376
s <- 1000001
s <- 2770000

while (s < 3000000) {
  df <- tryCatch(
    expr = {
      logger::log_info(paste("reading database from index", s))
      RPostgreSQL::dbGetQuery(
        connec, 
        paste(
          "SELECT * FROM public.hub_v2_data_agoraplusintervention
          ORDER BY key ASC LIMIT 1000000 OFFSET", s
        )
      )
    },
    error = function(e) {
      logger::log_error("Error while retrieving data")
      logger::log_error(e$message)
    }
  )


  for (i in 1:nrow(df)) {

    key <- df$key[i]
    type <- df$type[i]
    schema <- df$schema[i]
    data <- jsonlite::fromJSON(df$data[i])
    metadata <- jsonlite::fromJSON(df$metadata[i])

    logger::log_info(paste(s + i, type, schema, metadata$location))

    if (type == "press_conference") {
      table_name <- "agoraplus_qc_press_conferences"
      logger::log_info(paste("processing agoraplus_qc_press_conferences record #", s + i, "with key", key))

      row <- list(
        .url = metadata$url,
        .schema = schema,
        .lake_item_key = "migrated from hub2",
        .lake_item_path = "migrated from hub2",
        .lake_item_format = metadata$format,
        event_id = data$eventID,
        event_date = data$eventDate,
        event_start_time = data$eventStartTime,
        event_end_time = data$eventEndTime,
        event_title = data$eventTitle,
        event_subtitle = data$eventSubTitle,
        speaker_type = data$speakerType,
        speaker_party = data$speakerParty,
        speaker_gender = data$speakerGender,
        speaker_country = data$speakerCountry,
        speaker_district = data$speakerDistrict,
        speaker_full_name = data$speakerFullName,
        speaker_is_minister = data$speakerIsMinister,
        intervention_id = gsub("-", "", data$interventionID),
        intervention_lang = data$interventionLang,
        intervention_text = data$interventionText,
        intervention_type = data$interventionType,
        intervention_seq_num = data$interventionSeqNum
      )

      row <- lapply(row, function(x) 
          {x[is.null(x)] <- NA;x})

      attempt <- 1
      success <- 0

      while (attempt < 20 && !success) {
        success <- tryCatch(
          expr = {
            logger::log_info(paste("writing record #", s + i, "writing key", key))
            clessnverse::commit_warehouse_row(
              table = table_name,
              key <- key,
              row = row,
              refresh_data = TRUE,
              credentials = credentials
            )
          },
          error = function(e) {
            logger::log_error("could not write item")
            logger::log_error(e$message)
          }
        )

        if (is.null(success)) success <- 0

	      if (!success) {
          logger::log_info("sleeping 20 seconds...")
          attempt <- attempt + 1
          Sys.sleep(20)
        }
      }
    } #if (type == "press_conference")

    if (metadata$location == "CA") {
      table_name <- "agoraplus_canada_house_of_commons"
      logger::log_info(paste("processing agoraplus_canada_house_of_commons record #", s + i, "with key", key))

      row <- list(
        .url = metadata$url,
        .schema = schema,
        .lake_item_key = "migrated from hub2",
        .lake_item_path = "migrated from hub2",
        .lake_item_format = metadata$format,
        event_id = data$eventID,
        event_date = data$eventDate,
        event_start_time = data$eventStartTime,
        event_end_time = data$eventEndTime,
        event_title = data$eventTitle,
        event_subtitle = data$eventSubTitle,
        speaker_id = data$speakerID,
        speaker_type = data$speakerType,
        speaker_party = data$speakerParty,
        speaker_gender = data$speakerGender,
        speaker_country = data$speakerCountry,
        speaker_district = data$speakerDistrict,
        speaker_full_name = data$speakerFullName,
        speaker_is_minister = data$speakerIsMinister,
        intervention_id = data$interventionID,
        intervention_lang = data$interventionLang,
        intervention_text = data$interventionText,
        intervention_type = data$interventionType,
        intervention_doc_id = data$interventionDocID,
        intervention_doc_title = data$interventionDocTitle,
        intervention_seq_num = data$interventionSeqNum,
        intervention_text_en = data$interventionTextEN,
        intervention_text_fr = data$interventionTextFR,
        subject_of_business_id = data$subjectOfBusinessID,
        subject_of_business_seq_num = data$subjectOfBusinessSeqNum,
        subject_of_business_title = data$subjectOfBusinessTitle,
        subject_of_business_header = data$subjectOfBusinessHeader,
        subject_of_business_procedural_text = data$subjectOfBusinessProceduralText,
        subject_of_business_tabled_doc_id = data$subjectOfBusinessTabledDocID,
        subject_of_business_tabled_doc_title = data$subjectOfBusinessTabledDocTitle,
        subject_of_business_adopted_doc_id = data$subjectOfBusinessAdoptedDocID,
        subject_of_business_adopted_doc_title = data$subjectOfBusinessAdoptedDocTitle,
        object_of_business_id = data$objectOfBusinessID,
        object_of_business_seq_num = data$objectOfBusinessSeqNum,
        object_of_business_title = data$objectOfBusinessTitle,
        object_of_business_rubric = data$objectOfBusinessRubric
      )

      row <- lapply(row, function(x) 
          {x[is.null(x)] <- NA;x})

      attempt <- 1
      success <- 0

      while (attempt < 20 && !success) {
        success <- tryCatch(
          expr = {
            logger::log_info(paste("writing record #", s + i, "writing key", key))
            clessnverse::commit_warehouse_row(
              table = table_name,
              key <- key,
              row = row,
              refresh_data = TRUE,
              credentials = credentials
            )
          },
          error = function(e) {
            logger::log_error("could not write item")
            logger::log_error(e$message)
          }
        )

        if (is.null(success)) success <- 0

	      if (!success) {
          logger::log_info("sleeping 20 seconds...")
          attempt <- attempt + 1
          Sys.sleep(20)
        }
      }
    } #if (metadata$location == "CA")
    
    if (type == "parliament_debate" && metadata$location == "CA-QC") {
      if (schema == "vintage") {
        table_name <- "agoraplus_qcvintage_national_assembly"
        logger::log_info(paste("processing agoraplus_qcvintage_national_assembly record #", s + i, "with key", key))
      } else {
        table_name <- "agoraplus_quebec_national_assembly"
        logger::log_info(paste("processing agoraplus_quebec_national_assembly record #", s + i, "with key", key))
      }

      row <- list(
        .url = metadata$url,
        .schema = schema,
        .lake_item_key = "migrated from hub2",
        .lake_item_path = "migrated from hub2",
        .lake_item_format = metadata$format,
        event_id = data$eventID,
        event_date = data$eventDate,
        event_start_time = data$eventStartTime,
        event_end_time = data$eventEndTime,
        event_title = data$eventTitle,
        event_subtitle = data$eventSubTitle,
        speaker_type = data$speakerType,
        speaker_party = data$speakerParty,
        speaker_gender = data$speakerGender,
        speaker_country = data$speakerCountry,
        speaker_district = data$speakerDistrict,
        speaker_full_name = data$speakerFullName,
        speaker_is_minister = data$speakerIsMinister,
        intervention_id = data$interventionID,
        intervention_lang = data$interventionLang,
        intervention_text = data$interventionText,
        intervention_type = data$interventionType,
        intervention_seq_num = data$interventionSeqNum,
        subject_of_business_title = data$subjectOfBusinessTitle,
        object_of_business_title = data$objectOfBusinessTitle,
        object_of_business_rubric = data$objectOfBusinessRubric
      )

      row <- lapply(row, function(x) 
          {x[is.null(x)] <- NA;x})

      attempt <- 1
      success <- 0

      while (attempt < 20 && !success) {
        success <- tryCatch(
          expr = {
            logger::log_info(paste("writing record #", s + i, "writing key", key))
            clessnverse::commit_warehouse_row(
              table = table_name,
              key <- key,
              row = row,
              refresh_data = TRUE,
              credentials = credentials
            )
          },
          error = function(e) {
            logger::log_error("could not write item")
            logger::log_error(e$message)
          }
        )

        if (is.null(success)) success <- 0

        if (!success) {
          logger::log_info("sleeping 20 seconds...")
          attempt <- attempt + 1
          Sys.sleep(20)
        }
      }
    } #if (type == "parliament_debate" && metadata$location == "CA-QC")

  } #for (i in 1:nrow(df))

  s <- s + batch_size

}
