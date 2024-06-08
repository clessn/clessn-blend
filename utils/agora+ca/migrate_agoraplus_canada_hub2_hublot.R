library(dplyr, warn.conflicts = FALSE)

credentials <- hublot::get_credentials(
  hub_url = Sys.getenv('HUB3_URL'),
  username = Sys.getenv('HUB3_USERNAME'),
  password = Sys.getenv('HUB3_PASSWORD')
)

df <- df %>% arrange(data.eventID, data.interventionSeqNum)

for (i in 5001:nrow(df)) {

  logger::log_info(paste(i, df$type[i], df$schema[i], df$metadata.location[i]))

  if (df$metadata.location[i] == "CA") {

    key <- df$key[i]

    table_name <- "agoraplus_canada_house_of_commons"
    logger::log_info(paste("processing agoraplus_canada_house_of_commons record #", i, "with key", key))

    row <- list(
      .url = df$metadata.url[i],
      .schema = df$schema[i],
      .lake_item_key = "migrated from hub2",
      .lake_item_path = "migrated from hub2",
      .lake_item_format = df$metadata.format[i],
      event_id = df$data.eventID[i],
      event_date = df$data.eventDate[i],
      event_start_time = df$data.eventStartTime[i],
      event_end_time = df$data.eventEndTime[i],
      event_title = df$data.eventTitle[i],
      event_subtitle = df$data.eventSubTitle[i],
      speaker_id = df$data.speakerID[i],
      speaker_type = df$data.speakerType[i],
      speaker_party = df$data.speakerParty[i],
      speaker_gender = df$data.speakerGender[i],
      speaker_country = df$data.speakerCountry[i],
      speaker_district = df$data.speakerDistrict[i],
      speaker_full_name = df$data.speakerFullName[i],
      speaker_is_minister = df$data.speakerIsMinister[i],
      intervention_id = df$data.interventionID[i],
      intervention_lang = df$data.interventionLang[i],
      intervention_text = df$data.interventionText[i],
      intervention_type = df$data.interventionType[i],
      intervention_doc_id = df$data.interventionDocID[i],
      intervention_doc_title = df$data.interventionDocTitle[i],
      intervention_seq_num = df$data.interventionSeqNum[i],
      intervention_text_en = df$data.interventionTextEN[i],
      intervention_text_fr = df$data.interventionTextFR[i],
      subject_of_business_id = df$data.subjectOfBusinessID[i],
      subject_of_business_seq_num = df$data.subjectOfBusinessSeqNum[i],
      subject_of_business_title = df$data.subjectOfBusinessTitle[i],
      subject_of_business_header = df$data.subjectOfBusinessHeader[i],
      subject_of_business_procedural_text = df$data.subjectOfBusinessProceduralText[i],
      subject_of_business_tabled_doc_id = df$data.subjectOfBusinessTabledDocID[i],
      subject_of_business_tabled_doc_title = df$data.subjectOfBusinessTabledDocTitle[i],
      subject_of_business_adopted_doc_id = df$data.subjectOfBusinessAdoptedDocID[i],
      subject_of_business_adopted_doc_title = df$data.subjectOfBusinessAdoptedDocTitle[i],
      object_of_business_id = df$data.objectOfBusinessID[i],
      object_of_business_seq_num = df$data.objectOfBusinessSeqNum[i],
      object_of_business_title = df$data.objectOfBusinessTitle[i],
      object_of_business_rubric = df$data.objectOfBusinessRubric[i]
    )

    row <- lapply(row, function(x) 
        {x[is.null(x)] <- NA;x})

    attempt <- 1
    write_success <- list()

    while (attempt < 20 && typeof(write_success) == "list") {
      write_success <- tryCatch(
        expr = {
          logger::log_info(paste("writing record #", i, "writing key", key))
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

      if (is.null(write_success)) write_success <- list()

      if (typeof(write_success) == "list" && length(write_success) > 0) {
        logger::log_info("sleeping 20 seconds...")
        attempt <- attempt + 1
        Sys.sleep(20)
      }
    }
  } #if (metadata$location == "CA")
}
