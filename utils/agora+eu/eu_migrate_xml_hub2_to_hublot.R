# connect to hublot
credentials <<- hublot::get_credentials(
  Sys.getenv("HUB3_URL"), 
  Sys.getenv("HUB3_USERNAME"), 
  Sys.getenv("HUB3_PASSWORD"))

# connect to hub2
clessnhub::login(
   Sys.getenv("HUB_USERNAME"),
   Sys.getenv("HUB_PASSWORD"),
   Sys.getenv("HUB_URL"))


# create filter
filter <- clessnhub::create_filter(type="parliament_debate", schema="v2", metadata=list(location="EU", format="xml"), data=NULL)
# Now get the data
df <- clessnhub::get_items(table = 'agoraplus_interventions', filter = filter, download_data = TRUE)

for (i in 1:nrow(df)) {
  #grab a row from hub2
  source <- df[i,]

  #map fields from source to destination in a new row (list)
  row <- list(
    .url =  source$metadata.url,
    .schema =  "202303",
    event_id =  source$data.eventID,
    event_date =  source$data.eventDate,
    event_title =  source$data.eventTitle,
    speaker_type =  source$data.speakerType,
    speaker_party =  source$data.speakerParty,
    .lake_item_key =  NA,
    event_end_time =  source$data.eventEndTime,
    president_name =  NA,
    speaker_gender =  source$data.speakerGender,
    .lake_item_path =  NA,
    intervention_id =  source$data.interventionID,
    speaker_country =  source$data.speakerCountry,
    event_start_time =  source$data.eventStartTime,
    speaker_polgroup =  source$data.speakerPolGroup,
    .lake_item_format =  "xml",
    intervention_lang =  tolower(source$data.interventionLang),
    intervention_text =  source$data.interventionText,
    speaker_full_name =  source$data.speakerFullName,
    intervention_header =  NA,
    intervention_seq_num =  source$data.interventionSeqNum,
    intervention_text_en =  source$data.interventionTextEN,
    intervention_header_en =  NA,
    subject_of_business_id =  source$data.subjectOfBusinessID,
    speaker_full_name_native =  NA,
    subject_of_business_title =  source$data.subjectOfBusinessTitle
  )
}