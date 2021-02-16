from pytube import YouTube
from pytube import Playlist
from pydub import AudioSegment
from google.cloud import speech_v1 as googlespeech
from google.cloud import speech_v1p1beta1 as googlespeechbeta
import azure.cognitiveservices.speech as speechsdk
import requests
import time
import sys
import re
import io


""" Google  """
def transcribe_gcs(gcs_uri):
    """Asynchronously transcribes the audio file specified by the gcs_uri."""
    from google.cloud import speech
    from google.cloud import speech_v1p1beta1 as speechbeta

    client = speech.SpeechClient()

    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        sample_rate_hertz=8000,
        language_code="fr-CA",
        enable_automatic_punctuation=True,
    )

    operation = client.long_running_recognize(config=config, audio=audio)

    print("Waiting for operation to complete...")
    #response = operation.result(timeout=2000)

    while operation.running():
        time.sleep(10)
        sys.stdout.write(".")
        sys.stdout.flush()


    response = operation.result()
    
    full_transcript = ""
    # Each result is for a consecutive portion of the audio. Iterate through
    # them to get the transcripts for the entire audio file.
    for result in response.results:
        # The first alternative is the most likely one for this portion.
        print(u"Transcript: {}".format(result.alternatives[0].transcript))
        print("Confidence: {}".format(result.alternatives[0].confidence))
        full_transcript = full_transcript + " " + result.alternatives[0].transcript

    print(full_transcript)

    f = open("Google-test","w+") 
    f.write(full_transcript)
    f.close()

mp4_audio = AudioSegment.from_file("youtubeAudio.mp4", format="mp4")
mp4_audio.export("youtubeAudio.wav", format = "wav",parameters=["-ac", "1", "-ar", "16000", "-t", "50"])

transcribe_gcs('gs://agoraplus/youtubeAudio.wav')


####################################################################################################


""" Microsoft """
def GetVideoFromYoutube(video_url):
    # Convert video
    vid = YouTube(url=video_url)
    aud = vid.streams.filter(only_audio=True, subtype="mp4")
    aud.first().download(filename="youtubeAudio")
    mp4_audio = AudioSegment.from_file("youtubeAudio.mp4", format="mp4")
    mp4_audio.export("youtubeAudio.wav", format = "wav",parameters=["-ac", "2", "-ar", "16000"])
    return(vid)


def GetPlayListFromYouyube(url):
    p = Playlist(url)
    return(p)


def SpeechRecognitionWithFile(fileToTranscribe):
    """performs one-shot speech recognition with input from an audio file"""
    # <SpeechRecognitionWithFile>
    speech_config = speechsdk.SpeechConfig(
        subscription="b5421646c3b449a0856a089a67d84b2a", region="canadacentral",speech_recognition_language = "fr-CA")
    audio_input = speechsdk.AudioConfig(filename=fileToTranscribe)
    # Creates a speech recognizer using a file as audio input, also specify the speech language
    speech_recognizer = speechsdk.SpeechRecognizer(
        speech_config=speech_config, language="fr-CA", audio_config=audio_input)

    # Starts speech recognition, and returns after a single utterance is recognized. The end of a
    # single utterance is determined by listening for silence at the end or until a maximum of 15
    # seconds of audio is processed. It returns the recognition text as result.
    # Note: Since recognize_once() returns only a single utterance, it is suitable only for single
    # shot recognition like command or query.
    # For long-running multi-utterance recognition, use start_continuous_recognition() instead.
    result = speech_recognizer.recognize_once()

    # Check the result
    if result.reason == speechsdk.ResultReason.RecognizedSpeech:
        print("Recognized: {}".format(result.text))
    elif result.reason == speechsdk.ResultReason.NoMatch:
        print("No speech could be recognized: {}".format(result.no_match_details))
    elif result.reason == speechsdk.ResultReason.Canceled:
        cancellation_details = result.cancellation_details
        print("Speech Recognition canceled: {}".format(cancellation_details.reason))
        if cancellation_details.reason == speechsdk.CancellationReason.Error:
            print("Error details: {}".format(cancellation_details.error_details))
    return(result.text)
    # </SpeechRecognitionWithFile>



def SpeechContinuousRecognitionWithFile(fileToTranscribe):
    # <SpeechContinuousRecognitionWithFile>
    speech_config = speechsdk.SpeechConfig(subscription="b5421646c3b449a0856a089a67d84b2a", region="canadacentral")

    audio_input = speechsdk.AudioConfig(filename=fileToTranscribe)

    auto_detect_source_language_config = \
        speechsdk.languageconfig.AutoDetectSourceLanguageConfig(languages=["fr-CA", "en-US"])

    speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
        auto_detect_source_language_config=auto_detect_source_language_config)
    #speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
    #    language="fr-CA")

    done = False

    def stop_cb(evt):
        """callback that signals to stop continuous recognition upon receiving an event `evt`"""
        #print('CLOSING on {}'.format(evt))
        nonlocal done
        done = True

    all_results = []

    def handle_final_result(evt):
        nonlocal all_results
        all_results.append(evt.result.text)


    def increase_counter():
        sys.stdout.write(".")
        sys.stdout.flush()


    # Connect callbacks to the events fired by the speech recognizer
    #speech_recognizer.recognizing.connect(lambda evt: print('RECOGNIZING: {}'.format(evt)))
    speech_recognizer.recognizing.connect(lambda evt: increase_counter())
    #speech_recognizer.recognized.connect(lambda evt: print('RECOGNIZED: {}'.format(evt)))
    speech_recognizer.recognized.connect(handle_final_result)
    speech_recognizer.session_started.connect(lambda evt: print('\nSESSION STARTED: {}'.format(evt)))
    speech_recognizer.session_stopped.connect(lambda evt: print('\nSESSION STOPPED {}'.format(evt)))
    #speech_recognizer.canceled.connect(lambda evt: print('CANCELED {}'.format(evt)))
    # stop continuous recognition on either session stopped or canceled events
    speech_recognizer.session_stopped.connect(stop_cb)
    speech_recognizer.canceled.connect(stop_cb)

    # Start continuous speech recognition
    speech_recognizer.start_continuous_recognition()
    while not done:
        time.sleep(.5)

    speech_recognizer.stop_continuous_recognition()
    return(all_results)
    # </SpeechContinuousRecognitionWithFile>

p = GetPlayListFromYouyube('https://www.youtube.com/playlist?list=PLdgoQ6C3ckQv0XCp8S9zSswMuiss8lvcC')

#iterator = iter(p)
i = 1
for video in p.videos:
    if (video.title.find("COVID") != -1 and i == 3):
        print(video.title)
        vid = GetVideoFromYoutube(video.watch_url)
        #vid = GetVideoFromYoutube(next(iterator))
        transcribedText = SpeechContinuousRecognitionWithFile("youtubeAudio.wav")
        #transcribedText = SpeechRecognitionWithFile("youtubeAudio.wav")
        f = open("MS-"+vid.title,"w+") 
        f.write(' '.join([str(elem) for elem in transcribedText]))
        f.close()
        if (i==5):
            break
    i = i + 1



####################################################################################################

""" AWS """
def aws_transcription(uri):
    import boto3
    import urllib.request
    import json

    AWS_ACCESS_KEY_ID = 'AKIAIXXSISSQAWXWDQ6A'
    AWS_SECRET_ACCESS_KEY = '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'

    mp4_audio = AudioSegment.from_file("youtubeAudio.mp4", format="mp4")
    mp4_audio.export("youtubeAudio.wav", format = "wav",parameters=["-ac", "1", "-ar", "16000"])

    job_name = 'test1'
    job_uri = 'https://s3.amazonaws.com/agoraplus/youtubeAudio.wav'

    Transcribe = boto3.client('transcribe', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-east-2')

    Transcribe.start_transcription_job(TranscriptionJobName=job_name, 
        Media={'MediaFileUri': job_uri}, 
        MediaFormat='wav', 
        #IdentifyLanguage=True,
        LanguageCode='fr-CA',
        ShowSpeakerLabels = True,
        MaxSpeakerLabels = 3,
        )

    while True:
        status = Transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        print("Not ready yet...")
        time.sleep(2)
    print(status)

    if status['TranscriptionJob']['TranscriptionJobStatus'] == 'COMPLETED':
        response = urllib.request.urlopen(status['TranscriptionJob']['Transcript']['TranscriptFileUri'])
        data = json.loads(response.read())
        text = data['results']['transcripts'][0]['transcript']
        print(text)
        f = open("AWS-test","w+") 
        f.write(text)
        f.close()
        Transcribe.delete_transcription_job(TranscriptionJobName=job_name)
 
