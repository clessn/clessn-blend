import pytube
import pydub
import azure.cognitiveservices.speech as speechsdk
import requests
import time
import sys
import re
import io
import os
import unicodedata



def get_video_from_youtube(video_url):
    # Convert video
    vid = pytube.YouTube(url=video_url)
    aud = vid.streams.filter(only_audio=True, subtype="mp4")
    aud.first().download(filename="youtubeAudio")
    mp4_audio = pydub.AudioSegment.from_file("youtubeAudio.mp4", format="mp4")
    mp4_audio.export("youtubeAudio.wav", format = "wav",parameters=["-ar", "16000"])
    os.remove('youtubeAudio.mp4')
    return(vid)


def get_play_list_from_youtube(url):
    p = pytube.Playlist(url)
    return(p)



def speech_continuous_recognition_with_file(fileToTranscribe, languageDetection):
    # <speech_continuous_recognition_with_file>
    speech_config = speechsdk.SpeechConfig(subscription="b5421646c3b449a0856a089a67d84b2a", region="canadacentral")
    
    audio_input = speechsdk.AudioConfig(filename=fileToTranscribe)

    if (languageDetection == "bilingual"):
        auto_detect_source_language_config = \
            speechsdk.languageconfig.AutoDetectSourceLanguageConfig(languages=["fr-CA", "en-CA"])

        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
            auto_detect_source_language_config=auto_detect_source_language_config)
    elif (languageDetection == "french"):
        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
            language="fr-CA")
    elif (languageDetection == "english"):
        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
            language="en-CA")
    else:
        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
            language="fr-CA")

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
    # </speech_continuous_recognition_with_file>



def main():

    home_path = '/Users/patrick/'
    from_azure_file_path = home_path + 'Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/video_pipeline/from_azure/'
    parkinglot_file_path = home_path + 'Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/video_pipeline/from_azure/parkinglot/'
    to_hub_file_path = home_path + 'Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/video_pipeline/to_hub/'
    ready_to_hub_file_path = home_path + 'Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/video_pipeline/to_hub/ready/'
    done_file_path = home_path + 'Dropbox/quorum-agoraplus-graphiques/_SharedFolder_quorum-agoraplus-graphiques/video_pipeline/to_hub/done/'

    p = get_play_list_from_youtube('https://www.youtube.com/playlist?list=PLdgoQ6C3ckQv0XCp8S9zSswMuiss8lvcC')

    file_list_from_azure = [unicodedata.normalize('NFC', f) for f in os.listdir(from_azure_file_path)]
    file_list_parkinglot = [unicodedata.normalize('NFC', f) for f in os.listdir(parkinglot_file_path)]
    file_list_to_hub = [unicodedata.normalize('NFC', f) for f in os.listdir(to_hub_file_path)]
    file_list_ready_to_hub = [unicodedata.normalize('NFC', f) for f in os.listdir(ready_to_hub_file_path)]
    file_list_done = [unicodedata.normalize('NFC', f) for f in os.listdir(done_file_path)]

    file_list_full = file_list_from_azure + file_list_parkinglot + file_list_to_hub + file_list_ready_to_hub + file_list_done

    i = 1

    for video in p.videos:
        #if (video.title.find("COVID") != -1):
            print('\n')
            print(i)
            print(video.title)
            print(video.watch_url)
            
            video_uuid = video.watch_url.replace('/','')
            video_uuid = video_uuid.replace(':','')
            video_uuid = video_uuid.replace('=','')
            video_uuid = video_uuid.replace('?','')
            video_uuid = video_uuid.replace('.','')

            transctipt_file_path = from_azure_file_path
            transcript_file_name = video.publish_date.strftime("%Y-%m-%d")+video_uuid+"---"+video.title
            transcript_full_name = transctipt_file_path+transcript_file_name

            if (transcript_file_name in file_list_full):
                print('Already transcribed')
            else:
                print('Downloading video')
                vid = get_video_from_youtube(video.watch_url)
                print('Transcribing')
                transcribed_text = speech_continuous_recognition_with_file('youtubeAudio.wav', languageDetection='french')
                if (transcribed_text):
                    f = open(transcript_full_name,"w+") 
                    f.write(' '.join([str(elem) for elem in transcribed_text]))
                    f.close()
                os.remove('youtubeAudio.wav')
            # </if (video.title in file_list_full)>

            file_list_from_azure = [unicodedata.normalize('NFC', f) for f in os.listdir(from_azure_file_path)]

            print(transcript_file_name)

            if (transcript_file_name in file_list_from_azure): 
                print('found file')  
                f = open(transcript_full_name,"r")
                transcribed_text = f.read()
                lower_transcribed_text = transcribed_text.lower()
                words_list = lower_transcribed_text.split()
                f.close()

                if (transcribed_text.split()[0] != 'DATE'):
                    f = open(transcript_full_name,"w+")
                    transcribed_text = 'PATH  : ' + transcript_full_name + '\n\n' + transcribed_text
                    transcribed_text = 'URL   : ' + video.watch_url + '\n' + transcribed_text
                    transcribed_text = 'TITLE : ' + video.title + '\n' + transcribed_text
                    transcribed_text = 'DATE  : ' + video.publish_date.strftime("%Y-%m-%d") + '\n' + transcribed_text
                    f.write(transcribed_text)
                    f.close()
                #</if (transcribed_text.split()[0] != 'URL'):>
                
                if ("ministre" in words_list[1:200]):
                    print("relevant")
                else:
                    print("irrelevant")
                    if (os.path.exists(transcript_full_name)):
                        os.rename(transcript_full_name, parkinglot_file_path+transcript_file_name)
            #</if (transcript_file_name in file_list_from_azure):>

            if (i==10):
                break
            #</if (i==10)>

            i = i + 1
        #</if (video.title.find("COVID") != -1):>
    #</for video in p.videos:>
#</main>


if __name__ == "__main__":
    main()

