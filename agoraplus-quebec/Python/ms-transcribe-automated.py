import pytube
import dropbox
import pydub
import azure.cognitiveservices.speech as speechsdk
import requests
import datetime
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
    mp4_audio.export("youtubeAudio.wav", format = "wav",parameters=["-ar", "16000", "-filter:a", "atempo=0.75"])
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
    elif (languageDetection == "fr"):
        speech_recognizer = speechsdk.SpeechRecognizer(speech_config=speech_config, audio_config=audio_input, \
            language="fr-CA")
    elif (languageDetection == "en"):
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

def list_folder(dbx, folder, subfolder):
    """List a folder.
    Return a dict mapping unicode filenames to
    FileMetadata|FolderMetadata entries.
    """
    if (folder == "" and subfolder == ""):
        path=''
    else:
        path = '/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'))
        while '//' in path:
            path = path.replace('//', '/')
        path = path.rstrip('/')
    try:
        res = dbx.files_list_folder(path)
    except dropbox.exceptions.ApiError as err:
        print('Folder listing failed for', path, '-- assumed empty:', err)
        return {}
    else:
        rv = {}
        file_list = []
        for entry in res.entries:
            rv[entry.name] = entry
        for file in rv:
            file_list.append(file)
        return file_list

def upload(dbx, fullname, folder, subfolder, name, overwrite=False):
    """Upload a file.
    Return the request response, or None in case of error.
    """
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    while '//' in path:
        path = path.replace('//', '/')
    mode = (dropbox.files.WriteMode.overwrite
            if overwrite
            else dropbox.files.WriteMode.add)
    mtime = os.path.getmtime(fullname)
    with open(fullname, 'rb') as f:
        data = f.read()
    #with stopwatch('upload %d bytes' % len(data)):
    try:
        print('trying to upload')
        res = dbx.files_upload(
            data, path, mode,
            client_modified=datetime.datetime(*time.gmtime(mtime)[:6]),
            mute=True)
    except dropbox.exceptions.ApiError as err:
        print('*** API error', err)
        return None
    print('uploaded as', res.name.encode('utf8'))
    return res


def download(dbx, folder, subfolder, name):
    """Download a file.
    Return the bytes of the file, or None if it doesn't exist.
    """
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    while '//' in path:
        path = path.replace('//', '/')
    #with stopwatch('download'):
    try:
        md, res = dbx.files_download(path)
    except dropbox.exceptions.HttpError as err:
        print('*** HTTP error', err)
        return None
    data = res.content
    print(len(data), 'bytes; md:', md)
    return data


def main():
    select_publish_date_start = "2021-03-02"
    select_publish_date_end = "2021-03-02"

    TOKEN = '5C7ebqb9vcIAAAAAAAAAASJgLBAJLRjBwThwHLkuZIUmv7VgIdEXHS7A29x96JQc'
    dbx = dropbox.Dropbox(oauth2_access_token=TOKEN)


    home_path = ''
    base_path = '/'
    from_azure_file_path = home_path + base_path + 'from_azure/'
    parkinglot_file_path = home_path +  base_path + 'from_azure/parkinglot/'
    to_hub_file_path = home_path +  base_path + 'to_hub/'
    ready_to_hub_file_path = home_path +  base_path + 'to_hub/ready/'
    done_file_path = home_path +  base_path + 'to_hub/done/'

    p = get_play_list_from_youtube('https://www.youtube.com/playlist?list=PLdgoQ6C3ckQv0XCp8S9zSswMuiss8lvcC')

    file_list_from_azure = list_folder(dbx,base_path,from_azure_file_path)
    file_list_parkinglot = list_folder(dbx,base_path,parkinglot_file_path)
    file_list_to_hub = list_folder(dbx,base_path,to_hub_file_path)
    file_list_ready_to_hub = list_folder(dbx,base_path,ready_to_hub_file_path)
    file_list_done = list_folder(dbx,base_path,done_file_path)

    file_list_full = file_list_from_azure + file_list_parkinglot + file_list_to_hub + file_list_ready_to_hub + file_list_done
    
    i = 1

    for video in p.videos:
        print('\n')
        print(i)
        print(video.title)
        print(video.watch_url)

        if (video.publish_date.strftime("%Y-%m-%d") < select_publish_date_start or video.publish_date.strftime("%Y-%m-%d") > select_publish_date_end):
            print("not in wanted date range")
            continue
        # </if video not within wanted date range>

        #video_uuid = video.watch_url.replace('/','')
        #video_uuid = video_uuid.replace(':','')
        #video_uuid = video_uuid.replace('=','')
        #video_uuid = video_uuid.replace('?','')
        #video_uuid = video_uuid.replace('.','')
        video_uuid = video.watch_url.split("?v=")[1]
        print(video_uuid)

        language_list = ['fr','en']

        for lang in language_list:
            transctipt_file_path = from_azure_file_path
            #transcript_file_name = video.publish_date.strftime("%Y-%m-%d")+video_uuid+"---"+video.title+'.txt'
            transcript_file_name = video.publish_date.strftime("%Y-%m-%d")+"-"+lang+"-"+video_uuid+'.txt'
            transcript_full_name = transctipt_file_path+transcript_file_name
            transcript_blng_name = transctipt_file_path+video.publish_date.strftime("%Y-%m-%d")+"-"+video_uuid+'.txt'

            if ( {transcript_file_name, transcript_blng_name} & set(file_list_full) != set() ):
                print('Already transcribed')
            else:
                print('Downloading video')
                vid = get_video_from_youtube(video.watch_url)
                print('Transcribing in '+lang)
                transcribed_text = speech_continuous_recognition_with_file('youtubeAudio.wav', languageDetection=lang)
                if (transcribed_text):
                    f = open(transcript_file_name,"w+") 
                    f.write(' '.join([str(elem) for elem in transcribed_text]))
                    f.close()
                    upload(dbx, transcript_file_name, '', from_azure_file_path, transcript_file_name, False)
                    os.remove(transcript_file_name)

                os.remove('youtubeAudio.wav')
            # </if (video.title in file_list_full)>

            file_list_from_azure = list_folder(dbx,base_path,from_azure_file_path)

            print(transcript_full_name)

            if (transcript_file_name in file_list_from_azure): 
                print('found file')  
                download(dbx, '', from_azure_file_path, transcript_file_name)
                f = open(transcript_file_name,"r")
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
                    upload(dbx, transcript_file_name, '', from_azure_file_path, transcript_file_name, False)
                    os.remove(transcript_file_name)

                #</if (transcribed_text.split()[0] != 'URL'):>
                if ("ministre" in words_list[1:200]):
                    print("relevant")
                else:
                    print("irrelevant")
                    #if (os.path.exists(transcript_full_name)):
                    #    os.rename(transcript_full_name, parkinglot_file_path+transcript_file_name)
            #</if (transcript_file_name in file_list_from_azure):>               

        #</for lang in language_list>

        #if (i==10):
        #    break
        #</if (i==10)>

        i = i + 1
    #</for video in p.videos:>
#</main>


if __name__ == "__main__":
    main()

