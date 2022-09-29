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
import logging



def speech_continuous_recognition_with_file(fileToTranscribe, languageDetection):
    # <speech_continuous_recognition_with_file>
    azure_key = os.environ.get('AZURE_SPEECH2TEXT_KEY')

    if (azure_key is None):
        logging.info('no azure subscription found in env var')
        sys.exit('no azure subscription found in env var') 

    speech_config = speechsdk.SpeechConfig(subscription=azure_key, region="canadacentral")
    
    audio_input = speechsdk.AudioConfig(filename=fileToTranscribe)

    if (languageDetection == "bi"):
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
    speech_recognizer.session_started.connect(lambda evt: logging.info('\nSESSION STARTED: {}'.format(evt)))
    speech_recognizer.session_stopped.connect(lambda evt: logging.info('\nSESSION STOPPED {}'.format(evt)))
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
    logging.info("Listing folder " + folder + subfolder)
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
        logging.info('Folder listing failed for', path, '-- assumed empty:', err)
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
    logging.info("Uploading " + folder + subfolder + name)
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
        logging.info('trying to upload')
        res = dbx.files_upload(
            data, path, mode,
            client_modified=datetime.datetime(*time.gmtime(mtime)[:6]),
            mute=True)
    except dropbox.exceptions.ApiError as err:
        logging.info('*** API error', err)
        return None
    #print('uploaded as', res.name.encode('utf8'))
    return res

def download(dbx, folder, subfolder, name):
    """Download a file.
    Return the bytes of the file, or None if it doesn't exist.
    """
    logging.info("Downloading " + folder + subfolder + name)
    path = '/%s/%s/%s' % (folder, subfolder.replace(os.path.sep, '/'), name)
    while '//' in path:
        path = path.replace('//', '/')
    #with stopwatch('download'):
    try:
        md, res = dbx.files_download(path)
    except dropbox.exceptions.HttpError as err:
        logging.info('*** HTTP error', err)
        return None
    data = res.content
    #print(len(data), 'bytes; md:', md)
    return data



def main(argv):
    print(argv[1])

    logger = logging.getLogger()
    
    if (not logging.getLogger().hasHandlers()):
        scriptname = 'transcribe_wav_audio.py'
        filename = os.environ.get('LOG_PATH') + '/' + scriptname + '.log'
        if (filename[0] == '~'):
            filename = filename[1:len(filename)]
            filename = os.environ.get('HOME') + filename

        
        print('logging to ' + filename)
        logging.basicConfig(level=logging.INFO, filename=filename, filemode='w+', format='%(asctime)s : %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
        logging.info('logging to ' + filename)
    

    logging.info("==============================================")
    logging.info("starting youtube video extractor python script")

    logging.info("extracting youtube video at " + argv[1])

    TOKEN = os.environ.get('DROPBOX_TOKEN')
    dbx = dropbox.Dropbox(oauth2_access_token=TOKEN)

    home_path = '/'
    base_path = 'clessn-blend/_SharedFolder_clessn-blend/'
    from_azure_file_path = 'from_azure/'
    parkinglot_file_path = 'from_azure/parkinglot/'
    to_hub_file_path = 'to_hub/'
    ready_to_hub_file_path = 'to_hub/ready/'
    done_file_path = 'to_hub/done/'


    file_list_from_azure = list_folder(dbx,base_path,from_azure_file_path)
    file_list_parkinglot = list_folder(dbx,base_path,parkinglot_file_path)
    file_list_to_hub = list_folder(dbx,base_path,to_hub_file_path)
    file_list_ready_to_hub = list_folder(dbx,base_path,ready_to_hub_file_path)
    file_list_done = list_folder(dbx,base_path,done_file_path)

    file_list_full = file_list_from_azure + file_list_parkinglot + file_list_to_hub + file_list_ready_to_hub + file_list_done

    logging.info(argv[1])

    audio_file = argv[1]
    audio_file = "/Users/patrick/Dev/CLESSN/clessn-blend/scrapers/pressconferences-crush/tlmep.wav"

    #language_list = ['fr','en']
    language_list = ['fr']

    for lang in language_list:
        timestamp = time.strftime("%Y-%m-%d", time.strptime(time.ctime(os.path.getctime(audio_file))))
        transctipt_file_path = from_azure_file_path
        transcript_file_name = audio_file+timestamp+"-"+lang+"-"+'.txt'
        transcript_blng_name = transctipt_file_path+timestamp+"-"+"bi"+"-"+'.txt'
        transcript_neut_name = transctipt_file_path+timestamp+"-"+'.txt'
        transcript_full_name = transctipt_file_path+transcript_file_name
            
        transcribed_text = speech_continuous_recognition_with_file(argv[1], languageDetection=lang)

        if (transcribed_text):
            f = open(transcript_file_name,"w+") 
            f.write(' '.join([str(elem) for elem in transcribed_text]))
            f.close()
            upload(dbx, transcript_file_name, '', base_path+from_azure_file_path, transcript_file_name, True)
            os.remove(transcript_file_name)

            #os.remove('youtubeAudio.wav')
        # </if (video.title in file_list_full)>

        file_list_from_azure = list_folder(dbx,base_path,from_azure_file_path)

        logging.info(transcript_full_name)

        if (transcript_file_name in file_list_from_azure): 
            logging.info('found file')  
            logging.info(from_azure_file_path)
            logging.info(transcript_file_name)
            transcribed_text = download(dbx, '', base_path+from_azure_file_path, transcript_file_name)
            transcribed_text = transcribed_text.decode('UTF-8')
            #f = open(transcript_file_name,"r")
            #transcribed_text = f.read()
            lower_transcribed_text = transcribed_text.lower()
            words_list = lower_transcribed_text.split()
            #f.close()

            if (transcribed_text.split()[0] != 'DATE'):
                f = open(transcript_file_name,"w+")
                transcribed_text = 'PATH  : ' + transcript_full_name + '\n\n' + transcribed_text
                transcribed_text = 'URL   : ' + argv[1] + '\n' + transcribed_text
                #transcribed_text = 'TITLE : ' + video.title + '\n' + transcribed_text
                transcribed_text = 'DATE  : ' + video.publish_date.strftime("%Y-%m-%d") + '\n' + transcribed_text
                f.write(transcribed_text)
                f.close()
                upload(dbx, transcript_file_name, '', base_path+from_azure_file_path, transcript_file_name, True)
                os.remove(transcript_file_name)
            #</if (transcribed_text.split()[0] != 'DATE'):>
        #</if (transcript_file_name in file_list_from_azure):>               

    #</for lang in language_list>

    logging.shutdown()

if __name__ == "__main__":
    main(sys.argv)