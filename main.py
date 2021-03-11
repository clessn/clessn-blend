import os
import logging
import sys

sys.path.append("./agoraplus-quebec/python")

import agoraplusmstranscribeautomated as pyscraper

print("Running python script")
print('------')

try: 
    scriptname = 'agoraplusmstranscribeautomated.py'
    filename = os.environ.get('LOG_PATH') + '/' + scriptname + '.log'
    print('logging to ' + filename)
    logging.basicConfig(level=logging.INFO, filename=filename, filemode='w', format='%(asctime)s : %(levelname)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S')
    logging.info('logging to ' + filename)
    pyscraper.main()
 
except Exception as e: 
    print("An error occured : " + str(e))
    logging.info("An error occured : " + str(e))

else:
    print("Python script finished successfuly")
    logging.info("Python script finished successfuly")

finally:
    print("finally done")
    logging.info("finally done")