import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _save_data(filename, data): 
    ''' method for save the  data with no missing value in a csv file in folder:  "/csv_files"
    no return '''
    logger.info('generating  clean files for save ')
    filename=os.path.split(filename) # divide the filename to get the only the name {eluniversal|elpais}
    clean_name = filename[1].split( '.')[0]
    newFileName = '../csv_files/clean_{}.csv'.format(clean_name)
    
    with open(newFileName,  mode='w+', encoding='utf-8') as f: 
        data.to_csv(f, sep="|")#add attr  index=False if we dont want the index saved