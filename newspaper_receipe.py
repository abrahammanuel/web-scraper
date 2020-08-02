import argparse 
import logging
import csv
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import os
import pandas as pd
import hashlib

logger = logging.getLogger(__name__)


def main(filename): 
    logger.info('Starting cleaning process')

    df = _read_data(filename)# 1
    newspaper_uid = _extract_newspaper_uid(filename)  # 2          
    df = _add_newspaper_uid_column(df,  newspaper_uid) # 3
    df = _extraer_host(df) # 4
    df = _fill_missing_titles(df)
    df = _generate_uids_for_row(df)
    df = _remove_new_lines_from(df)

    return df
    
def _read_data(filename): 
    logger.info('reading file: {}'.format(filename))

    return pd.read_csv(filename, delimiter="|")

def _extract_newspaper_uid(filename): 
    filename = os.path.split(filename)
    logging.info('extracting newspaper uid')                                
    newspaper_uid = filename[1].split('_')[0]

    logger.info('newspaper uid detected:  {}'.format(newspaper_uid))
    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('filling newspaper uid column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid

    return df

def _extraer_host(df):
    logger.info('extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df

def _fill_missing_titles(df): 
    logger.info('filling missing titles' )

    missing_titles_mask = df['title'].isna()

    if any(missing_titles_mask): 
        missing_titles =(df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.replace('-', ' '))
                        )
        df.loc[missing_titles_mask, 'title']= missing_titles.loc[:, 'missing_titles']
    else :  logger.info('no title had to be filled  ')
    return df

def _save_articles(namefile, data): 
    ''' method for save the  data with no missing value in a csv file'''
    filename =os.path.split(namefile)
    newFileName = filename[0]+'/'+filename[1].split('.')[0]+"(no_missing)_"+".csv"
    
    print(data.columns)
    with open(newFileName,  mode='w+', encoding='utf-8') as f: 
        data.to_csv(f, sep="|", index=False)  

def _generate_uids_for_row(df): 
    logger.info('Generating uids for each row')        
    uids = ( df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1 )
            .apply(lambda hash_obj: hash_obj.hexdigest())
            )
    df['uid'] = uids
    return df.set_index('uid')

def _remove_new_lines_from(df): 
    logger.info('Removing new lines from body')
    stripped_body = (df
                    .apply(lambda row: row['body'], axis=1)
                    .apply(lambda body: list(body))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\n', ' '), letters)))
                    .apply(lambda letters: list(map(lambda letter: letter.replace('\r', ' '), letters)))
                    .apply(lambda letters:  ''.join(letters))
                    )
    df['body'] = stripped_body

    return df
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', 
                        help='The Path to the dirty data', 
                        type=str)
    args = parser.parse_args()
    df = main(args.filename)
    _save_articles(args.filename, df)
    print(df.loc[: , : ])



    