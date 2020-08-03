import argparse 
import logging
import csv
logging.basicConfig(level=logging.INFO)
from urllib.parse import urlparse
import os
import pandas as pd
import hashlib
import save_csv_file_clean#to save dataframe in a file
import nltk
from nltk.corpus import stopwords
logger = logging.getLogger(__name__)
from re import sub

def main(filename): 
    logger.info('Starting cleaning process')

    df = _read_data(filename)# 1
    newspaper_uid = _extract_newspaper_uid(filename)  # 2          
    df = _add_newspaper_uid_column(df,  newspaper_uid) # 3
    df = _extraer_host(df) # 4
    df = _fill_missing_titles(df)
    df = _generate_uids_for_row(df)
    df = _remove_new_lines_from(df,  ['body', 'datos'])
    df = _tokenize_text(df, ['title', 'body'])
    df = _remove_duplicate_entries(df, ['title'])
    df = _drop_rows_with_missing_values(df)


    return df
    
def _read_data(filename): 
    logger.info('1. reading file: {}'.format(filename))

    return pd.read_csv(filename, delimiter="|")

def _extract_newspaper_uid(filename): 
    filename = os.path.split(filename)
    logging.info('extracting newspaper uid')                                
    newspaper_uid = filename[1].split('_')[0]

    logger.info('newspaper uid detected:  {}'.format(newspaper_uid))
    return newspaper_uid

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('2. filling newspaper uid column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid

    return df

def _extraer_host(df):
    logger.info('3. extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)

    return df

def _fill_missing_titles(df): 
    logger.info('4. filling missing titles' )

    missing_titles_mask = df['title'].isna()

    if any(missing_titles_mask): 
        missing_titles =(df[missing_titles_mask]['url']
                        .str.extract(r'(?P<missing_titles>[^/]+)$')
                        .applymap(lambda title: title.replace('-', ' '))
                        )
        df.loc[missing_titles_mask, 'title']= missing_titles.loc[:, 'missing_titles']
    else :  logger.info('no title had to be filled  ')
    return df


def _generate_uids_for_row(df): 
    logger.info('5. Generating uids for each row')        
    uids = ( df
            .apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1 )
            .apply(lambda hash_obj: hash_obj.hexdigest())
            )
    df['uid'] = uids
    return df.set_index('uid')

def _remove_new_lines_from(df, columns): 
    logger.info('6. removing new lines and spaces')        
    stripped_body =lambda col:  (df
                    .dropna()
                    .apply(lambda row: row[col], axis=1)
                    .apply(lambda body: list(body))
                    .apply(lambda letters: list(map(lambda letter: sub('[\n|\r]', '', letter), letters)))
                    .apply(lambda letters: list(map(lambda letter: sub('\s{2: }', '', letter), letters)))
                    .apply(lambda letters:  ''.join(letters))
                    )

    for col in columns: 
        df[col] =stripped_body(col)
    
    return df

def _tokenize_text(df, columns): 
    logger.info('7. generating quantitive tokens of words')
    stop_words = set(stopwords.words('spanish'))
    tokenize_col=lambda col:(df
                        .dropna()
                        .apply(lambda row: nltk.word_tokenize(row[col]), axis=1)
                        .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
                        .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
                        .apply(lambda words_list: list(filter(lambda word: word not in stop_words, words_list)))
                        .apply(lambda valid_words_list: len(valid_words_list))
                    )

     
    for column in columns: 
        try:
            df[column]
            logger.info(f'generating numnber of unique token for {column}')
            token_col = tokenize_col(column)
            new_col_name='n_tokens_'+column
            df[new_col_name] = token_col
        except :
            raise TypeError (f'there is not column named {column}  ')
            

     

    return df

def _remove_duplicate_entries(df, column_names): 
    logger.info('8. Removing duplicate entries')
    
    for col in column_names: 
        try: 
            df[col]
            df.drop_duplicates(subset=[col], keep='first', inplace=True)
        except : 
            print(f'\n *********{col} no existe columna con ese nombre *******')
            continue

    return df

def _drop_rows_with_missing_values(df): 
    logger.info("9. dropping rows with missing values ")
    return df.dropna()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', 
                        help='The Path to the dirty data', 
                        type=str)
    args = parser.parse_args()
    df = main(args.filename )
    save_csv_file_clean._save_data(args.filename, df) #for saving the file
    for i in range(len(df.columns)):  print(df.iloc[5:15 ,i ])



    