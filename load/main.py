import argparse
import logging
logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

import pandas as pd

from article import Article
from base import Base, engine,  Session

def main(filename): 
    '''methof that load clean dataframe into the database
    
    no return '''
    Base.metadata.create_all(engine)#create all the tables in the base
    session = Session()
    articles = pd.read_csv(filename, sep="|") # , index_col=0

    for index,  row in articles.iterrows():# iterrows returns a iterable of index and rows 
        logger.info('loading article uid {} into DB'.format(row['uid']))
        
        #we stored just the values but not the index
        article = Article(row['uid'], 
                        row['body'], 
                        row['datos'], 
                        row['host'], 
                        row['newspaper_uid'], 
                        row['n_tokens_body'], 
                        row['n_tokens_title'], 
                        row['title'], 
                        row['url'])
        #add each article in session                        
        session.add(article)

    #the session is saved in database and later close the session
    session.commit()
    session.close()
    

if __name__ == '__main__': 
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', 
                        help='the file you want to load into the db', 
                        type = str)
    args = parser.parse_args()
    main(args.filename)
