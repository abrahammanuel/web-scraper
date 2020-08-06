import argparse
import logging
import re
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError
import datetime
import csv
from common import config

import news_page_object as news
logging.basicConfig(level=logging.INFO)


logger = logging.getLogger(__name__)

#building regExp 
is_well_formed_Link = re.compile(r'^https?://.+/.+$')#https://example.com/hello
is_root_path = re.compile(r'^/.+$')  #  /sometex123

def _news_scraper(news_site_uid, num='0'):
    num = int(num)
    ''' returns a list with data''' 
    host = config()['news_sites'][news_site_uid]['url']
    logging.info('Beginning scraper for {}'.format(host))
    
    homepage = news.HomePage(news_site_uid,  host)
    Larticles = []
    if num != None: logger.warning('just {} articles will be fetched'.format(num))
    for link in homepage.article_links: 
        article = _fetch_article(news_site_uid,  host,  link)

        if article: 
            logger.info('Article fetched!')
            Larticles.append(article)
            if num == 0:
                continue 
            elif len(Larticles)>num: 
                break

    return Larticles



def _fetch_article(news_site_uid,  host,  link): 
    '''method to fetch one article passed  '''
    logger.info('start fetching article at {}'.format(link))

    article = None
    try:
        article = news.ArticlePage(news_site_uid,  _build_link(host,  link))
    except (HTTPError, MaxRetryError) as e:
        logger.warning('Error while fetching the article',  exc_info = False)
    
    if article and not article.body : 
        logger.warning('Invalid article. There is no body ')
        return None
    
    return article

def _build_link(host,  link):
    '''return string with url '''
    if is_well_formed_Link.match(link): 
        return link
    elif is_root_path.match(link): 
        return '{}{}'.format(host, link)
    else: 
        return '{host}/{uri}'.format(host=host, uri=link)

def _save_articles(news_site_uid,  articles): 
    ''' method for save tha data inaa csv file; no return'''
    #now = datetime.datetime.now().strftime('%Y_%m_%d')
    out_file_name = '../csv_files/{news_site_uid}.csv'.format(news_site_uid=news_site_uid)
    csv_headers = list(filter(lambda property: not property.startswith('_'),dir(articles[0]) ))

    with open(out_file_name,  mode='w+', encoding='utf-8') as f: 
        writer = csv.writer(f, delimiter='|', quoting=csv.QUOTE_ALL , )   
        writer.writerow(csv_headers)

        for article in articles:
            row = [str(getattr(article, prop)) for prop in csv_headers]
            writer.writerow(row)
    

if __name__ == '__main__': 
    parser = argparse.ArgumentParser()
    
    news_site_choices = list(config()['news_sites'].keys())
    parser.add_argument('news_site',  
            help='the news site you want to scrape', 
            type=str, 
            choices= news_site_choices)
    parser.add_argument('int', type=str, nargs='?')

    args = parser.parse_args()
    
    # gets the data from the url    
    data = _news_scraper(args.news_site, args.int)

    #creat the csv file from the data
    _save_articles(args.news_site,  articles = data)
