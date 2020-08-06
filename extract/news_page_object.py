import requests
import bs4

from common import config
class NewsPage:
    '''    class to get whatever element of website  
        '''
    def __init__(self, news_site_uid, url): 
        self._config = config()['news_sites'][news_site_uid]#get all the config in config.yaml
        self._queries = self._config['queries']
        self._html = None
        self._url = url
        '''_headers is for acces with permission to the websites emulating a web browser'''
        self._headers ={'User-Agent':  "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"}

        self._visit(self._url)  
        
    def _select(self,  query_string): 
        '''func selects a part from the {html} as its body or attributes

            query_string ---> string'''
        return self._html.select(query_string)


    def _visit(self, url):
        '''Func wich  from de instance makes request to the {url} given and return: the html text with bs4.beautifulsoup
        
        url---> a {string} of the url'''

        response = requests.get(url, headers=self._headers)
        response.raise_for_status()

        self._html = bs4.BeautifulSoup(response.text,  'html.parser')

class HomePage(NewsPage): 
    ''' this class with its article_links method gets the links from the homepage url '''
    def __init__(self, news_site_uid,  url):
        super().__init__(news_site_uid, url)
    
    @property
    def article_links(self):
        '''this func looks for every article link of the url given,  
        return =>  a {set} of links''' 
        lista=self._select(self._queries['homepage_article_links'])
        link_list =[link for link in lista if link and link.has_attr('href')] 


        return set(link['href'] for link in link_list)
        
class ArticlePage(NewsPage):
    ''' this class help you access to the elements of the articles of the website.
        
        (url, body, title,  datos)==> properties[list]'''       
    def __init__(self, news_site_uid,  url):
        super().__init__(news_site_uid,  url)

    @property
    def url(self): 
        result = self._url
        return result if len(result) else ""

    @property
    def body(self):
    #get the body of the website
        result = self._select(self._queries['article_body'])
        return result[0].text if len(result)  else ''
    @property
    def title(self): 
    #get the title of the website
        result = self._select(self._queries['article_title'])
        return result[0].text if len(result)  else ''

    @property
    def datos(self): 
        result = self._select(self._queries['datos'])
        return result[0].text if len(result) else ''
