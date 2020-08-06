import logging 
logging.basicConfig(level = logging.INFO)
import subprocess
import shlex

logger = logging.getLogger(__name__)
news_sites_uids = ['elpais'] #, 'elpais'

def main(): 
    _extract()
    _transform()
    _load()

def _extract(): 
    logger.info('starting extract process')
    for news_site_uid in news_sites_uids:  
        subprocess.run(['python', 'main.py', news_site_uid, '0'],  cwd='./extract')
        #subprocess.run(['find', '.', '-name', '{}*'.format(news_site_uid), '-exec', 'mv', '{}', '../transform/{}_.csv'.format(news_site_uid), ';'], cwd='./extract')
        

def _transform(): 
    logger.info('starting transform process')

    for news_site_uid in news_sites_uids: 
        # dirty_data_filename = '{}.csv'.format(news_site_uid)
        filename = '{}.csv'.format(news_site_uid) 
        subprocess.run(['python', 'main.py','../csv_files/{}'.format(filename) ], cwd='./transform')  

        # subprocess.run(['rm', dirty_data_filename], cwd='./transform')
        # subprocess.run(['mv', clean_data_filename, '../load/{}.csv'.format(news_site_uid)], cwd='./transform')


def _load(): 
    logger.info('starting load process')
    
    for news_site_uid in news_sites_uids: 
        logger.info('loading data from file {}'.format(news_site_uid))
        clean_data_filename= '../csv_files/clean_{}.csv'.format(news_site_uid)
        subprocess.run(['python','main.py', clean_data_filename],  cwd='./load')
        #subprocess.run(['find', './*.csv', '-type', 'f','-exec', 'rm', '{}', ';'],  cwd='./csv_files')  #para terminal de bash
    


if __name__ == '__main__': 
    main()

    subprocess.Popen('powershell Remove-Item ./csv_files/*.csv '.split())
    #para eliminar los archivos csv luego de poblar la base de datos hacemos


