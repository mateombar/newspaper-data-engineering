from common import config
import argparse
import logging
import csv
import datetime
# Regular Expressions
import re
import news_page_objects as news
from requests.exceptions import HTTPError
from urllib3.exceptions import MaxRetryError  

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)
#s? = 's' caracter optional, .+ = 1 o mas letras, $ = fin, ^ = comienzo del patron
is_well_formed_link = re.compile(r'^https?://.+/.+$') # https://example.com/hello
# Path que comienza desde la raiz, porque tienen una diagonal
is_root_path = re.compile(r'^/.+$') # /some-text

def _news_scraper(news_site_uid):
    host = config()['news_sites'][news_site_uid]['url']
    logging.info('Beginning scraper for {}'.format(host))
    # Obtener el homepage
    homepage = news.HomePage(news_site_uid, host)
    articles = []
    # Recorrer cada uno de los vinculos que existen en el homepage
    for link in homepage.article_links:
        # Obtener el articulo
        article = _fetch_article(news_site_uid, host, link)
        
        if article:
            logger.info('Article Fetched!!!!!!!')
            articles.append(article)
            # break
    _save_articles(news_site_uid, articles)        

def _save_articles(news_site_uid, articles):
    now = datetime.datetime.now().strftime('%Y_%m_%d')
    out_file_name = '{news_site_uid}_{datetime}_articles.csv'.format(news_site_uid = news_site_uid, datetime = now)
    # Para acceder a las propiedades de ArticlePage
    csv_headers = list(filter(lambda property: not property.startswith('_'), dir(articles[0])))
    with open(out_file_name, mode='w+', encoding= "utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)

        for article in articles:
            # row es una forma de determinar nuestros valores dentro de nuestro objeto
            row = [str(getattr(article, prop)) for prop in csv_headers]
            writer.writerow(row)


def _fetch_article(news_site_uid, host, link):
    logger.info('Start fetching article at {}'.format(link))

    article = None
    # Obtener los datos, pueden haber errores
    try:
        article = news.ArticlePage(news_site_uid, _build_link(host, link))
    # HttpError = Cuando el vinculo da error
    # MaxRetryError = Cuando intenta acceder de manera infinita al vinculo
    except (HTTPError, MaxRetryError) as e:
        logger.warning('Error while fetching the article', exc_info=False)

    if article and not article.body:
        logger.warning('Invalid Article. There is no body')
        return None

    return article

def _build_link(host, link):
    # Es verdadero si se tiene una url completa
    if is_well_formed_link.match(link):
        return link
    # Si el link arranca con diagonal
    elif is_root_path.match(link):
        return '{}{}'.format(host,link)
    # Si no comienza con diagonal
    else:
        return '{host}/{uri}'.format(host=host, uri=link)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    news_site_choices = list(config()['news_sites'].keys())
    parser.add_argument('news_site', help='The news site that you want to scrape',
                        type=str, choices=news_site_choices)
    args = parser.parse_args()
    _news_scraper(args.news_site)
