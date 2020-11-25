from common import config
import requests
import bs4

#Clase principal
class NewsPage:
    def __init__(self, news_site_uid, url):
        # Obtener una referencia de nuestra configuracion
        self._config = config()['news_sites'][news_site_uid]
        # Obtener queries de la configuracion
        self._queries = self._config['queries']
        self._html = None
        self._url = url
        self._visit(url)

    # Obtener info del doc que acabamos de parsear
    def _select(self,query_string):
        return self._html.select(query_string)

    # Visitar una pagina
    def _visit(self, url):
        # Obtener el response de la url del sitio
        response = requests.get(url)
        response.encoding ='utf-8'
        # Metodo que arroja un error si la solicitud no fue concluida correctamente
        response.raise_for_status()
        self._html = bs4.BeautifulSoup(response.text, 'html.parser')

# Es una instancia que extiende de la pagina de noticias
# Con el parametro NewsPage hace referencia a que extiende de la clase NewsPage
class HomePage(NewsPage):
    def __init__(self, news_site_uid, url):
        #Para inicializar la superclase
        super().__init__(news_site_uid,url)

    #Propiedad, computar cuales son nuestros article links
    @property
    def article_links(self):
        link_list = []
        for link in self._select(self._queries['homepage_articles_links']):
            if link and link.has_attr('href'):
                link_list.append(link)
        # Retornar la conf pero sin elementos repetidos, 
        # set() construye una coleccion desordenada de elementos unicos
        return set(link['href'] for link in link_list)

class ArticlePage(NewsPage):
    def __init__(self, news_site_uid, url):
        super().__init__(news_site_uid,url)

    #Crear propiedad y computar los datos solicitados
    @property
    def body(self):
        result = self._select(self._queries['article_body'])
        return result[0].text if len(result) else ''
    @property
    def title(self):
        result = self._select(self._queries['article_title'])
        return result[0].text if len(result) else ''
    
    @property
    def url(self):
        return self._url