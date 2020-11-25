import argparse
import logging
from urllib.parse import urlparse

import pandas as pd
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

def main(filename):
    logger.info('Starting cleaning process')
    # Leer el DataFrame
    df = _read_data(filename)
    # Extraer el newspaper_uid del archivo por medio del nombre del mismo
    # Recordar que el nombre del csv empieza con el nombre del newspaper
    newspaper_uid = _extract_newspaper_uid(filename)
    # Agregar la columna newspaper_uid al DataFrame
    df = _add_newspaper_uid_column(df, newspaper_uid)
    # Extraer el host del DataFrame
    df = _extract_host(df)

    return df
    
def _extract_host(df):
    logger.info('Extrating host from urls')
    # Obtener el host de las urls mediande apply y la libreria urllib con el metodo netloc
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df

def _add_newspaper_uid_column(df, newspaper_uid):
    logger.info('Filling newspaper_uid_column with {}'.format(newspaper_uid))
    df['newspaper_uid'] = newspaper_uid
    return df

# Leer El archivo csv y convertirlo en DataFrame
def _read_data(filename):
    logger.info('Reading file {}'.format(filename))
    return pd.read_csv(filename)

# Extraer el newspaper_uid del archivo por medio del nombre del mismo
def _extract_newspaper_uid(filename):
    logger.info('Extrating newspaper uid')
    # Seleccionar la primera parte del string que esté dividido por el caracter '_'
    newspaper_uid = filename.split('_')[0]
    logger.info('Newspaper uid detected {}'.format(newspaper_uid))
    return newspaper_uid

if __name__ == '__main__':
    # Preguntar al usuario que tipo de dato quiere
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',help='The path to the dirty data', type=str)
    # Parsear Argumentos
    args = parser.parse_args()
    df = main(args.filename)
    print(df)