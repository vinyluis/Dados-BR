""" 
FUNÇÕES DE LEITURA DE MICRODADOS DO IBGE
BASEADO EM: https://github.com/otavio-s-s/lerMicrodados
"""

import pandas as pd
import ftplib
import yaml
from yaml import Loader
from zipfile import ZipFile


def ler_PNAD(path, ano, header=True) -> dict:
    '''
    Realiza a leitura dos microdados da PNAD diretamente do arquivo .zip baixado do site do IBGE
    e retorna um dicionário com dataframes Pandas.

    Parameters
    ----------
    path: str
        Caminho relativo ou absoluto para o arquivo .zip baixado do site do IBGE em:
        https://www.ibge.gov.br/estatisticas/sociais/habitacao/9127-pesquisa-nacional-por-amostra-de-domicilios?=&t=microdados
    
    ano: int
        Ano da PNAD a ser processada.

    header:  bool, default = True
        Acrescenta o código da variável como nome de cada coluna.    
    
    Returns
    -------
    dataframes: dict
        Dicionário contendo os dataframes correspondentes às tabelas processadas.
    '''

    with open("ibge/pnad.yml", "r") as file:
        _pnad_conf = yaml.load(file, Loader=Loader)

    if ano not in _pnad_conf["anos"]:
        raise ValueError(f'Ano inválido. Os anos aceitavéis são: {_pnad_conf["anos"]}')

    dataframes = {}
    zip_file = ZipFile(path)
    for text_file in zip_file.infolist():

        dict_name = f'{ano}'
        file_name = text_file.filename
        table_name = '{}'.format(file_name.split('.')[0]).split('/')[1]
        widths = _pnad_conf[dict_name][table_name]['widths']
        headers = _pnad_conf[dict_name][table_name]['headers']

        print(file_name)
        print(table_name)
        
        with zip_file.open(file_name) as file:
            df = pd.read_fwf(file, widths=widths, index=False, header=None, dtype=str)
        if header:
            df.columns = headers
        
        dataframes[table_name] = df
        
    return dataframes
    
