""" 
FUNÇÕES DE LEITURA DE MICRODADOS DO IBGE
BASEADO EM: https://github.com/otavio-s-s/lerMicrodados
"""

import pandas as pd
import yaml
from yaml import Loader
from zipfile import ZipFile
from io import BytesIO
import requests

def ler_PNAD(ano, header=True, path = None) -> dict:
    '''
    Realiza a leitura dos microdados da PNAD diretamente do site do IBGE,
    ou localmente do arquivo .zip baixado, e retorna um dicionário com dataframes Pandas.

    Parameters
    ----------
    ano: int
        Ano da PNAD a ser processada.

    header:  bool. Default = True
        Acrescenta o código da variável como nome de cada coluna.    
    
    path: str, None. Default = None
        Caminho relativo ou absoluto para o arquivo .zip baixado do site do IBGE em:
        https://www.ibge.gov.br/estatisticas/sociais/habitacao/9127-pesquisa-nacional-por-amostra-de-domicilios?=&t=microdados
        Caso o parâmetro for None, a função tentará recuperar a base diretamente do site do IBGE

    Returns
    -------
    dataframes: dict
        Dicionário contendo os dataframes correspondentes às tabelas processadas.
    '''

    # Recupera a configuração
    with open("ibge/pnad.yml", "r") as file:
        _pnad_conf = yaml.load(file, Loader=Loader)

    # Verifica a validade do ano
    if ano not in _pnad_conf["anos"]:
        raise ValueError(f'Ano inválido. Os anos disponíveis são: {_pnad_conf["anos"]}')

    # Caso a leitura seja local
    if path != None:
        try:
            with ZipFile(path) as zip_file:
                dataframes = _PNAD_processing(zip_file, ano, _pnad_conf, header)
        except:
            raise Exception(f"Não foi possível abrir o arquivo \"{path}\"")
    
    # Caso a leitura seja remota
    else:
        url = _pnad_conf["ftp_url"]
        dict_name = f'{ano}'
        filename = _pnad_conf[dict_name]["filename"]
        full_path = f"{url}{ano}/{filename}"

        try:
            req = requests.get(full_path)
            with ZipFile(BytesIO(req.content)) as zip_file:
                dataframes = _PNAD_processing(zip_file, ano, _pnad_conf, header)
        except:
            raise Exception(f"Não foi possível recuperar o arquivo do ano {ano} do site do IBGE")

    return dataframes


def _PNAD_processing(zip_file, ano, conf, header = True) -> dict:

    '''
    FUNÇÃO PRIVADA
    Realiza o pré processamento através do arquivo zip com os dados do PNAD de um determinado ano.

    Parameters
    ----------
    zip_file: ZipFile
        Arquivo recuperado do site do IBGE com as tabelas do PNAD de um ano.

    ano: int
        Ano da PNAD a ser processada.
    
    conf: dict
        Configurações lidas do arquivo YAML.

    header:  bool. Default = True
        Acrescenta o código da variável como nome de cada coluna.    

    Returns
    -------
    dataframes: dict
        Dicionário contendo os dataframes correspondentes às tabelas processadas.
    '''

    _pnad_conf = conf

    dataframes = {}
    for text_file in zip_file.infolist():

        dict_name = f'{ano}'
        filename = text_file.filename
        table_name = '{}'.format(filename.split('.')[0]).split('/')[1]
        widths = _pnad_conf[dict_name][table_name]['widths']
        headers = _pnad_conf[dict_name][table_name]['headers']

        print(f"Processando tabela {table_name}")
        
        try:
            with zip_file.open(filename) as file:
                df = pd.read_fwf(file, widths=widths, index=False, header=None, dtype=str)
            if header:
                df.columns = headers
        except Exception as e:
            raise e

        dataframes[table_name] = df
        
    return dataframes
