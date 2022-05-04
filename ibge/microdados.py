"""
FUNÇÕES DE LEITURA DE MICRODADOS DO IBGE
BASEADO EM: https://github.com/otavio-s-s/lerMicrodados
"""

import pandas as pd
import yaml
from zipfile import ZipFile
from io import BytesIO
import requests


def ler_PNAD(ano, header=True, path=None) -> dict:
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
        Se o parâmetro for None, a função tentará recuperar a base diretamente do site do IBGE

    Returns
    -------
    dataframes: dict
        Dicionário contendo os dataframes correspondentes às tabelas processadas.
    '''

    # Recupera a configuração
    with open("ibge/pnad.yml", "r") as file:
        _pnad_conf = yaml.load(file, Loader=yaml.Loader)

    # Verifica a validade do ano
    if ano not in _pnad_conf["anos"]:
        raise ValueError(f'Ano inválido. Os anos disponíveis são: {_pnad_conf["anos"]}')

    # Caso a leitura seja local
    if path is not None:
        try:
            with ZipFile(path) as zip_file:
                dataframes = _PNAD_processing(zip_file, ano, _pnad_conf, header)
        except Exception:
            raise BaseException(f"Não foi possível abrir o arquivo \"{path}\"")

    # Caso a leitura seja remota
    else:
        url = _pnad_conf["ftp_url"]
        dict_name = f'{ano}'
        filename = _pnad_conf[dict_name]["filename"]
        full_path = f"{url}{ano}/{filename}"

        try:
            print("Recuperando dados do site do IBGE")
            req = requests.get(full_path)
            with ZipFile(BytesIO(req.content)) as zip_file:
                dataframes = _PNAD_processing(zip_file, ano, _pnad_conf, header)
        except Exception:
            raise BaseException(f"""Não foi possível recuperar o arquivo do ano {ano} do site do IBGE.\n
                            Tente baixar diretamente do site: {url}{ano}""")

    return dataframes


def _PNAD_processing(zip_file, ano, conf, header=True) -> dict:
    '''
    FUNÇÃO PRIVADA

    Realiza o pré processamento através do arquivo zip com os dados da PNAD de um determinado ano.

    Parameters
    ----------
    zip_file: ZipFile
        Arquivo recuperado do site do IBGE com as tabelas da PNAD de um ano.

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


def ler_POF(ano, header=True, path=None) -> dict:
    '''
    Realiza a leitura dos microdados da POF diretamente do site do IBGE,
    ou localmente do arquivo .zip baixado, e retorna um dicionário com dataframes Pandas.

    Parameters
    ----------
    ano: int
        Ano da POF a ser processada.

    header:  bool. Default = True
        Acrescenta o código da variável como nome de cada coluna.

    path: str, None. Default = None
        Caminho relativo ou absoluto para o arquivo .zip baixado do site do IBGE em:
        https://www.ibge.gov.br/estatisticas/sociais/habitacao/9050-pesquisa-de-orcamentos-familiares.html?=&t=microdados
        Se o parâmetro for None, a função tentará recuperar a base diretamente do site do IBGE

    Returns
    -------
    dataframes: dict
        Dicionário contendo os dataframes correspondentes às tabelas processadas.
    '''

    # Recupera a configuração
    with open("ibge/pof.yml", "r") as file:
        _pof_conf = yaml.load(file, Loader=yaml.Loader)

    # Verifica a validade do ano
    if ano == 2018:
        print("A PNAD 2017/2018 está identificada neste programa pelo ano 2017. Alterando ano para 2017.")
        ano = 2017
    if ano not in _pof_conf["anos"]:
        raise ValueError(f'Ano inválido. Os anos disponíveis são: {_pof_conf["anos"]}')

    # Caso a leitura seja local
    if path is not None:
        try:
            with ZipFile(path, mode='r') as zip_file:
                dataframes = _POF_processing(zip_file, ano, _pof_conf, header)
        except Exception:
            raise BaseException(f"Não foi possível abrir o arquivo \"{path}\"")

    # Caso a leitura seja remota
    else:
        url = _pof_conf["ftp_url"]
        dict_name = f'{ano}'
        filename = _pof_conf[dict_name]["filename"]
        full_path = f"{url}{filename}"

        try:
            print("Recuperando dados do site do IBGE")
            req = requests.get(full_path)
            with ZipFile(BytesIO(req.content)) as zip_file:
                dataframes = _POF_processing(zip_file, ano, _pof_conf, header)
        except Exception:
            raise BaseException(f"""Não foi possível recuperar o arquivo do ano {ano} do site do IBGE.\n
                            Tente baixar diretamente do site: {url}""")

    return dataframes


def _POF_processing(zip_file, ano, conf, header=True) -> dict:
    '''
    FUNÇÃO PRIVADA

    Realiza o pré processamento através do arquivo zip com os dados da POF de um determinado ano.

    Parameters
    ----------
    zip_file: ZipFile
        Arquivo recuperado do site do IBGE com as tabelas da POF de um ano.

    ano: int
        Ano da POF a ser processada.

    conf: dict
        Configurações lidas do arquivo YAML.

    header:  bool. Default = True
        Acrescenta o código da variável como nome de cada coluna.

    Returns
    -------
    dataframes: dict
        Dicionário contendo os dataframes correspondentes às tabelas processadas.
    '''

    _pof_conf = conf

    dataframes = {}
    for text_file in zip_file.infolist():

        dict_name = f'{ano}'
        filename = text_file.filename
        table_name = filename.split('.')[0]
        widths = _pof_conf[dict_name][table_name]['widths']
        headers = _pof_conf[dict_name][table_name]['headers']

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
