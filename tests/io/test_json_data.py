from src.io.get_files_dict import main as get_files_dict
import pandas as pd
import json


def test_json_data_url_current_vs_local_cnaes():
    with open('tests/fixtures/cnaes.json', encoding='cp1252') as json_file:
        dict_expected = json.load(json_file)

    dict_files = get_files_dict()
    _key = None
    for key in dict_files['TABELAS']:
        if 'CNAE' in key:
            _key = key
            break

    # get current file
    url = dict_files['TABELAS'][_key]['link_to_download']
    df = pd.read_csv(url, sep=';', encoding='cp1252', header=None)
    df[0] = df[0].astype(str)
    _dict = dict(df.values)

    # assert len dict
    assert len(_dict) == len(dict_expected)

    # assert same keys
    assert set(_dict.keys()) - set(dict_expected.keys()) == set()

    # assert values
    for key in _dict.keys():
        assert _dict[key] == dict_expected[key]


def test_json_data_url_current_vs_local_natju():
    with open('tests/fixtures/natju.json', encoding='cp1252') as json_file:
        dict_expected = json.load(json_file)

    dict_files = get_files_dict()
    _key = None
    for key in dict_files['TABELAS']:
        if 'NATJUCSV' in key:
            _key = key
            break

    # get current file
    url = dict_files['TABELAS'][_key]['link_to_download']
    df = pd.read_csv(url, sep=';', encoding='cp1252', header=None)
    df[0] = df[0].astype(str)
    _dict = dict(df.values)

    # assert len dict
    assert len(_dict) == len(dict_expected)

    # assert same keys
    assert set(_dict.keys()) - set(dict_expected.keys()) == set()

    # assert values
    for key in _dict.keys():
        assert _dict[key] == dict_expected[key]


def test_json_data_url_current_vs_local_qual_socio():
    dict_expected = {'0': 'Não informada', "5": "Administrador", "8": "Conselheiro de Administração", "9": "Curador",
                     "10": "Diretor", "11": "Interventor", "12": "Inventariante", "13": "Liquidante", "14": "Mãe",
                     "15": "Pai", "16": "Presidente", "17": "Procurador", "18": "Secretário",
                     "19": "Síndico (Condomínio)", "20": "Sociedade Consorciada", "21": "Sociedade Filiada",
                     "22": "Sócio", "23": "Sócio Capitalista", "24": "Sócio Comanditado", "25": "Sócio Comanditário",
                     "26": "Sócio de Indústria", "28": "Sócio-Gerente",
                     "29": "Sócio Incapaz ou Relat.Incapaz (exceto menor)",
                     "30": "Sócio Menor (Assistido/Representado)", "31": "Sócio Ostensivo", "32": "Tabelião",
                     "33": "Tesoureiro", "34": "Titular de Empresa Individual Imobiliária", "35": "Tutor",
                     "37": "Sócio Pessoa Jurídica Domiciliado no Exterior",
                     "38": "Sócio Pessoa Física Residente no Exterior", "39": "Diplomata", "40": "Cônsul",
                     "41": "Representante de Organização Internacional", "42": "Oficial de Registro",
                     "43": "Responsável", "46": "Ministro de Estado das Relações Exteriores",
                     "47": "Sócio Pessoa Física Residente no Brasil",
                     "48": "Sócio Pessoa Jurídica Domiciliado no Brasil", "49": "Sócio-Administrador",
                     "50": "Empresário", "51": "Candidato a cargo Político Eletivo", "52": "Sócio com Capital",
                     "53": "Sócio sem Capital", "54": "Fundador", "55": "Sócio Comanditado Residente no Exterior",
                     "56": "Sócio Comanditário Pessoa Física Residente no Exterior",
                     "57": "Sócio Comanditário Pessoa Jurídica Domiciliado no Exterior",
                     "58": "Sócio Comanditário Incapaz", "59": "Produtor Rural", "60": "Cônsul Honorário",
                     "61": "Responsável indígena", "62": "Representante da Instituição Extraterritorial",
                     "63": "Cotas em Tesouraria", "64": "Administrador Judicial",
                     "65": "Titular Pessoa Física Residente ou Domiciliado no Brasil",
                     "66": "Titular Pessoa Física Residente ou Domiciliado no Exterior",
                     "67": "Titular Pessoa Física Incapaz ou Relativamente Incapaz (exceto menor)",
                     "68": "Titular Pessoa Física Menor (Assistido/Representado)", "69": "Beneficiário Final",
                     "70": "Administrador Residente ou Domiciliado no Exterior",
                     "71": "Conselheiro de Administração Residente ou Domiciliado no Exterior",
                     "72": "Diretor Residente ou Domiciliado no Exterior",
                     "73": "Presidente Residente ou Domiciliado no Exterior",
                     "74": "Sócio-Administrador Residente ou Domiciliado no Exterior",
                     "75": "Fundador Residente ou Domiciliado no Exterior",
                     "78": "Titular Pessoa Jurídica Domiciliada no Brasil",
                     "79": "Titular Pessoa Jurídica Domiciliada no Exterior"}

    dict_files = get_files_dict()
    _key = None
    for key in dict_files['TABELAS']:
        if 'QUALSCSV' in key:
            _key = key
            break

    # get current file
    url = dict_files['TABELAS'][_key]['link_to_download']
    df = pd.read_csv(url, sep=';', encoding='cp1252', header=None)
    df[0] = df[0].astype(str)
    _dict = dict(df.values)

    # assert len dict
    assert len(_dict) == len(dict_expected)

    # assert same keys
    assert set(_dict.keys()) - set(dict_expected.keys()) == set()

    # assert values
    for key in _dict.keys():
        assert _dict[key] == dict_expected[key]


def test_json_data_url_current_vs_local_motivos():
    with open('tests/fixtures/motivos.json', encoding='cp1252') as json_file:
        dict_expected = json.load(json_file)

    dict_files = get_files_dict()
    _key = None
    for key in dict_files['TABELAS']:
        if 'MOTICSV' in key:
            _key = key
            break

    # get current file
    url = dict_files['TABELAS'][_key]['link_to_download']
    df = pd.read_csv(url, sep=';', encoding='cp1252', header=None)
    df[0] = df[0].astype(str)
    _dict = dict(df.values)

    # assert len dict
    assert len(_dict) == len(dict_expected)

    # assert same keys
    assert set(_dict.keys()) - set(dict_expected.keys()) == set()

    # assert values
    for key in _dict.keys():
        assert _dict[key] == dict_expected[key]


def test_json_data_url_current_vs_local_pais():
    with open('tests/fixtures/pais.json', encoding='cp1252') as json_file:
        dict_expected = json.load(json_file)

    dict_files = get_files_dict()
    _key = None
    for key in dict_files['TABELAS']:
        if 'PAISCSV' in key:
            _key = key
            break

    # get current file
    url = dict_files['TABELAS'][_key]['link_to_download']
    df = pd.read_csv(url, sep=';', encoding='cp1252', header=None)
    df[0] = df[0].astype(str)
    _dict = dict(df.values)

    # assert len dict
    assert len(_dict) == len(dict_expected)

    # assert same keys
    assert set(_dict.keys()) - set(dict_expected.keys()) == set()

    # assert values
    for key in _dict.keys():
        assert _dict[key] == dict_expected[key]


def test_json_data_url_current_vs_local_munic():
    with open('tests/fixtures/municipios.json', encoding='cp1252') as json_file:
        dict_expected = json.load(json_file)

    dict_files = get_files_dict()
    _key = None
    for key in dict_files['TABELAS']:
        if 'MUNICCSV' in key:
            _key = key
            break

    # get current file
    url = dict_files['TABELAS'][_key]['link_to_download']
    df = pd.read_csv(url, sep=';', encoding='cp1252', header=None)
    df[0] = df[0].astype(str)
    _dict = dict(df.values)

    # assert len dict
    assert len(_dict) == len(dict_expected)

    # assert same keys
    assert set(_dict.keys()) - set(dict_expected.keys()) == set()

    # assert values
    for key in _dict.keys():
        assert _dict[key] == dict_expected[key]
