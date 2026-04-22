#!/usr/bin/env python3
"""
Busca pessoa juridica estrangeira como socia em empresas brasileiras.
Usa os arquivos Socios*.zip dos dados abertos da Receita Federal.


identif_socio = 3 -> Pessoa Juridica Estrangeira


Uso:
    python busca_socio_estrangeiro.py
    python busca_socio_estrangeiro.py --nome "TITAN CAPITAL HOLDINGS"
    python busca_socio_estrangeiro.py --nome "AVENTTTI" "VORCARO" "BANVOX"
    python busca_socio_estrangeiro.py --download
"""


import argparse
import glob
import os
import sys
import zipfile


import pandas as pd
from tqdm import tqdm


BASE_PATH = os.path.join(os.path.dirname(__file__), "dados_receita")
URL_BASE = "https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/2026-03-16/"


COLUNAS = [
    "cnpj_basico", "identif_socio", "nome_socio", "cnpj_cpf_socio",
    "qualif_socio", "data_entrada", "pais", "repr_legal",
    "nome_repr", "qualif_repr", "faixa_etaria",
]


QUALIFICACOES = {
    "05": "Administrador", "08": "Conselheiro de Administracao",
    "10": "Diretor", "16": "Presidente", "21": "Socio",
    "22": "Socio-Administrador", "49": "Socio-Gerente",
