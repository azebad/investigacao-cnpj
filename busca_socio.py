#!/usr/bin/env python3
"""
Busca nomes na base de socios da Receita Federal (dados abertos CNPJ).

Uso:
    python busca_socio.py --download
    python busca_socio.py --nome "VICTOR HENRIQUE MEDEIROS LIMA"
    python busca_socio.py --parcial "VICTOR" "MEDEIROS" "LIMA" --faixa 5 6
"""

import argparse
import glob
import os
import sys
import zipfile

import pandas as pd
import requests
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
    "65": "Titular (EI)", "66": "MEI",
}


def baixar_arquivos():
    os.makedirs(BASE_PATH, exist_ok=True)
    print(f"Baixando Socios0.zip a Socios9.zip em: {BASE_PATH}\n")
    for i in range(10):
        nome = f"Socios{i}.zip"
        destino = os.path.join(BASE_PATH, nome)
        url = f"{URL_BASE}{nome}"
        if os.path.exists(destino) and os.path.getsize(destino) > 1_000_000:
            print(f"  {nome} ja existe ({os.path.getsize(destino)/1e6:.0f} MB) - pulando")
            continue
        print(f"  Baixando {nome}...")
        try:
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()
            total = int(r.headers.get("content-length", 0))
            with open(destino, "wb") as f, tqdm(
                total=total, unit="B", unit_scale=True, desc=nome, leave=False
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    bar.update(len(chunk))
            print(f"  OK {nome} ({os.path.getsize(destino)/1e6:.0f} MB)")
        except Exception as e:
            print(f"  ERRO em {nome}: {e}")
    print("\nDownload concluido.")


def buscar(nomes_exatos=None, termos_parciais=None, faixas=None):
    arquivos = sorted(glob.glob(os.path.join(BASE_PATH, "Socios*.zip")))
    if not arquivos:
        print(f"ERRO: Nenhum arquivo Socios*.zip em {BASE_PATH}")
        print("Execute primeiro: python busca_socio.py --download")
        sys.exit(1)
    print(f"\nProcessando {len(arquivos)} arquivo(s)...\n")
    resultados = []
    for zip_path in arquivos:
        nome_arq = os.path.basename(zip_path)
        encontrados = 0
        with zipfile.ZipFile(zip_path) as z:
            for fname in z.namelist():
                with z.open(fname) as f:
                    for chunk in pd.read_csv(
                        f, sep=";", encoding="latin-1",
                        header=None, dtype=str,
                        chunksize=500_000, names=COLUNAS,
                    ):
                        nome_col = chunk["nome_socio"].str.upper().str.strip()
                        faixa_col = chunk["faixa_etaria"].str.strip()
                        mask = pd.Series(False, index=chunk.index)
                        if nomes_exatos:
                            mask |= nome_col.isin([n.upper().strip() for n in nomes_exatos])
                        if termos_parciais:
                            m = pd.Series(True, index=chunk.index)
                            for termo in termos_parciais:
                                m &= nome_col.str.contains(termo.upper(), na=False)
                            if faixas:
                                m &= faixa_col.isin(faixas)
                            mask |= m
                        hits = chunk[mask]
                        if not hits.empty:
                            resultados.append(hits)
                            encontrados += len(hits)
        status = f"{encontrados} resultado(s)" if encontrados else "-"
        print(f"  {nome_arq}: {status}")
    if not resultados:
        return pd.DataFrame()
    return pd.concat(resultados, ignore_index=True).drop_duplicates()


def exibir_resultados(df):
    print(f"\n{'='*65}")
    print(f"  TOTAL: {len(df)} ocorrencia(s) encontrada(s)")
    print(f"{'='*65}\n")
    for _, row in df.iterrows():
        qualif_cod = str(row.get("qualif_socio", "")).strip()
        qualif_desc = QUALIFICACOES.get(qualif_cod, f"Cod. {qualif_cod}")
        faixa = row.get("faixa_etaria", "").strip()
        faixa_desc = {"1":"ate 12","2":"13-20","3":"21-30","4":"31-40",
                      "5":"31-40","6":"41-50","7":"51-60","8":"61-70","9":"70+"}.get(faixa, faixa)
        print(f"  Nome:        {row.get('nome_socio','')}")
        print(f"  CNPJ Basico: {row.get('cnpj_basico','').strip()}")
        print(f"  CPF (mask):  {row.get('cnpj_cpf_socio','').strip()}")
        print(f"  Qualif.:     {qualif_desc}")
        print(f"  Entrada:     {row.get('data_entrada','').strip()}")
        print(f"  Faixa etar.: {faixa_desc}")
        print()
    os.makedirs("resultados", exist_ok=True)
    saida = "resultados/socios_encontrados.csv"
    df.to_csv(saida, index=False, encoding="utf-8-sig")
    print(f"  -> Salvo em: {saida}")


def main():
    parser = argparse.ArgumentParser(description="Busca socios na base da Receita Federal")
    parser.add_argument("--download", action="store_true", help="Baixar Socios*.zip")
    parser.add_argument("--nome", nargs="+", metavar="NOME", help="Nome(s) exato(s)")
    parser.add_argument("--parcial", nargs="+", metavar="TERMO", help="Termos parciais")
    parser.add_argument("--faixa", nargs="+", metavar="COD", default=["5","6"],
                        help="Codigos de faixa etaria (padrao: 5 6 = 31-50 anos)")
    args = parser.parse_args()
    if args.download:
        baixar_arquivos()
        if not args.nome and not args.parcial:
            return
    if not args.nome and not args.parcial:
        print("Busca padrao: Victor Henrique Medeiros Lima + variacoes\n")
        nomes_exatos = [
            "VICTOR HENRIQUE MEDEIROS LIMA",
            "VICTOR H MEDEIROS LIMA",
            "VICTOR MEDEIROS LIMA",
            "VICTOR HENRIQUE LIMA",
        ]
        termos_parciais = ["VICTOR", "MEDEIROS", "LIMA"]
        faixas = ["5", "6"]
    else:
        nomes_exatos = args.nome or []
        termos_parciais = args.parcial or []
        faixas = args.faixa
    df = buscar(nomes_exatos=nomes_exatos, termos_parciais=termos_parciais, faixas=faixas)
    if df.empty:
        print("\n  Nenhum resultado encontrado.")
        print("  Tente: --faixa 1 2 3 4 5 6 7 8 9")
    else:
        exibir_resultados(df)


if __name__ == "__main__":
    main()
