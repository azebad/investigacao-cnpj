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
}

PAISES = {
    "249": "Reino Unido", "076": "Brasil", "032": "Argentina",
    "175": "Ilhas Cayman", "014": "Alemanha", "246": "EUA",
    "105": "França", "119": "Irlanda", "158": "Luxemburgo",
    "174": "Malta", "191": "Paises Baixos", "229": "Suíça",
    "225": "Portugal",
}


def baixar_arquivos():
    """Baixa os arquivos Socios*.zip se ainda nao existirem."""
    os.makedirs(BASE_PATH, exist_ok=True)
    import requests
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
            print(f"  OK {nome}")
        except Exception as e:
            print(f"  ERRO em {nome}: {e}")
    print("\nDownload concluido.")


def buscar_pj_estrangeira(termos):
    """
    Busca socios do tipo 3 (PJ Estrangeira) cujo nome contenha
    qualquer um dos termos fornecidos.
    """
    arquivos = sorted(glob.glob(os.path.join(BASE_PATH, "Socios*.zip")))
    if not arquivos:
        print(f"ERRO: Nenhum arquivo Socios*.zip em {BASE_PATH}")
        print("Execute primeiro: python busca_socio_estrangeiro.py --download")
        sys.exit(1)

    termos_upper = [t.upper().strip() for t in termos]
    print(f"\nBuscando PJ estrangeira com termos: {termos_upper}")
    print(f"Processando {len(arquivos)} arquivo(s)...\n")

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
                        # Filtrar apenas identif_socio == '3' (PJ estrangeira)
                        mask_tipo = chunk["identif_socio"].str.strip() == "3"
                        chunk_estrang = chunk[mask_tipo]
                        if chunk_estrang.empty:
                            continue

                        nome_col = chunk_estrang["nome_socio"].str.upper().str.strip()
                        mask_nome = pd.Series(False, index=chunk_estrang.index)
                        for termo in termos_upper:
                            mask_nome |= nome_col.str.contains(termo, na=False)

                        hits = chunk_estrang[mask_nome]
                        if not hits.empty:
                            resultados.append(hits)
                            encontrados += len(hits)

        status = f"{encontrados} resultado(s)" if encontrados else "-"
        print(f"  {nome_arq}: {status}")

    if not resultados:
        return pd.DataFrame()
    return pd.concat(resultados, ignore_index=True).drop_duplicates()


def enriquecer_cnpj(df):
    """Tenta buscar razao social das empresas encontradas via BrasilAPI."""
    try:
        import requests
        cnpjs_unicos = df["cnpj_basico"].str.strip().unique()
        print(f"\nBuscando razao social de {len(cnpjs_unicos)} empresa(s)...")
        nomes = {}
        for cnpj_basico in cnpjs_unicos[:50]:  # limita a 50 para nao sobrecarregar
            # Tenta o CNPJ basico com 0001 como sufixo
            cnpj_completo = cnpj_basico.zfill(8) + "0001"
            try:
                r = requests.get(
                    f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_completo}",
                    timeout=5
                )
                if r.status_code == 200:
                    data = r.json()
                    nomes[cnpj_basico] = data.get("razao_social", "")
            except Exception:
                pass
        df["razao_social_br"] = df["cnpj_basico"].map(nomes).fillna("")
    except Exception:
        df["razao_social_br"] = ""
    return df


def exibir_resultados(df):
    print(f"\n{'='*65}")
    print(f"  TOTAL: {len(df)} ocorrencia(s) encontrada(s)")
    print(f"{'='*65}\n")

    for _, row in df.iterrows():
        pais_cod = str(row.get("pais", "")).strip().zfill(3)
        pais_nome = PAISES.get(pais_cod, f"Cod. {pais_cod}")
        qualif_cod = str(row.get("qualif_socio", "")).strip()
        qualif_desc = QUALIFICACOES.get(qualif_cod, f"Cod. {qualif_cod}")
        razao = row.get("razao_social_br", "")

        print(f"  Socio estrangeiro: {row.get('nome_socio','')}")
        print(f"  Pais de origem:    {pais_nome}")
        print(f"  CNPJ basico BR:    {row.get('cnpj_basico','').strip()}")
        if razao:
            print(f"  Razao social BR:   {razao}")
        print(f"  Qualificacao:      {qualif_desc}")
        print(f"  Data de entrada:   {row.get('data_entrada','').strip()}")
        print()

    os.makedirs("resultados", exist_ok=True)
    saida = "resultados/socios_estrangeiros_encontrados.csv"
    df.to_csv(saida, index=False, encoding="utf-8-sig")
    print(f"  -> Salvo em: {saida}")


def main():
    parser = argparse.ArgumentParser(
        description="Busca PJ estrangeira como socia em empresas brasileiras (Receita Federal)"
    )
    parser.add_argument("--download", action="store_true", help="Baixar Socios*.zip")
    parser.add_argument(
        "--nome", nargs="+", metavar="TERMO",
        help="Termos de busca no nome do socio estrangeiro"
    )
    args = parser.parse_args()

    if args.download:
        baixar_arquivos()
        if not args.nomebusca_socio_estrangeiro.py:
            return

    # Termos padrao se nao especificado
    termos = args.nome if args.nome else [
        "TITAN CAPITAL HOLDINGS",
        "TITAN CAPITAL HOLDING",
        "AVENTTTI",
        "BANVOX",
        "NORTH SEA CAPITAL",
        "WNT CAPITAL",
    ]

    df = buscar_pj_estrangeira(termos)

    if df.empty:
        print("\n  Nenhum resultado encontrado.")
        print("  Os termos buscados nao aparecem como socios estrangeiros")
        print("  em nenhuma empresa brasileira nos dados de marco/2026.")
        return

    # Enriquecer com razao social brasileira
    df = enriquecer_cnpj(df)
    exibir_resultados(df)


if __name__ == "__main__":
    main()
