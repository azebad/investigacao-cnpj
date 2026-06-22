#!/usr/bin/env python3
"""
Raspa processos no JusBrasil para um termo de busca (ex: "Qista"), usando
uma conta logada.

Requisitos:
    pip install -r requirements.txt
    playwright install chromium

Credenciais (arquivo .env na raiz do projeto, nunca commitado):
    JUSBRASIL_EMAIL=seu_email@exemplo.com
    JUSBRASIL_SENHA=sua_senha

Uso:
    python jusbrasil_scraper.py --termo "Qista"
    python jusbrasil_scraper.py --termo "Qista" --max-paginas 10
    python jusbrasil_scraper.py --termo "Qista" --headful   # ver o navegador
"""

import argparse
import csv
import os
import sys
import time

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

LOGIN_URL = "https://www.jusbrasil.com.br/login"
BUSCA_URL = "https://www.jusbrasil.com.br/busca?q={termo}&p={pagina}"
SAIDA_DIR = "resultados"


def login(page, email, senha):
    print("Fazendo login no JusBrasil...")
    page.goto(LOGIN_URL, wait_until="domcontentloaded")
    page.fill('input[type="email"], input[name="email"]', email)
    page.fill('input[type="password"], input[name="password"]', senha)
    page.click('button[type="submit"]')
    page.wait_for_load_state("networkidle", timeout=30_000)
    if "login" in page.url:
        raise RuntimeError(
            "Login parece ter falhado (ainda na pagina de login). "
            "Verifique JUSBRASIL_EMAIL/JUSBRASIL_SENHA ou possivel captcha."
        )
    print("  Login OK.")


def extrair_resultados_pagina(page):
    """Extrai os cards de resultado de processo/decisao da pagina atual.

    Os seletores podem precisar de ajuste se o JusBrasil mudar o layout.
    """
    cards = page.locator("article, [data-testid='search-result-card']")
    total = cards.count()
    resultados = []
    for i in range(total):
        card = cards.nth(i)
        try:
            titulo = card.locator("h2, h3, a").first.inner_text(timeout=2000).strip()
        except Exception:
            titulo = ""
        try:
            link_el = card.locator("a").first
            link = link_el.get_attribute("href") or ""
            if link and not link.startswith("http"):
                link = "https://www.jusbrasil.com.br" + link
        except Exception:
            link = ""
        try:
            trecho = card.inner_text(timeout=2000).strip().replace("\n", " | ")
        except Exception:
            trecho = ""
        if titulo or link:
            resultados.append({"titulo": titulo, "link": link, "trecho": trecho})
    return resultados


def buscar(termo, max_paginas, headful):
    email = os.environ.get("JUSBRASIL_EMAIL")
    senha = os.environ.get("JUSBRASIL_SENHA")
    if not email or not senha:
        print("ERRO: defina JUSBRASIL_EMAIL e JUSBRASIL_SENHA em um arquivo .env")
        sys.exit(1)

    todos = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headful)
        context = browser.new_context()
        page = context.new_page()

        login(page, email, senha)

        for pagina in range(1, max_paginas + 1):
            url = BUSCA_URL.format(termo=termo, pagina=pagina)
            print(f"Pagina {pagina}: {url}")
            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(2000)

            resultados = extrair_resultados_pagina(page)
            if not resultados:
                print("  Nenhum resultado nesta pagina - encerrando paginacao.")
                break
            print(f"  {len(resultados)} resultado(s) encontrados.")
            todos.extend(resultados)
            time.sleep(2)

        browser.close()
    return todos


def salvar(resultados, termo):
    os.makedirs(SAIDA_DIR, exist_ok=True)
    nome_arquivo = f"jusbrasil_{termo.lower().replace(' ', '_')}.csv"
    caminho = os.path.join(SAIDA_DIR, nome_arquivo)
    with open(caminho, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["titulo", "link", "trecho"])
        writer.writeheader()
        writer.writerows(resultados)
    print(f"\n-> {len(resultados)} resultado(s) salvos em: {caminho}")


def main():
    parser = argparse.ArgumentParser(description="Raspa processos no JusBrasil")
    parser.add_argument("--termo", required=True, help='Termo de busca (ex: "Qista")')
    parser.add_argument("--max-paginas", type=int, default=20, help="Limite de paginas")
    parser.add_argument("--headful", action="store_true", help="Mostrar o navegador")
    args = parser.parse_args()

    resultados = buscar(args.termo, args.max_paginas, args.headful)
    if not resultados:
        print("\nNenhum resultado encontrado.")
    else:
        salvar(resultados, args.termo)


if __name__ == "__main__":
    main()
