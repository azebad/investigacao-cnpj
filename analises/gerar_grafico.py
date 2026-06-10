import openpyxl
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

import os
XLSX = os.path.join(os.path.dirname(__file__), "Ibov_mensal.xlsx")

wb = openpyxl.load_workbook(XLSX, data_only=True)
ws = wb["Planilha1"]
rows = list(ws.iter_rows(min_row=5, max_row=135, values_only=True))
header = rows[0]
dates = list(header[4:])

# Empresas de utilities (energia elétrica, saneamento e gás) presentes no
# Ibovespa ao longo do período. AXIA3/AXIA6/AXIA7 = Eletrobras (renomeada
# para Axia Energia em nov/2025).
util_tickers = {
    "AXIA3", "AXIA5", "AXIA6", "AXIA7",  # Eletrobras / Axia Energia
    "SBSP3",   # Sabesp
    "EQTL3",   # Equatorial Energia
    "CPLE3", "CPLE5", "CPLE6",  # Copel
    "CMIG4",   # Cemig
    "ENGI11",  # Energisa
    "EGIE3",   # Engie Brasil Energia
    "ISAE4",   # ISA Energia Brasil (ex-CTEEP)
    "CSMG3",   # Copasa
    "TAEE11",  # Taesa
    "CPFE3",   # CPFL Energia
    "AURE3",   # Auren Energia
    "ENBR3",   # EDP - Energias do Brasil
}

n = len(dates)
sums = [0.0] * n
for r in rows[1:]:
    code = r[3]
    if code in util_tickers:
        for i, v in enumerate(r[4:]):
            if isinstance(v, (int, float)):
                sums[i] += v

fig, ax = plt.subplots(figsize=(11, 5.5))
ax.plot(dates, sums, color="#1f7a4d", linewidth=2)
ax.fill_between(dates, sums, color="#1f7a4d", alpha=0.15)

eventos = {
    "2022-06": "Privatização da\nEletrobras (jun/2022)",
    "2024-07": "Privatização da\nSabesp (jul/2024)",
}
for d, val in zip(dates, sums):
    key = d.strftime("%Y-%m")
    if key in eventos:
        ax.annotate(
            eventos[key],
            xy=(d, val),
            xytext=(0, 25),
            textcoords="offset points",
            ha="center",
            fontsize=9,
            fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="gray"),
        )

ax.set_title("Peso do setor de utilities na carteira do Ibovespa (jun/2021 - jun/2026)")
ax.set_ylabel("% da carteira teórica do Ibovespa")
ax.set_ylim(0, 18)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.grid(axis="y", linestyle="--", alpha=0.4)

ax.text(
    0.0, -0.18,
    "Fonte: dados mensais da carteira teórica do Ibovespa (planilha fornecida pelo usuário).\n"
    "Soma dos pesos de: Axia Energia/Eletrobras, Sabesp, Equatorial, Copel, Cemig, Energisa, Engie Brasil,\n"
    "ISA Energia, Copasa, Taesa, CPFL Energia, Auren e EDP - Energias do Brasil.",
    transform=ax.transAxes, ha="left", fontsize=8, style="italic", color="gray",
)

plt.tight_layout()
plt.savefig("/home/user/investigacao-cnpj/analises/peso-utilities-ibovespa.png", dpi=150)

# imprime resumo para conferência
for d, s in zip(dates, sums):
    print(d.strftime("%Y-%m"), round(s, 2))
