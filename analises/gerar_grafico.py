import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import date

# Estimativa do peso aproximado do setor de utilities (energia elétrica,
# saneamento e gás) na carteira teórica do Ibovespa, 2021-2026.
# Construída a partir de eventos de mercado documentados (privatizações da
# Eletrobras e da Sabesp, e a entrada do setor entre os 4 maiores do índice
# em 2025) - NÃO são dados oficiais quadrimestrais da B3.
pontos = [
    (date(2021, 1, 1), 5.0, "Início de 2021"),
    (date(2022, 1, 1), 5.5, "Início de 2022"),
    (date(2022, 6, 1), 7.5, "Privatização da Eletrobras (jun/2022)"),
    (date(2023, 1, 1), 7.0, "Início de 2023"),
    (date(2024, 1, 1), 7.5, "Início de 2024"),
    (date(2024, 7, 1), 8.5, "Privatização da Sabesp (jul/2024)"),
    (date(2025, 1, 1), 9.0, "Início de 2025"),
    (date(2025, 9, 1), 10.0, "Utilities vira o 4º maior setor do Ibovespa"),
    (date(2026, 6, 1), 10.0, "Atual (jun/2026)"),
]

datas = [p[0] for p in pontos]
pesos = [p[1] for p in pontos]

fig, ax = plt.subplots(figsize=(10, 5.5))
ax.plot(datas, pesos, marker="o", color="#1f7a4d", linewidth=2)

destaques = {
    date(2022, 6, 1): "Privatização\nEletrobras",
    date(2024, 7, 1): "Privatização\nSabesp",
    date(2025, 9, 1): "4º maior\nsetor do Ibovespa",
}
for d, peso, _ in pontos:
    if d in destaques:
        ax.annotate(
            destaques[d],
            xy=(d, peso),
            xytext=(0, 18),
            textcoords="offset points",
            ha="center",
            fontsize=9,
            fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="gray"),
        )

ax.set_title("Peso estimado das utilities na carteira do Ibovespa (2021-2026)")
ax.set_ylabel("% aproximado da carteira teórica")
ax.set_ylim(0, 12)
ax.xaxis.set_major_locator(mdates.YearLocator())
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax.grid(axis="y", linestyle="--", alpha=0.4)

ax.text(
    0.5, -0.18,
    "Estimativa construída a partir de eventos de mercado (privatizações e notícias setoriais),\n"
    "não a partir das carteiras teóricas quadrimestrais oficiais da B3.",
    transform=ax.transAxes, ha="center", fontsize=8, style="italic", color="gray",
)

plt.tight_layout()
plt.savefig("/home/user/investigacao-cnpj/analises/peso-utilities-ibovespa.png", dpi=150)
print("done")
