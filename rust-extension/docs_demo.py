r"""Demonstração dos recursos de documentação do **pdoc**.

Este módulo não faz parte do ETL: ele existe para exercitar, num lugar só, os
recursos de composição de documentação que o [pdoc](https://pdoc.dev/) oferece
— fórmulas matemáticas (`--math`), diagramas [mermaid](https://mermaid.js.org/)
(`--mermaid`), inclusão de arquivos markdown (`.. include::`), docstring
dinâmica (conteúdo gerado por execução, na última seção) e os marcadores
usuais de markdown. Compare este HTML gerado com o fonte em `docs_demo.py`
para ver como cada efeito foi obtido.

## Marcadores básicos de markdown

Texto em **negrito**, em *itálico*, em ***negrito e itálico***, `código
inline`, ~~riscado~~ e [links externos](https://pdoc.dev/). Parágrafos são
separados por linha em branco.

Listas com marcadores (aninháveis):

- extração — leitura dos parquet particionados de `data/raw`;
- transformação:
    - join das 3 tabelas no DuckDB;
    - gasto acumulado + tier na extensão Rust;
- carga — escrita particionada em `data/rich`.

Listas numeradas:

1. gerar os dados (`data/generate_data.py --generate`);
2. rodar o pipeline (`uv run run_etl.py`);
3. conferir o resultado em `data/rich/order_metrics/`.

Citação em bloco:

> *"Dados que não viram decisão são só custo de armazenamento."*
> — provérbio apócrifo de engenharia de dados

Bloco de código com syntax highlighting:

```python
from etl_rust_ext import compute_customer_running_spend

enriched = compute_customer_running_spend(batch, threshold_prata=500.0)
```

## Tabelas

| Tier   | Condição (gasto acumulado $S$)  | Default    |
|--------|---------------------------------|------------|
| bronze | $S < t_\text{prata}$            | $S < 500$  |
| prata  | $t_\text{prata} \le S < t_\text{ouro}$ | $500 \le S < 2000$ |
| ouro   | $S \ge t_\text{ouro}$           | $S \ge 2000$ |

## Diagrama mermaid do pipeline

Com a flag `--mermaid`, blocos cercados por ```` ```mermaid ```` viram
diagramas renderizados:

```mermaid
flowchart LR
    A[(data/raw<br/>parquet particionado)] -->|read_parquet + JOIN| B[DuckDB]
    B -->|Arrow Table| C[pyarrow<br/>projeção]
    C -->|RecordBatch<br/>zero-copy| D[Rust<br/>pyo3-arrow]
    D -->|cumulative_spend<br/>customer_tier| E[pandas<br/>resumo]
    D --> F[(data/rich<br/>order_metrics)]
```

Outros tipos de diagrama também funcionam, como sequência:

```mermaid
sequenceDiagram
    participant Py as Python (pyarrow)
    participant Rs as Rust (pyo3-arrow)
    Py->>Rs: RecordBatch via __arrow_c_array__ (sem cópia)
    Rs->>Rs: loop sequencial com HashMap
    Rs-->>Py: RecordBatch + 2 colunas novas
```

### Gráficos de barras e de linha (`xychart-beta`)

O mermaid também plota gráficos de dados com o tipo `xychart-beta`. Barras —
por exemplo, a receita por categoria de produto (valores ilustrativos):

```mermaid
xychart-beta
    title "Receita por categoria (R$ milhões, ilustrativo)"
    x-axis [eletronicos, casa, vestuario, livros, alimentos]
    y-axis "Receita (R$ mi)" 0 --> 12
    bar [10.8, 9.4, 8.7, 7.9, 7.2]
```

E linha, para séries históricas — a receita mês a mês das 6 partições de
`orders`:

```mermaid
xychart-beta
    title "Receita mensal 2025 (R$ milhões, ilustrativo)"
    x-axis [jan, fev, mar, abr, mai, jun]
    y-axis "Receita (R$ mi)" 6.5 --> 8
    line [7.1, 7.3, 6.9, 7.6, 7.4, 7.7]
```

Os dois tipos podem ser sobrepostos no mesmo gráfico (barra + linha juntas,
útil para comparar valor mensal com meta ou média):

```mermaid
xychart-beta
    title "Receita mensal vs. meta (R$ milhões, ilustrativo)"
    x-axis [jan, fev, mar, abr, mai, jun]
    y-axis "Receita (R$ mi)" 6 --> 8.5
    bar [7.1, 7.3, 6.9, 7.6, 7.4, 7.7]
    line [7.2, 7.2, 7.2, 7.5, 7.5, 7.5]
```

## Fórmulas matemáticas

Com a flag `--math`, fórmulas inline usam um cifrão — como $S_c(n)$ ou
$O(n \log n)$ — e fórmulas de destaque usam dois:

$$S_c(n) = \sum_{k=1}^{n} a_{c,k}$$

Veja as docstrings das funções abaixo para mais exemplos. Importante: use
docstrings *raw* (prefixo `r` antes das aspas triplas) para que os `\` do
LaTeX não sejam interpretados como escapes do Python.

## Inclusão de arquivo markdown

Tudo a partir da linha horizontal abaixo vem de `docs_includes/glossario.md`,
puxado com a diretiva reStructuredText `.. include::` (caminho relativo ao
arquivo deste módulo):

---

.. include:: docs_includes/glossario.md
"""

from __future__ import annotations


def juros_compostos(principal: float, taxa: float, periodos: int) -> float:
    r"""Calcula o montante final sob juros compostos.

    A fórmula clássica, aqui em *display math* (delimitada por dois cifrões):

    $$M = C \, (1 + i)^n$$

    onde $C$ é o `principal`, $i$ a `taxa` por período e $n$ o número de
    `periodos`. A taxa equivalente para outra periodicidade é
    $i_\text{eq} = (1 + i)^{m} - 1$ (exemplo de fórmula *inline* com
    subscrito e sobrescrito).

    Args:
        principal: capital inicial $C$ (em unidades monetárias).
        taxa: taxa de juros $i$ por período, em fração (0.01 = 1%).
        periodos: número de períodos $n$ de capitalização.

    Returns:
        O montante $M$ após $n$ períodos.

    Exemplo (bloco doctest — o pdoc renderiza `>>>` como código, e
    `python -m doctest` consegue executá-lo):

    >>> round(juros_compostos(1000.0, 0.01, 12), 2)
    1126.83
    """
    return principal * (1.0 + taxa) ** periodos


def media_movel(valores: list[float], janela: int) -> list[float]:
    r"""Calcula a média móvel simples de uma série.

    Para cada posição $t \ge j - 1$ (com janela $j$), o valor de saída é

    $$\bar{x}_t = \frac{1}{j} \sum_{k=t-j+1}^{t} x_k$$

    e as $j - 1$ primeiras posições não têm valor definido (aqui, devolvidas
    como a média parcial dos elementos disponíveis, escolha comum em
    dashboards — *não* a única possível):

    | Estratégia p/ início da série | Efeito                          |
    |-------------------------------|---------------------------------|
    | média parcial (esta função)   | série de saída do mesmo tamanho |
    | descartar posições            | saída encurtada em $j-1$ itens  |
    | preencher com `NaN`           | tamanho igual, com buracos      |

    Args:
        valores: série de entrada $x_1, \dots, x_n$.
        janela: tamanho $j$ da janela ($j \ge 1$).

    Returns:
        Lista com as médias móveis, do mesmo tamanho da entrada.
    """
    if janela < 1:
        raise ValueError(f"janela deve ser >= 1, recebi {janela}")
    saida: list[float] = []
    for t in range(len(valores)):
        inicio = max(0, t - janela + 1)
        trecho = valores[inicio : t + 1]
        saida.append(sum(trecho) / len(trecho))
    return saida


# ---------------------------------------------------------------------------
# Docstring dinâmica: o pdoc IMPORTA o módulo para documentá-lo, então código
# de nível de módulo roda no momento da geração — e docstrings são só strings.
# Abaixo, o __doc__ ganha uma seção construída executando juros_compostos de
# verdade; se a implementação mudar, a tabela da documentação muda junto.


def _tabela_de_juros(principal: float, taxa: float, periodos: list[int]) -> str:
    r"""Monta, em markdown, a tabela de montantes usada na seção "Docstring dinâmica".

    Função privada (prefixo `_`): não aparece na documentação gerada, mas é
    executada durante o import para produzir o conteúdo da última seção do
    `__doc__` do módulo.
    """
    # o "%" fica fora dos cifrões de propósito: em LaTeX ele inicia comentário
    linhas = [
        "| $n$ (períodos) | Montante $M$ de $C = {:.2f}$ a $i$ = {:.1%} |".format(principal, taxa),
        "|---:|---:|",
    ]
    for n in periodos:
        linhas.append(f"| {n} | {juros_compostos(principal, taxa, n):.2f} |")
    return "\n".join(linhas)


__doc__ += f"""

---

## Docstring dinâmica (conteúdo gerado por execução)

O `>>>` dos doctests é **estático** — o pdoc não executa nada ao renderizar
(quem executa doctests, para *conferir* se a saída documentada ainda bate, é
`python -m doctest`). Mas como o pdoc *importa* o módulo para documentá-lo,
uma docstring pode ser construída em tempo de import: `__doc__` é uma string
como outra qualquer.

Esta seção inteira foi anexada ao `__doc__` por uma f-string no fim de
`docs_demo.py`. A tabela abaixo é o **resultado real** de chamar
`juros_compostos(1000.0, 0.01, n)` no momento da geração da documentação —
se a implementação da função mudar, a tabela muda junto, sem risco de
desatualizar:

{_tabela_de_juros(1000.0, 0.01, [1, 6, 12, 24, 60])}

Dois cuidados com essa técnica:

- o código roda a **cada import** do módulo (não só no pdoc) — precisa ser
  barato e sem efeitos colaterais;
- o *View Source* do pdoc mostra o código-fonte (o template da f-string),
  não o texto final — o leitor que compara fonte e HTML pode estranhar.
"""
