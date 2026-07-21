r"""Materialização de dados 1:N no Rust: "isso não vira um ORM de novo?"

Este exemplo responde a uma dúvida legítima que surge ao migrar de um ETL
baseado em ORM para a stack colunar. O raciocínio é: *"o RecordBatch chega
rápido no Rust, mas o meu algoritmo precisa de um `Contrato` com um vetor de
`Parametro` dentro para calcular a rentabilidade. Se eu construo esse grafo
de objetos no Rust, não recriei exatamente o problema do ORM?"*

A resposta curta: **não, se você emprestar fatias em vez de copiar dados** —
e o exemplo mede a diferença entre as três estratégias possíveis.

## Por que o ORM é lento (e o que disso sobrevive no Rust)

A lentidão do ORM não vem de "materializar objetos" em abstrato, mas de cinco
custos distintos. Vale checar quais deles atravessam para o lado Rust:

| Custo do ORM | Sobrevive em Rust? |
|---|---|
| **Metadados por linha em runtime** — cada instância é um `PyObject` com refcount, `__dict__` e rastreamento de GC (centenas de bytes de overhead por objeto) | **Não.** Uma struct Rust é memória pura: sem refcount, sem GC, sem dicionário de atributos |
| **Escrituração do ORM** — identity map, unit of work, atributos instrumentados (todo acesso passa por *descriptors* que registram estado), lazy loading | **Não.** Não existe nada disso; é só dado |
| **Travessia de fronteira por linha** — o flush serializa linha a linha pelo protocolo do banco | **Não.** Os dados já estão na memória do processo; montar a struct é leitura de RAM, não I/O |
| **Execução interpretada** — cada operação é dispatch de bytecode | **Não.** Código de máquina, inlinável e vetorizável |
| **Alocação de heap por linha** | **SIM — é o único que sobrevive, e o que este exemplo mede** |

Quatro dos cinco custos desaparecem só por sair do Python. O quinto — alocar
memória por linha — continua existindo em Rust (~100x mais barato, mas ainda
O(n)), e é evitável. Há ainda um custo extra que não tem relação com GC:
materializar structs converte o layout **colunar (SoA)** em **orientado a
linha (AoS)**, sacrificando localidade de cache e vetorização.

## Como o Arrow representa 1:N (a chave da implementação)

Uma coluna `list<float64>` **não** é um vetor de vetores. São duas peças: um
array PLANO com os valores de todas as linhas concatenados, e um vetor de
*offsets* que marca onde começa cada sublista:

```mermaid
flowchart TB
    subgraph OFF["offsets (n+1 posições)"]
        direction LR
        O["[0, 3, 5, 8]"]
    end
    subgraph VAL["values: array plano e contíguo"]
        direction LR
        V["[t0 t1 t2 | t3 t4 | t5 t6 t7]"]
    end
    OFF -->|"linha 0 = values[0..3]"| L0["contrato 0: 3 parâmetros"]
    OFF -->|"linha 1 = values[3..5]"| L1["contrato 1: 2 parâmetros"]
    OFF -->|"linha 2 = values[5..8]"| L2["contrato 2: 3 parâmetros"]
```

Consequência decisiva: **os parâmetros de cada contrato já são uma fatia
contígua do buffer**. Pegar `&values[offsets[i]..offsets[i+1]]` custa copiar
um ponteiro e um comprimento — não os dados. É isso que um ORM nunca pode
fazer: ele é obrigado a copiar as linhas para dentro dos objetos; o Arrow
permite emprestar uma vista.

## As três estratégias medidas

Todas as três chamam **exatamente o mesmo núcleo de cálculo**
(`project_with_params`, que recebe `&[f64]` e `&[i32]`), então a diferença de
tempo isola só o custo de materialização:

**A) `project_nested_materialized`** — a tradução literal do modelo de ORM:
uma struct `ContratoOwned` com `Vec<f64>`/`Vec<i32>` próprios, preenchidos com
`.to_vec()`. **2 alocações de heap por contrato**, e o grafo inteiro de
objetos existe na memória antes do cálculo começar.

**B) `project_nested_reused`** — ainda copia os dados, mas os dois `Vec` são
criados UMA vez fora do laço; a cada linha faz `clear()` (que zera o
comprimento mas **preserva a capacidade**) + `extend_from_slice()`. As
alocações caem de O(n) para O(1) — o heap só é tocado até o buffer atingir a
maior sublista. É a saída quando o algoritmo precisa mesmo de dados próprios
(porque vai mutá-los, por exemplo).

**C) `project_nested_borrowed`** — **zero alocação, zero cópia**. A struct
`ContratoRef<'a>` guarda `&'a [f64]`/`&'a [i32]` apontando para os buffers
Arrow originais. Você mantém a ergonomia de "um contrato com seu vetor de
parâmetros" para escrever o algoritmo com clareza, sem copiar um byte. O
lifetime `'a` é a garantia — verificada em tempo de compilação — de que a
fatia não sobrevive ao buffer de origem; é a segurança que torna o empréstimo
viável e que uma linguagem gerenciada não tem como oferecer.

## O que esperar do resultado

As três produzem resultados idênticos (o exemplo verifica) — muda só o custo:

| Estratégia | Alocações | Ganho de performance (aprox.) |
|---|---|---|
| **A)** `Vec` próprio por contrato (estilo ORM) | 2 por linha — O(n) | 1x (linha de base) |
| **B)** buffers reaproveitados (`clear()` + refill) | O(1) | **~3x** |
| **C)** fatias emprestadas sobre o `ListArray` | **zero** | **~4x** |

Medição com 1M de contratos (~2,5M de parâmetros no total); os números
absolutos variam com a máquina, mas a ordem entre as estratégias é estável.
Evitar a alocação por linha vale um fator relevante — não é
micro-otimização.

Mas repare na **escala absoluta**: as três processam 1M de contratos em
*dezenas de milissegundos*. É aí que mora a diferença real para o ORM — um
`Vec` por linha em Rust custa ~50ms a mais; um objeto Python por linha
custaria segundos, mais pressão de GC e memória. Os quatro custos caros já
foram eliminados por estarmos em Rust; o que sobra é o quinto, e ele é
mensurável, evitável e benigno.

A lição: **o padrão "não materialize por linha" continua valendo dentro do
Rust** — só que aqui o preço de errar é um fator ~4x em milissegundos, e não
a diferença entre um ETL que roda e um que não termina.

Rode com: ``uv run run_nested_params.py`` (a partir de ``rust-extension``).
"""

from __future__ import annotations

import time

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from etl_rust_ext import (
    project_nested_borrowed,
    project_nested_materialized,
    project_nested_reused,
)

NUM_CONTRATOS = 1_000_000
MAX_PARAMS = 4  # cada contrato tem de 1 a MAX_PARAMS tranches
RNG_SEED = 11
RODADAS = 3


def gerar_contratos_com_parametros(n: int) -> pa.RecordBatch:
    """Monta o RecordBatch com o relacionamento 1:N como colunas ``list<...>``.

    Num cenário real, este batch viria de um JOIN no DuckDB entre a tabela de
    contratos e a de parâmetros, agregando os parâmetros com ``list(...)`` —
    exatamente o que o ``DuckDB/examples/09`` demonstra. Aqui geramos direto
    para o exemplo ser autocontido.
    """
    rng = np.random.default_rng(RNG_SEED)

    # quantos parâmetros cada contrato tem (1..MAX_PARAMS)
    tamanhos = rng.integers(1, MAX_PARAMS + 1, size=n, dtype=np.int32)
    total_params = int(tamanhos.sum())

    # os valores de TODOS os contratos, concatenados (o array "plano" do Arrow)
    taxas_planas = rng.uniform(0.005, 0.02, size=total_params)
    prazos_planos = rng.integers(6, 61, size=total_params, dtype=np.int32)

    # os offsets: soma acumulada dos tamanhos, começando em 0 -> n+1 posições
    offsets = np.zeros(n + 1, dtype=np.int32)
    np.cumsum(tamanhos, out=offsets[1:])

    # ListArray.from_arrays monta a coluna list a partir de (offsets, valores)
    # SEM copiar os valores — é a mesma estrutura descrita no diagrama acima
    taxas = pa.ListArray.from_arrays(pa.array(offsets), pa.array(taxas_planas))
    prazos = pa.ListArray.from_arrays(pa.array(offsets), pa.array(prazos_planos))

    return pa.record_batch(
        {
            "id_contrato": pa.array(np.arange(1, n + 1, dtype=np.int64)),
            "principal": pa.array(rng.uniform(10_000, 500_000, size=n)),
            "parametros_taxa": taxas,
            "parametros_prazo": prazos,
        }
    )


def cronometrar(fn, batch: pa.RecordBatch, rodadas: int = RODADAS) -> tuple[float, pa.Table]:
    """Roda a variante `rodadas` vezes e devolve (melhor tempo, resultado).

    Usa o MELHOR tempo (não a média): em microbenchmarks, o mínimo é a
    estimativa mais estável do custo real, pois elimina o ruído de
    escalonamento do SO e de outras cargas na máquina.
    """
    melhor = float("inf")
    resultado = None
    for _ in range(rodadas):
        inicio = time.perf_counter()
        saida = fn(batch)
        melhor = min(melhor, time.perf_counter() - inicio)
        resultado = saida
    return melhor, pa.Table.from_batches([resultado])


if __name__ == "__main__":
    print(f"Gerando {NUM_CONTRATOS:,} contratos com 1..{MAX_PARAMS} parâmetros cada...")
    batch = gerar_contratos_com_parametros(NUM_CONTRATOS)
    total_params = len(batch.column("parametros_taxa").values)
    print(f"  {total_params:,} parâmetros no total "
          f"(média de {total_params / NUM_CONTRATOS:.1f} por contrato)")
    print(f"  schema 1:N: {batch.schema.field('parametros_taxa').type}\n")

    variantes = [
        ("A) Vec por contrato (estilo ORM)", project_nested_materialized, "2 x n alocações"),
        ("B) buffers reaproveitados", project_nested_reused, "O(1) alocações"),
        ("C) fatias emprestadas", project_nested_borrowed, "ZERO alocações"),
    ]

    resultados = []
    for nome, fn, alocacoes in variantes:
        tempo, tabela = cronometrar(fn, batch)
        resultados.append((nome, tempo, tabela, alocacoes))
        print(f"{nome:36s} {tempo * 1000:7.1f}ms   ({alocacoes})")

    # as três precisam concordar: mesmo núcleo de cálculo, materialização diferente
    print("\n[check] as três variantes produzem resultados idênticos:", end=" ")
    base = resultados[0][2]
    print(all(r[2].equals(base) for r in resultados))

    t_a, t_c = resultados[0][1], resultados[2][1]
    print(f"[placar] A (materializa) {t_a * 1000:.1f}ms vs C (empresta) {t_c * 1000:.1f}ms "
          f"-> {t_a / t_c:.2f}x")
    print(f"\nEvitar a alocação por linha vale ~{t_a / t_c:.0f}x — não é micro-otimização.")
    print(f"Mas repare na ESCALA: as três processam {NUM_CONTRATOS:,} contratos em dezenas")
    print("de ms. Em Python, um objeto por linha custaria segundos + pressão de GC.")
    print("Os 4 custos caros do ORM já sumiram por ser Rust; sobra só a alocação —")
    print("mensurável, evitável e benigna. O padrão 'não materialize por linha' vale aqui também.")

    print("\n[amostra] id_contrato -> receita_projetada:")
    print(base.slice(0, 5).to_pandas().to_string(index=False))
    soma = pc.sum(base["receita_projetada"]).as_py()
    print(f"\nreceita total projetada: {soma:,.2f}")
