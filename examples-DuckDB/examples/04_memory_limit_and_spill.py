"""Exemplo 4 — Configurando memory_limit e spill para disco (temp_directory).

Numa base transacional, "não caber na memória" raramente é problema do usuário:
o servidor gerencia buffer pool e a query, no pior caso, fica lenta. No DuckDB
embutido, quem dimensiona a memória é você — e a pergunta "e se o dataset for
maior que a RAM?" tem resposta configurável: **spill** (derramar blocos
intermediários para disco), o mecanismo que permite ao DuckDB processar
datasets maiores que a memória disponível.

Comandos usados (todos via `SET`, válidos para a conexão):

`SET memory_limit='150MB'`
    Teto de memória do motor. O default é ~80% da RAM da máquina; aqui
    forçamos um valor bem menor que o dataset de orders (33.7M linhas,
    ~265MB só em parquet comprimido) exatamente para provocar o spill.

`SET temp_directory='...'`
    Onde gravar os blocos que não couberem no teto. Um `ORDER BY` da tabela
    inteira precisa (numa primeira fase) de todas as linhas para ordenar —
    quando isso estoura o `memory_limit`, o DuckDB grava "runs" parciais em
    disco e as intercala no final (external sort, o mesmo algoritmo clássico
    de fita), em vez de falhar com out-of-memory. Joins e agregações grandes
    fazem o análogo com partições de hash.

`SET preserve_insertion_order=false`
    Por default, o DuckDB garante que resultados sem `ORDER BY` saiam na
    ordem dos arquivos de origem — garantia que custa memória e limita o
    paralelismo. Em ETL analítico essa ordem raramente importa; desligá-la
    libera o motor para reordenar/spillar à vontade. (Se a ordem importa,
    a resposta certa é um `ORDER BY` explícito, nunca a ordem implícita.)

`SET threads=4`
    Limita o paralelismo do motor. Cada thread reserva um piso de memória de
    trabalho; com um teto tão baixo, o default (uma thread por núcleo) faz esse
    piso, agregado, estourar o `memory_limit` numa máquina de muitos núcleos —
    e o motor aborta com out-of-memory ANTES de conseguir spillar. Fixar um
    número pequeno de threads mantém a demonstração de spill determinística em
    qualquer máquina, independentemente da contagem de núcleos. Não à toa,
    reduzir as threads é a primeira solução que o próprio DuckDB sugere no erro
    de memória — é outro botão do mesmo tema (dimensionar recursos do motor).

Rode com: `uv run examples/04_memory_limit_and_spill.py`
"""

import shutil
from pathlib import Path

import duckdb

from _common import ORDERS_GLOB, section

TMP_SPILL_DIR = Path(__file__).resolve().parent / "_tmp_spill"


if __name__ == "__main__":
    TMP_SPILL_DIR.mkdir(exist_ok=True)
    con = duckdb.connect()

    section("Configurando um teto de memória bem menor que o dataset")
    con.execute("SET memory_limit='150MB'")
    con.execute(f"SET temp_directory='{TMP_SPILL_DIR}'")
    # Sem ordem de inserção preservada, o DuckDB tem mais liberdade para
    # paralelizar/spillar sem precisar manter a ordem original das linhas.
    con.execute("SET preserve_insertion_order=false")
    # Teto de memória baixo exige limitar as threads: o default (uma por núcleo)
    # faz o piso de memória por thread, somado, estourar os 150MB numa máquina de
    # muitos núcleos, abortando com out-of-memory ANTES de spillar. Um número
    # pequeno e fixo torna a demonstração determinística em qualquer máquina.
    con.execute("SET threads=4")
    print(con.sql("SELECT current_setting('memory_limit'), current_setting('temp_directory')").fetchone())

    section("ORDER BY sobre as ~33.7M linhas de orders (não cabe nos 150MB configurados)")
    query = f"""
        SELECT customer_id, product_id, quantity
        FROM read_parquet('{ORDERS_GLOB}')
        ORDER BY quantity DESC, customer_id
    """
    relation = con.sql(query)
    primeira_linha = relation.fetchone()
    print(f"query concluída sem erro de memória; primeira linha do resultado ordenado: {primeira_linha}")

    section("Arquivos de spill gravados em temp_directory durante a execução")
    spill_files = list(TMP_SPILL_DIR.iterdir())
    if spill_files:
        for f in spill_files:
            print(f"{f.name}: {f.stat().st_size / (1024 * 1024):.1f}MB")
    else:
        print("nenhum arquivo de spill encontrado (a conexão já pode ter liberado os buffers)")

    section("Comparando com memory_limit alto: a mesma query roda só em RAM")
    con.execute("SET memory_limit='4GB'")
    con.sql(query).fetchone()
    print("com memory_limit alto, o DuckDB evita tocar disco sempre que possível")

    con.close()
    shutil.rmtree(TMP_SPILL_DIR, ignore_errors=True)
