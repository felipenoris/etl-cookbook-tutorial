"""Exemplo 20 — Window functions além do básico: navegação, quartis e frames.

O exemplo 03 apresentou o essencial de window functions: `ROW_NUMBER`,
`QUALIFY` e uma média móvel simples com `ROWS BETWEEN 2 PRECEDING AND CURRENT
ROW`. Este exemplo aprofunda os quatro recursos que aparecem em quase todo
relatório analítico e costumam confundir:

- **Funções de navegação** `LAG`/`LEAD`: olham a linha anterior/seguinte
  DENTRO da janela — variação período a período, distância ao vizinho num
  ranking, sem self-join.
- **`NTILE(n)`**: divide as linhas ordenadas em `n` baldes de tamanho quase
  igual — quartis, decis, percentis de negócio.
- **A diferença `ROWS` vs `RANGE`** na cláusula de frame: `ROWS` conta linhas
  FÍSICAS; `RANGE` inclui todos os **pares com o mesmo valor** de `ORDER BY`
  (os *peers*). Só diverge quando há empates — e aí a diferença é silenciosa e
  perigosa.
- **`FIRST_VALUE`/`LAST_VALUE`** e a **pegadinha do frame padrão**: o frame
  default vai até a *linha atual*, então `LAST_VALUE` ingênuo devolve a linha
  corrente, não a última da partição. Corrige-se abrindo o frame para
  `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Uma window function NÃO reduz linhas (ao contrário do `GROUP BY`): ela anexa um
valor calculado sobre uma janela a cada linha. Por isso não se pode agrupar por
cima de uma window na mesma consulta (dá erro de binder) — encapsule numa
subquery/CTE, como abaixo.

Rode com: `uv run examples/20_window_functions_advanced.py`
"""

import duckdb

from _common import ORDERS_GLOB, PRODUCTS_GLOB, section

if __name__ == "__main__":
    con = duckdb.connect()
    # receita por produto e por categoria: a base dos exemplos de navegação e quartil
    con.execute(
        f"""
        CREATE VIEW receita_produto AS
        SELECT p.category AS categoria,
               p.product_name AS produto,
               SUM(o.quantity * p.unit_price)::DECIMAL(18, 2) AS receita
        FROM read_parquet('{ORDERS_GLOB}', hive_partitioning=true) o
        JOIN read_parquet('{PRODUCTS_GLOB}') p ON o.product_id = p.product_id
        GROUP BY categoria, produto;
        """
    )

    section("LAG/LEAD sobre um RANKING: distância ao concorrente de cima e de baixo")
    # sem self-join: LEAD olha a próxima categoria (menor receita), LAG a anterior
    con.sql(
        """
        WITH cat AS (
            SELECT categoria, SUM(receita)::DECIMAL(18, 2) AS receita
            FROM receita_produto GROUP BY categoria
        )
        SELECT categoria, receita,
               receita - LEAD(receita) OVER (ORDER BY receita DESC) AS vantagem_sobre_o_proximo,
               LAG(receita) OVER (ORDER BY receita DESC) - receita AS distancia_do_anterior
        FROM cat ORDER BY receita DESC
        """
    ).show()
    print("-> LEAD/LAG acessam a linha vizinha na ordem da janela; o líder não tem")
    print("   'anterior' (LAG = NULL) e o lanterna não tem 'próximo' (LEAD = NULL).")

    section("NTILE(4): dividindo os 200 produtos em quartis de receita")
    con.sql(
        """
        WITH q AS (
            SELECT produto, receita, NTILE(4) OVER (ORDER BY receita DESC) AS quartil
            FROM receita_produto
        )
        SELECT quartil,
               COUNT(*) AS produtos,
               MIN(receita) AS receita_min,
               MAX(receita) AS receita_max
        FROM q GROUP BY quartil ORDER BY quartil
        """
    ).show()
    print("-> NTILE reparte em n baldes de tamanho ~igual (50 e 50 aqui); o quartil 1")
    print("   concentra os campeões de receita. Trocar por NTILE(10) daria decis.")

    section("FIRST_VALUE + share da partição: o líder de cada categoria e o peso de cada produto")
    con.sql(
        """
        SELECT categoria, produto, receita,
               FIRST_VALUE(produto) OVER (PARTITION BY categoria ORDER BY receita DESC) AS lider_da_categoria,
               ROUND(100.0 * receita / SUM(receita) OVER (PARTITION BY categoria), 1) AS pct_da_categoria
        FROM receita_produto
        QUALIFY ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY receita DESC) <= 2
        ORDER BY categoria, receita DESC
        """
    ).show(max_width=110)
    print("-> FIRST_VALUE repete o topo da partição em cada linha; SUM(...) OVER (PARTITION BY)")
    print("   dá o total da categoria SEM colapsar as linhas — daí o % de participação.")

    # ------------------------------------------------------------------
    # Os dois pontos que exigem controle de empates: dados sintéticos claros
    # ------------------------------------------------------------------
    section("ROWS vs RANGE (controle): a diferença aparece SÓ com empates no ORDER BY")
    con.sql(
        """
        WITH vendas(dia, valor) AS (
            VALUES (1, 10), (1, 20), (2, 5), (3, 7), (3, 8)
        )
        SELECT dia, valor,
               SUM(valor) OVER (ORDER BY dia ROWS  BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum_por_linha,
               SUM(valor) OVER (ORDER BY dia RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS acum_por_faixa
        FROM vendas ORDER BY dia, valor
        """
    ).show()
    print("-> nas linhas de 'dia' repetido, ROWS soma até a linha física corrente, enquanto")
    print("   RANGE inclui TODOS os pares do mesmo dia (peers) — dia 1: 30 e 30; dia 3: 42 e 50.")
    print("   RANGE é o default quando se omite o frame; para acumulado linha a linha use ROWS.")

    section("Pegadinha do LAST_VALUE (controle): o frame padrão para na linha atual")
    con.sql(
        """
        WITH t(grupo, v) AS (VALUES ('a', 1), ('a', 2), ('a', 3))
        SELECT grupo, v,
               LAST_VALUE(v) OVER (PARTITION BY grupo ORDER BY v) AS ingenuo,
               LAST_VALUE(v) OVER (
                   PARTITION BY grupo ORDER BY v
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ) AS com_frame_completo
        FROM t ORDER BY v
        """
    ).show()
    print("-> o LAST_VALUE 'ingênuo' devolve a PRÓPRIA linha (1,2,3): o frame default é")
    print("   'até a linha atual'. Para o último da partição, abra o frame até UNBOUNDED")
    print("   FOLLOWING. (FIRST_VALUE não sofre disso: o início do frame já é o começo.)")

    section("Resumo")
    print("- LAG/LEAD: navegação relativa sem self-join (variação, distância ao vizinho);")
    print("- NTILE(n): baldes de tamanho igual (quartis/decis);")
    print("- ROWS conta linhas; RANGE inclui os peers do mesmo valor de ORDER BY — cuidado com empates;")
    print("- LAST_VALUE precisa de frame explícito até UNBOUNDED FOLLOWING para achar o último.")
