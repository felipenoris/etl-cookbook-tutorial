"""Exemplo 21 — Transações e MVCC: atomicidade, isolamento e conflitos.

O exemplo 07 usa um banco persistente como staging e cita "transações" de
passagem. Aqui elas são o tema: o DuckDB é **ACID** e transacional como um
Postgres — só que embutido no processo, sem servidor. Isso importa em ETL
sempre que um lote precisa entrar **inteiro ou nada** (carregar uma dimensão,
aplicar um conjunto de correções) e enquanto isso os leitores não podem ver
um estado pela metade.

Os comandos e o que este exemplo demonstra, cada um medido:

`BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK`
    Delimitam uma unidade atômica. Sem `BEGIN` explícito, cada statement é sua
    própria transação (autocommit). Dentro de um `BEGIN...COMMIT`, ou tudo é
    aplicado no `COMMIT`, ou nada é, no `ROLLBACK`.

**Atomicidade sob erro**: um erro no meio da transação (violação de constraint,
p.ex.) **aborta** a transação — os statements seguintes falham até você mandar
`ROLLBACK`. Nenhuma escrita parcial sobrevive. É o que garante que um lote com
uma linha ruim não deixe metade dos dados aplicados.

**MVCC / isolamento por snapshot**: o DuckDB usa *Multi-Version Concurrency
Control*. Cada transação enxerga um **snapshot** consistente do banco no
instante em que começou; as escritas não commitadas de outra transação são
**invisíveis** para ela. Leitores nunca bloqueiam escritores e vice-versa —
aqui demonstrado com duas conexões (`con.cursor()`) sobre o mesmo banco.

**Concorrência otimista**: duas transações que alteram a **mesma linha**
colidem — a segunda leva um `TransactionException` ("Conflict on update"). O
DuckDB não usa locks pessimistas de linha; detecta o conflito e aborta uma das
transações, que deve refazer o trabalho.

Rode com: `uv run examples/21_transactions_and_mvcc.py`
"""

import tempfile
from pathlib import Path

import duckdb

from _common import section


def saldos(con) -> list:
    return con.sql("SELECT id, saldo FROM conta ORDER BY id").fetchall()


if __name__ == "__main__":
    workdir = Path(tempfile.mkdtemp(prefix="duckdb_tx_"))
    db_path = workdir / "banco.db"
    con = duckdb.connect(str(db_path))
    con.execute(
        """
        CREATE TABLE conta (id INTEGER PRIMARY KEY, saldo DECIMAL(12, 2));
        INSERT INTO conta VALUES (1, 100.00), (2, 100.00);
        """
    )
    print(f"estado inicial: {saldos(con)}")

    section("1) ROLLBACK: uma transferência inteira é desfeita")
    con.execute("BEGIN TRANSACTION")
    con.execute("UPDATE conta SET saldo = saldo - 30 WHERE id = 1")
    con.execute("UPDATE conta SET saldo = saldo + 30 WHERE id = 2")
    print(f"dentro da transação (ainda não commitada): {saldos(con)}")
    con.execute("ROLLBACK")
    print(f"após ROLLBACK — nada mudou:                 {saldos(con)}")
    print("-> as duas escritas formam uma unidade; o ROLLBACK descarta o lote inteiro.")

    section("2) Atomicidade sob erro: uma linha ruim aborta o lote todo")
    con.execute("BEGIN TRANSACTION")
    con.execute("UPDATE conta SET saldo = saldo - 30 WHERE id = 1")  # escrita 'boa'
    try:
        con.execute("INSERT INTO conta VALUES (1, 0)")  # viola a PRIMARY KEY
    except duckdb.ConstraintException as exc:
        print(f"erro no meio da transação: {str(exc).splitlines()[0]}")
    # a transação está abortada: qualquer statement falha até o ROLLBACK
    try:
        con.execute("SELECT 1")
    except duckdb.TransactionException as exc:
        print(f"a transação ficou abortada: {str(exc).splitlines()[0]}")
    con.execute("ROLLBACK")
    print(f"após ROLLBACK — a escrita 'boa' também sumiu: {saldos(con)}")
    print("-> tudo-ou-nada: um erro não deixa metade do lote aplicado.")

    section("3) MVCC: uma transação não commitada é INVISÍVEL para outra conexão")
    escritor = con.cursor()  # segunda conexão sobre o MESMO banco
    leitor = con.cursor()
    escritor.execute("BEGIN TRANSACTION")
    escritor.execute("UPDATE conta SET saldo = 999.00 WHERE id = 1")
    print(f"o ESCRITOR (na sua transação) vê:      {escritor.sql('SELECT saldo FROM conta WHERE id=1').fetchall()}")
    print(f"o LEITOR (outro snapshot) ainda vê:    {leitor.sql('SELECT saldo FROM conta WHERE id=1').fetchall()}")
    escritor.execute("COMMIT")
    print(f"após o COMMIT do escritor, o LEITOR vê: {leitor.sql('SELECT saldo FROM conta WHERE id=1').fetchall()}")
    print("-> cada transação lê um snapshot consistente; o leitor nunca bloqueia nem")
    print("   enxerga escritas pela metade — só o que já foi commitado.")

    section("4) Concorrência otimista: duas transações na MESMA linha colidem")
    con.execute("UPDATE conta SET saldo = 100.00 WHERE id = 1")  # reset
    t1 = con.cursor()
    t2 = con.cursor()
    t1.execute("BEGIN TRANSACTION")
    t2.execute("BEGIN TRANSACTION")
    t1.execute("UPDATE conta SET saldo = saldo - 10 WHERE id = 1")
    try:
        t2.execute("UPDATE conta SET saldo = saldo - 20 WHERE id = 1")  # mesma linha, t1 aberta
        print("(não deveria chegar aqui)")
    except duckdb.TransactionException as exc:
        print(f"a 2ª transação foi rejeitada: {str(exc).splitlines()[0]}")
        t2.execute("ROLLBACK")
    t1.execute("COMMIT")
    print(f"vence quem commitou; saldo final: {saldos(con)}")
    print("-> sem locks pessimistas: o DuckDB detecta o conflito e aborta uma; a aplicação")
    print("   deve tratar o erro e refazer a transação perdedora.")

    section("Resumo")
    print("- BEGIN/COMMIT/ROLLBACK: agrupam escritas numa unidade atômica (tudo-ou-nada);")
    print("- erro no meio ABORTA a transação — nenhuma escrita parcial sobrevive;")
    print("- MVCC: cada transação lê um snapshot; não-commitado é invisível a outros;")
    print("- concorrência otimista: colisão na mesma linha gera TransactionException.")
    con.close()
    print(f"(banco de exemplo em {db_path} — apague quando quiser)")
