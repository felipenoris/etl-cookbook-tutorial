"""Testes de contrato do exemplo 21 (transações e MVCC).

Valida atomicidade (ROLLBACK e abort sob erro), isolamento por snapshot entre
conexões e a concorrência otimista (conflito na mesma linha).
"""

import duckdb
import pytest


@pytest.fixture
def con(tmp_path):
    connection = duckdb.connect(str(tmp_path / "banco.db"))
    connection.execute(
        """
        CREATE TABLE conta (id INTEGER PRIMARY KEY, saldo DECIMAL(12, 2));
        INSERT INTO conta VALUES (1, 100.00), (2, 100.00);
        """
    )
    yield connection
    connection.close()


def saldo(con, id_):
    return con.sql(f"SELECT saldo FROM conta WHERE id = {id_}").fetchone()[0]


def test_rollback_discards_the_whole_batch(con):
    con.execute("BEGIN TRANSACTION")
    con.execute("UPDATE conta SET saldo = saldo - 30 WHERE id = 1")
    con.execute("UPDATE conta SET saldo = saldo + 30 WHERE id = 2")
    con.execute("ROLLBACK")
    assert saldo(con, 1) == 100 and saldo(con, 2) == 100


def test_error_aborts_transaction_and_no_partial_write_survives(con):
    con.execute("BEGIN TRANSACTION")
    con.execute("UPDATE conta SET saldo = saldo - 30 WHERE id = 1")  # escrita 'boa'
    with pytest.raises(duckdb.ConstraintException):
        con.execute("INSERT INTO conta VALUES (1, 0)")  # viola a PK
    # a transação fica abortada: qualquer statement falha até o ROLLBACK
    with pytest.raises(duckdb.TransactionException):
        con.execute("SELECT 1")
    con.execute("ROLLBACK")
    assert saldo(con, 1) == 100  # a escrita 'boa' também foi desfeita


def test_uncommitted_write_is_invisible_to_another_connection(con):
    escritor = con.cursor()
    leitor = con.cursor()
    escritor.execute("BEGIN TRANSACTION")
    escritor.execute("UPDATE conta SET saldo = 999.00 WHERE id = 1")
    assert saldo(escritor, 1) == 999           # o escritor vê a própria mudança
    assert saldo(leitor, 1) == 100             # o leitor ainda vê o snapshot antigo
    escritor.execute("COMMIT")
    assert saldo(leitor, 1) == 999             # após o commit, passa a ver


def test_optimistic_concurrency_conflict_on_same_row(con):
    t1 = con.cursor()
    t2 = con.cursor()
    t1.execute("BEGIN TRANSACTION")
    t2.execute("BEGIN TRANSACTION")
    t1.execute("UPDATE conta SET saldo = saldo - 10 WHERE id = 1")
    with pytest.raises(duckdb.TransactionException):
        t2.execute("UPDATE conta SET saldo = saldo - 20 WHERE id = 1")  # mesma linha
    t2.execute("ROLLBACK")
    t1.execute("COMMIT")
    assert saldo(con, 1) == 90  # venceu quem commitou
