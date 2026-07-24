"""Exemplo 1 — Carregando parquet com backend Arrow e manipulando dtypes.

Conceitos:
- `pd.read_parquet(..., engine="pyarrow", dtype_backend="pyarrow")` faz a leitura
  inteira em cima do Arrow: os dados chegam no DataFrame como `ArrowDtype`
  (`int64[pyarrow]`, `string[pyarrow]`, etc.), evitando a conversão para os tipos
  "clássicos" do numpy (object para string, float64 para inteiros com nulos...).
- Isso é diferente do backend "numpy_nullable" (Int64, string) que também suporta
  nulos, mas ainda guarda os dados em memória no formato numpy por baixo.
- Ler um diretório particionado (`customers/region=.../part-0.parquet`) funciona
  como ler um arquivo único: o pandas delega ao pyarrow.dataset, que reconstrói as
  colunas de partição (aqui, `region`) a partir do caminho.

Rode com: `uv run examples/01_loading_and_dtypes.py`
"""

from _common import load_customers, load_orders, section

if __name__ == "__main__":
    section("Lendo o dataset particionado de customers")
    customers = load_customers()
    print(customers.dtypes)
    print(f"\n{len(customers)} clientes carregados de data/raw/customers/region=*/")

    section("Lendo uma partição de orders (mês 1)")
    orders = load_orders([1])
    print(orders.dtypes)

    section("dtypes ArrowDtype expõem o tipo Arrow subjacente")
    print(orders["order_date"].dtype.pyarrow_dtype)
    print(orders["status"].dtype.pyarrow_dtype)

    section("Convertendo status para category (dtype nativo do pandas)")
    # `.astype("category")` continua funcionando normalmente mesmo com backend Arrow;
    # é útil quando se quer usar operações específicas de Categorical (ordenação,
    # comparação por código) em vez de comparação de strings.
    orders["status_cat"] = orders["status"].astype("category")
    print(orders["status_cat"].cat.categories.tolist())

    section("Convertendo quantity (int32[pyarrow]) para float64[pyarrow]")
    print(orders["quantity"].astype("float64[pyarrow]").head(3))

    section("Extraindo componentes de data com o accessor .dt (funciona sob Arrow)")
    print(orders["order_date"].dt.day.head(3))
    print(orders["order_date"].dt.month.head(3))
