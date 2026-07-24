"""Exemplo 13 — Lendo parquet direto de buckets S3 públicos (httpfs).

**Este exemplo exige acesso à internet** (baixa ~2MB no total). Nos testes,
ele é marcado com `network` — pule com `uv run pytest --no-network`.

O mesmo `read_parquet` dos exemplos locais aceita URLs remotas — a extensão
**httpfs** é autocarregada quando o DuckDB encontra `https://` ou `s3://` no
caminho. O ponto central para ETL: o DuckDB **não baixa o arquivo inteiro**.
Ele usa *range requests* (HTTP `Range: bytes=...`) para ler só o que precisa:

1. primeiro o *footer* do parquet (schema + metadados dos row groups);
2. depois, apenas os bytes das COLUNAS projetadas nos ROW GROUPS que passam
   pelos filtros (zonemaps/pruning — exemplo 12).

Por isso o layout estudado no exemplo 12 (particionamento + ordenação na
escrita) vale ouro no S3: cada row group pulado é uma requisição HTTP — e
seus bytes de egress — que não acontecem.

Esquemas de URL:

`https://...` (arquivos públicos servidos por HTTP/CDN)
    Funciona sem configuração nenhuma.

`s3://bucket/caminho/*.parquet` (API S3: buckets públicos ou privados)
    Precisa de um "secret" dizendo como autenticar. Para bucket público com
    acesso anônimo, um secret VAZIO com a região basta:
    `CREATE SECRET (TYPE s3, PROVIDER config, REGION 'us-east-1')`.
    Para buckets privados, troque por `PROVIDER credential_chain` — o DuckDB
    usa a cadeia padrão da AWS (variáveis de ambiente, ~/.aws, IAM role),
    sem credencial hardcoded no código.
    Bônus: globs e `hive_partitioning=true` funcionam no S3 igualzinho ao
    disco local (o exemplo usa um bucket público REALMENTE particionado).

Bases usadas (ambas pequenas e estáveis):
- `blobs.duckdb.org` — estações (29KB) e serviços de trem (1.6MB) da malha
  ferroviária holandesa, mantidas pela DuckDB Labs para documentação;
- `s3://noaa-ghcn-pds` — clima histórico da NOAA (AWS Open Data), parquet
  particionado por `YEAR=`/`ELEMENT=`; o ano de 1763 tem poucos KB.

Rode com: `uv run examples/13_reading_public_s3.py`
"""

import duckdb

from _common import section

STATIONS_URL = "https://blobs.duckdb.org/stations.parquet"
SERVICES_URL = "https://blobs.duckdb.org/train_services.parquet"
NOAA_GLOB = "s3://noaa-ghcn-pds/parquet/by_year/YEAR=1763/*/*.parquet"

if __name__ == "__main__":
    con = duckdb.connect()

    section("DESCRIBE remoto: só o footer do parquet atravessa a rede")
    con.sql(f"DESCRIBE SELECT * FROM read_parquet('{STATIONS_URL}')").show(max_rows=6)

    section("Agregação remota: projeção + filtro decidem quais bytes baixar")
    con.sql(
        f"""
        SELECT country, COUNT(*) AS estacoes
        FROM read_parquet('{STATIONS_URL}')
        GROUP BY country ORDER BY estacoes DESC LIMIT 5
        """
    ).show()

    section("JOIN entre dois parquets remotos (estações x serviços de trem)")
    con.sql(
        f"""
        SELECT s.name_long AS estacao, COUNT(*) AS partidas
        FROM read_parquet('{SERVICES_URL}') t
        JOIN read_parquet('{STATIONS_URL}') s ON t.station_code = s.code
        GROUP BY estacao ORDER BY partidas DESC LIMIT 5
        """
    ).show()

    section("Esquema s3:// com acesso anônimo: secret de configuração + região")
    con.execute("CREATE SECRET aws_publico (TYPE s3, PROVIDER config, REGION 'us-east-1')")
    con.sql(
        f"""
        SELECT ELEMENT, COUNT(*) AS medicoes,
               ROUND(AVG(DATA_VALUE) / 10.0, 1) AS media  -- decimos de °C/mm
        FROM read_parquet('{NOAA_GLOB}', hive_partitioning=true)
        GROUP BY ELEMENT ORDER BY medicoes DESC
        """
    ).show()
    print("(clima medido em 1763, direto de um bucket S3 público particionado)")

    section("Uma view sobre a URL esconde o 'remoto' do resto do pipeline")
    con.execute(f"CREATE VIEW estacoes AS SELECT * FROM read_parquet('{STATIONS_URL}')")
    total = con.sql("SELECT COUNT(*) FROM estacoes WHERE type ILIKE '%intercity%'").fetchone()
    print(f"estações intercity: {total[0]} — o consumidor da view nem sabe que é S3")
