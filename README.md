![pluggy2duckdb](pluggy2duckdb.png)

Install the project dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

Fill `.env` and `items.yaml` as shown in `.env.example` and `items.yaml.example`. Then run the raw ingestion pipeline and the silver transformation pipeline from the project root:

```bash
uv run pluggy-raw
uv run pluggy-silver
```

```python
import duckdb

duckdb.sql("SELECT * FROM 'data/silver/transactions.parquet' LIMIT 3").show()
```

| item | account | id | description | description_raw | ... | order | created_at | updated_at |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| nubank | f602c14f... | 097b99a6... | Amazon... | Amazon... | ... | 2 | 2026-09-05T16:26:47.810Z | 2026-09-11T16:32:43.346Z |
| nubank | 584f0c45... | 09c4b436... | Outro | Outro | ... | 1 | 2025-03-11T12:13:58.807Z | 2025-03-11T12:13:59.807Z |
| nubank | f602c14f... | 0a18507d... | Posto de Gasolina... | Posto de Gasolina... | ... | 0 | 2026-05-21T02:28:40.508Z | 2026-05-29T02:35:26.616Z |
