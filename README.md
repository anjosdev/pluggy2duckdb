![pluggy2duckdb](pluggy2duckdb.png)

Fill `.env` and `items.yaml` as shown in `.env.example` and `items.yaml.example`, then run:

```bash
docker run --rm --env-file .env -v ./config:/app/config -v ./data:/app/data -v ./items.yaml:/app/items.yaml:ro pluggy2duckdb pluggy-raw
# or via Docker Compose:
docker compose run --rm raw
```

```bash
docker run --rm -v ./data:/app/data pluggy2duckdb pluggy-silver
# or via Docker Compose:
docker compose run --rm silver
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
