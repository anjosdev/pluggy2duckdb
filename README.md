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

After `pluggy-silver`, create clean and concise datasets from the silver data.
For transactions, run:

```bash
uv run pluggy-silver-transactions-clean
```

The transactions clean ETL reads `data/silver/transactions.parquet` and writes
`data/silver_clean/transactions_clean.parquet` with Zstandard compression. By default,
it merges on `transaction_id`: incoming rows replace matching rows in full,
new IDs are inserted, and existing IDs absent from the input are retained.
Use overwrite to rebuild from only the input:

```bash
uv run pluggy-silver-transactions-clean \
  --input data/silver/transactions.parquet \
  --output-dir data/silver_clean \
  --mode overwrite
```

Python callers can use
`from pluggy2duckdb.silver_transactions_clean import run_transactions_clean`, then
`run_transactions_clean(input_path, output_dir, mode="merge")` to obtain the output
`Path`. The fixed column order is `transaction_id`, `account_id`, `description`,
`currency_code`, `amount`, `status`, `type`, `payer_document_number`, `payer_name`,
`receiver_document_number`, `receiver_name`, `is_credit_card`, `date`, `created_at`,
`updated_at`. All columns are strings except `amount` (`decimal128(38,10)`) and
`is_credit_card` (boolean). Timestamp casts use UTC; card flags are `true` or
`false`. Description falls back to `description_raw` only for nulls.
Missing or malformed payment JSON yields
null party fields, and string document numbers preserve leading zeros.

The input must include all referenced source columns, even when their values are
null. Null, empty, or whitespace-only transaction IDs abort the run. Duplicate
incoming IDs use the newest original typed `updated_at` (nulls last), then the
full source row serialized as JSON in descending order to break ties. Existing
merge output must match the clean schema and contain unique, nonempty IDs.
Empty input retains existing rows in merge mode and writes a typed empty file in
overwrite mode. Writers use a POSIX advisory lock throughout reading and merging,
then publish a temporary file by atomic replacement; failed runs preserve the
previous output. Each successful run logs input, duplicate, merge, and output
row counts.

Install development dependencies and run the tests with:

```bash
uv sync --extra dev
uv run --extra dev pytest
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
