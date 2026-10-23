"""The ordered, fixed transactions clean contract."""

import pyarrow as pa

OUTPUT_SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("account_id", pa.string()),
        ("description", pa.string()),
        ("currency_code", pa.string()),
        ("amount", pa.decimal128(38, 10)),
        ("status", pa.string()),
        ("type", pa.string()),
        ("payer_document_number", pa.string()),
        ("payer_name", pa.string()),
        ("receiver_document_number", pa.string()),
        ("receiver_name", pa.string()),
        ("is_credit_card", pa.bool_()),
        ("date", pa.string()),
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
    ]
)

REQUIRED_INPUT_COLUMNS = {
    "id",
    "account_id",
    "description",
    "description_raw",
    "currency_code",
    "amount",
    "status",
    "type",
    "payment_data",
    "credit_card_metadata",
    "date",
    "created_at",
    "updated_at",
}
