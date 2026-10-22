"""Executable endpoint support; OpenAPI discovery alone does not enable a route."""

from pluggy2duckdb.models import EndpointSpec

_SPECS = [
    EndpointSpec("/items/{id}", "item", "items"),
    EndpointSpec("/items/{id}/resources", "item", "resources", "page"),
    EndpointSpec("/items/{id}/scr", "item", "scr"),
    EndpointSpec("/consents", "item", "consents", query_key="itemId"),
    EndpointSpec("/identity", "item", "identities", query_key="itemId"),
    EndpointSpec("/loans", "item", "loans", query_key="itemId"),
    EndpointSpec("/accounts", "item", "accounts", query_key="itemId"),
    EndpointSpec("/investments", "item", "investments", "page", query_key="itemId"),
    EndpointSpec("/connectors/{id}", "connector", "connectors", parent="/items/{id}"),
    EndpointSpec("/accounts/{id}", "account", "account_details", parent="/accounts"),
    EndpointSpec(
        "/accounts/{id}/statements", "account", "statements", parent="/accounts"
    ),
    EndpointSpec("/accounts/{id}/balance", "account", "balances", parent="/accounts"),
    EndpointSpec(
        "/v2/transactions",
        "account",
        "transactions",
        "cursor",
        "/accounts",
        "accountId",
    ),
    EndpointSpec(
        "/transactions", "account", "transactions", "page", "/accounts", "accountId"
    ),
    EndpointSpec("/bills", "account", "bills", "page", "/accounts", "accountId"),
    EndpointSpec(
        "/investments/{id}", "investment", "investment_details", parent="/investments"
    ),
    EndpointSpec(
        "/investments/{id}/transactions",
        "investment",
        "investment_transactions",
        "page",
        "/investments",
    ),
    EndpointSpec("/consents/{id}", "consent", "consent_details", parent="/consents"),
    EndpointSpec("/identity/{id}", "identity", "identity_details", parent="/identity"),
    EndpointSpec("/loans/{id}", "loan", "loan_details", parent="/loans"),
    EndpointSpec("/connectors", "global", "connectors"),
    EndpointSpec("/v2/items", "global", "items", "cursor"),
    EndpointSpec("/categories", "global", "categories"),
    EndpointSpec("/categories/rules", "global", "category_rules"),
    EndpointSpec("/webhooks", "global", "webhooks"),
]
ENDPOINTS = {spec.key: spec for spec in _SPECS}


def resolve_endpoints(active: dict) -> list[EndpointSpec]:
    unsupported = set(active) - ENDPOINTS.keys()
    if unsupported:
        raise ValueError(
            f"Enabled endpoints are not implemented: {sorted(unsupported)}"
        )
    if "GET /transactions" in active and "GET /v2/transactions" in active:
        raise ValueError("Enable only one transactions endpoint")
    return [spec for key, spec in ENDPOINTS.items() if key in active]
