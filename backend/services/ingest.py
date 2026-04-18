import argparse
import hashlib
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
from dotenv import load_dotenv
from postgrest.exceptions import APIError
from supabase import create_client

load_dotenv()

PROJECTS_DIR = Path(__file__).resolve().parents[3]
INVENTORY_PATH = PROJECTS_DIR / "POP_InventorySnapshot.xlsx"
SALES_PATH = PROJECTS_DIR / "POP_SalesTransactionHistory.csv"
PO_PATH = PROJECTS_DIR / "POP_PurchaseOrderHistory.XLSX"
CHARGEBACKS_PATH = PROJECTS_DIR / "POP_ChargeBack_Deductions_Penalties_Freight.xlsx"

INVENTORY_SHEET_MAP = {
    "Site 1 - SF": "SF",
    "Site 2 - NJ": "NJ",
    "Site 3 - LA": "LA",
}
LOCNCODE_TO_DC = {"1": "SF", "2": "NJ", "3": "LA"}
SHIP_TO_DC = {"LIVERMORE": "SF", "NEW JERSEY": "NJ", "LOS ANGELES": "LA"}
TRANSFER_ACCOUNT_TO_DC = {
    "SF - COGS - Transfer": "SF",
    "NJ - COGS - Transfer": "NJ",
    "LA - COGS - Transfer": "LA",
}
CHARGEBACK_CAUSE_CODES = {"CRED11-F", "CRED11-O", "CRED08", "CRED12"}
PENALTY_CATEGORIES = (
    "LATE_DELIVERY",
    "EARLY_DELIVERY",
    "SHORT_SHIP",
    "DAMAGED_GOODS",
    "LABELING_ERROR",
    "OTHER",
)
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_CLAUDE_MODEL = os.environ.get(
    "OPENROUTER_CLAUDE_MODEL", "anthropic/claude-sonnet-4"
)
OPENROUTER_CLASSIFICATION_CHUNK_SIZE = 150
HASH_KEYED_TABLES = {
    "sales_history",
    "po_history",
    "chargebacks",
    "transfer_cost_history",
    "penalty_history",
}
ALL_TARGETS = [
    "inventory_snapshots",
    "sales_history",
    "po_history",
    "chargebacks",
    "penalty_history",
    "transfer_cost_history",
    "transfer_cost_lookup",
    "lead_time_lookup",
    "customer_dc_mapping",
]


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")


def _strip_text(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def _null_if_too_long(series: pd.Series, max_length: int) -> pd.Series:
    return series.where(series.str.len().le(max_length) | series.isna(), None)


def _normalize_state(series: pd.Series) -> pd.Series:
    cleaned = _strip_text(series).str.upper()
    return _null_if_too_long(cleaned, 2)


def _to_nullable_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _to_nullable_float(series: pd.Series, decimals: int | None = None) -> pd.Series:
    out = pd.to_numeric(series, errors="coerce")
    if decimals is not None:
        out = out.round(decimals)
    return out


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _to_iso_date(series: pd.Series) -> pd.Series:
    dt = _to_datetime(series)
    out = dt.dt.strftime("%Y-%m-%d")
    return out.where(dt.notna(), None)


def _series_to_hashable_strings(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series.dt.strftime("%Y-%m-%dT%H:%M:%S").where(series.notna(), "")
    return series.astype("string").fillna("")


def _add_source_row_hash(
    df: pd.DataFrame,
    key_columns: Iterable[str] | None = None,
    *,
    dedupe_label: str | None = None,
) -> pd.DataFrame:
    columns = list(key_columns or df.columns)
    hash_input = pd.DataFrame({column: _series_to_hashable_strings(df[column]) for column in columns})
    row_strings = hash_input.agg("\x1f".join, axis=1)
    out = df.copy()
    out["source_row_hash"] = row_strings.map(lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest())
    dup_mask = out["source_row_hash"].duplicated(keep=False)
    duplicate_row_count = int(dup_mask.sum())
    before_len = len(out)
    unique_dup_hashes = int(out.loc[dup_mask, "source_row_hash"].nunique()) if duplicate_row_count else 0
    out = out.drop_duplicates(subset=["source_row_hash"]).copy()
    if duplicate_row_count:
        prefix = f"{dedupe_label}: " if dedupe_label else ""
        print(
            f"{prefix}dedupe: {duplicate_row_count} rows share {unique_dup_hashes} duplicate "
            f"source_row_hash value(s); kept first occurrence per hash ({before_len} -> {len(out)} rows)."
        )
    return out


def _prepare_records(df: pd.DataFrame) -> list[dict]:
    cleaned = df.astype(object).where(pd.notna(df), None)
    return cleaned.to_dict(orient="records")


def _create_client():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def _extract_json_object(raw_text: str) -> str:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if "\n" in text:
            text = text.split("\n", 1)[1]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("Claude response did not contain a JSON object.")
    return text[start : end + 1]


def _chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _openrouter_message_text(body: dict) -> str:
    choices = body.get("choices") or []
    message = choices[0].get("message", {}) if choices else {}
    content = message.get("content", "")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        return "".join(
            str(part.get("text", "")) if isinstance(part, dict) else str(part)
            for part in content
        ).strip()
    return str(content).strip()


def _call_openrouter_for_penalty_chunk(
    descriptions: list[str],
    *,
    api_key: str,
    chunk_index: int,
    total_chunks: int,
) -> dict[str, str]:
    prompt = f"""
You will classify chargeback penalty descriptions into categories.
For each description, return a category from this fixed list:
- LATE_DELIVERY
- EARLY_DELIVERY
- SHORT_SHIP
- DAMAGED_GOODS
- LABELING_ERROR
- OTHER

Respond only with a JSON object mapping each description to its category.

Descriptions:
{json.dumps(descriptions)}
""".strip()

    payload = {
        "model": OPENROUTER_CLAUDE_MODEL,
        "max_tokens": 12000,
        "temperature": 0,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": (
                    "Respond with exactly one valid JSON object and no markdown fences. "
                    "Every input description must appear exactly once as a key. "
                    "Each value must be one of: " + ", ".join(PENALTY_CATEGORIES) + "."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    }
    request = Request(
        OPENROUTER_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": "https://cursor.sh",
            "X-Title": "aabatterysupplychain-ingest",
        },
        method="POST",
    )
    try:
        with urlopen(request) as response:
            body = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError) as exc:
        raise RuntimeError(f"OpenRouter request failed for chunk {chunk_index}/{total_chunks}: {exc}") from exc

    if "error" in body:
        raise RuntimeError(
            f"OpenRouter error for chunk {chunk_index}/{total_chunks}: {body['error']}"
        )

    response_text = _openrouter_message_text(body)
    parsed = json.loads(_extract_json_object(response_text))
    if not isinstance(parsed, dict):
        raise ValueError(
            f"OpenRouter response for chunk {chunk_index}/{total_chunks} was not a JSON object."
        )

    result: dict[str, str] = {}
    invalid = 0
    for description in descriptions:
        category = parsed.get(description)
        if category not in PENALTY_CATEGORIES:
            invalid += 1
            category = "OTHER"
        result[description] = category

    if invalid:
        _log(
            f"chargebacks: chunk {chunk_index}/{total_chunks} returned {invalid} missing/invalid category assignments; defaulted those to OTHER."
        )

    counts = pd.Series(result.values()).value_counts().to_dict()
    _log(
        f"chargebacks: chunk {chunk_index}/{total_chunks} classified {len(descriptions)} descriptions into categories {counts}."
    )
    return result


def _call_claude_for_penalty_categories(descriptions: list[str]) -> dict[str, str]:
    if not descriptions:
        return {}

    _log(f"chargebacks: classifying {len(descriptions)} unique descriptions with OpenRouter Claude.")
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        _log("chargebacks: OPENROUTER_API_KEY not set; defaulting all penalty_category values to OTHER.")
        return {description: "OTHER" for description in descriptions}

    chunks = _chunked(descriptions, OPENROUTER_CLASSIFICATION_CHUNK_SIZE)
    result: dict[str, str] = {}
    try:
        for index, chunk in enumerate(chunks, start=1):
            _log(
                f"chargebacks: sending OpenRouter classification chunk {index}/{len(chunks)} "
                f"with {len(chunk)} descriptions."
            )
            result.update(
                _call_openrouter_for_penalty_chunk(
                    chunk,
                    api_key=api_key,
                    chunk_index=index,
                    total_chunks=len(chunks),
                )
            )
    except Exception as exc:
        _log(
            f"chargebacks: OpenRouter Claude classification failed ({exc}); defaulting missing categories to OTHER."
        )
        return {description: "OTHER" for description in descriptions}

    return result


def _is_duplicate_unique_constraint_error(err: APIError) -> bool:
    message = ""
    if err.args and isinstance(err.args[0], dict):
        message = str(err.args[0].get("message", "") or "")
    if not message:
        message = str(getattr(err, "message", "") or "")
    return message.startswith("duplicate key value violates unique constraint")


def _write_batches(
    table_name: str,
    records: list[dict],
    *,
    conflict_columns: Iterable[str] | None = None,
    batch_size: int = 1000,
) -> None:
    if not records:
        _log(f"{table_name}: no rows to upload.")
        return

    client = _create_client()
    on_conflict = ",".join(conflict_columns) if conflict_columns else None
    conflict_cols = on_conflict.split(",") if on_conflict else []
    _log(
        f"{table_name}: starting upload of {len(records)} records"
        + (f" with upsert key ({on_conflict})" if on_conflict else " with plain inserts")
        + f"; batch_size={batch_size}."
    )

    for start in range(0, len(records), batch_size):
        batch = records[start : start + batch_size]
        _log(f"{table_name}: uploading rows {start}..{start + len(batch) - 1}")
        if on_conflict:
            key_counts: dict[tuple, int] = {}
            for record in batch:
                key = tuple(record.get(column) for column in conflict_cols)
                key_counts[key] = key_counts.get(key, 0) + 1
            dup_keys = sum(1 for _key, count in key_counts.items() if count > 1)
            if dup_keys:
                _log(
                    f"{table_name}: warning: batch at offset {start} contains {dup_keys} "
                    f"duplicate upsert key(s) on ({on_conflict}); same batch may error in Postgres."
                )
        try:
            if on_conflict:
                client.table(table_name).upsert(batch, on_conflict=on_conflict).execute()
            else:
                client.table(table_name).insert(batch).execute()
        except APIError as err:
            if on_conflict and _is_duplicate_unique_constraint_error(err):
                _log(
                    f"{table_name}: duplicate-key conflict while upserting batch at offset {start} "
                    f"on ({on_conflict}); treating as idempotent skip. Detail: {err}"
                )
                continue
            raise

    _log(f"{table_name}: uploaded {len(records)} rows.")


def upload_table(
    table_name: str,
    df: pd.DataFrame,
    *,
    conflict_columns: Iterable[str] | None = None,
) -> None:
    _write_batches(table_name, _prepare_records(df), conflict_columns=conflict_columns)


def insert_table(table_name: str, df: pd.DataFrame) -> None:
    _write_batches(table_name, _prepare_records(df))


def load_inventory_snapshots(path: Path = INVENTORY_PATH) -> pd.DataFrame:
    _log(f"inventory_snapshots: reading workbook {path.name}.")
    sheets = pd.read_excel(path, sheet_name=list(INVENTORY_SHEET_MAP.keys()), dtype=object)
    frames = []
    for sheet_name, dc_value in INVENTORY_SHEET_MAP.items():
        df = sheets[sheet_name].copy()
        _log(f"inventory_snapshots: sheet '{sheet_name}' -> dc={dc_value}, rows={len(df)}.")
        df = df.rename(
            columns={
                "Item Number": "sku_id",
                "Description": "description",
                "Available": "available",
                "On Hand": "on_hand",
            }
        )
        df["sku_id"] = _strip_text(df["sku_id"])
        df["description"] = _strip_text(df["description"])
        df["available"] = _to_nullable_int(df["available"])
        df["on_hand"] = _to_nullable_int(df["on_hand"])
        df["dc"] = dc_value
        df["snapshot_date"] = date.today().isoformat()
        frames.append(df[["sku_id", "description", "available", "on_hand", "dc", "snapshot_date"]])
    out = pd.concat(frames, ignore_index=True)
    _log(f"inventory_snapshots: prepared {len(out)} total rows across {len(frames)} sheets.")
    return out


def load_sales_history(path: Path = SALES_PATH) -> pd.DataFrame:
    _log(f"sales_history: reading CSV {path.name}.")
    df = pd.read_csv(path, parse_dates=["DOCDATE"], dtype={"LOCNCODE": "string"}, low_memory=False)
    _log(f"sales_history: raw rows={len(df)}.")
    df["LOCNCODE"] = _strip_text(df["LOCNCODE"])
    df["SOP TYPE"] = _strip_text(df["SOP TYPE"])
    df = df[df["LOCNCODE"].isin(LOCNCODE_TO_DC) & (df["SOP TYPE"] == "Invoice")].copy()
    _log(f"sales_history: rows after DC + Invoice filters={len(df)}.")

    out = pd.DataFrame(
        {
            "dc": df["LOCNCODE"].map(LOCNCODE_TO_DC),
            "salesperson_id": _strip_text(df["SLPRSNID"]),
            "customer_number": _strip_text(df["CUSTNMBR"]),
            "city": _strip_text(df["CITY"]),
            "state": _normalize_state(df["STATE"]),
            "sop_number": _strip_text(df["SOPNUMBE"]),
            "doc_date": _to_iso_date(df["DOCDATE"]),
            "sku_id": _strip_text(df["ITEMNMBR"]),
            "item_desc": _strip_text(df["ITEMDESC"]),
            "quantity_adj": _to_nullable_int(df["QUANTITY_adj"]),
            "uom": _strip_text(df["UOFM"]),
            "qty_base_uom": _to_nullable_int(df["QTYBSUOM"]),
            "ext_price_adj": _to_nullable_float(df["XTNDPRCE_adj"], 2),
            "ext_cost_adj": _to_nullable_float(df["EXTDCOST_adj"], 2),
            "customer_type": _strip_text(df["Customer Type"]),
            "product_type": _strip_text(df["Product Type"]),
            "gross_profit": _to_nullable_float(df["Gross_Profit_adj"], 2),
            "margin_pct": _to_nullable_float(df["Margin_Pct_adj"], 4),
            "unit_price_adj": _to_nullable_float(df["Unit_Price_adj"], 4),
        }
    )
    out = _add_source_row_hash(out, dedupe_label="sales_history")
    _log(f"sales_history: prepared {len(out)} rows after normalization/dedupe.")
    return out


def _load_po_source(path: Path = PO_PATH) -> pd.DataFrame:
    _log(f"po_history: reading workbook {path.name}.")
    df = pd.read_excel(path, dtype=object)
    _log(f"po_history: raw rows={len(df)}.")
    out = pd.DataFrame(
        {
            "po_number": _to_nullable_int(df["PO Number"]),
            "po_date": _to_datetime(df["PO Date"]),
            "required_date": _to_datetime(df["Required Date"]),
            "promised_ship_date": _to_datetime(df["Promised Ship Date"]),
            "receipt_date": _to_datetime(df["Receipt Date"]),
            "pop_receipt_number": _to_nullable_int(df["POP Receipt Number"]),
            "sku_id": _strip_text(df["Item Number"]),
            "item_description": _strip_text(df["Item Description"]),
            "qty_shipped": _to_nullable_int(df["QTY Shipped"]),
            "qty_invoiced": _to_nullable_int(df["QTY Invoiced"]),
            "unit_cost": _to_nullable_float(df["Unit Cost"], 4),
            "extended_cost": _to_nullable_float(df["Extended Cost"], 2),
            "vendor_id": _strip_text(df["Vendor ID"]),
            "location_code": _to_nullable_int(df["Location Code"]),
            "dc": _strip_text(df["Primary Ship To Address"]).map(SHIP_TO_DC),
            "ship_to_address": _strip_text(df["Primary Ship To Address"]),
            "shipping_method": _strip_text(df["Shipping Method"]),
        }
    )
    out = out[out["dc"].notna()].copy()
    _log(f"po_history: rows after ship-to-address to dc mapping={len(out)}.")
    return out


def load_po_history(path: Path = PO_PATH) -> pd.DataFrame:
    out = _load_po_source(path)

    for column in ["po_date", "required_date", "promised_ship_date", "receipt_date"]:
        out[column] = _to_iso_date(out[column])

    out = _add_source_row_hash(
        out[
            [
                "po_number",
                "po_date",
                "required_date",
                "promised_ship_date",
                "receipt_date",
                "pop_receipt_number",
                "sku_id",
                "item_description",
                "qty_shipped",
                "qty_invoiced",
                "unit_cost",
                "extended_cost",
                "vendor_id",
                "location_code",
                "dc",
                "ship_to_address",
                "shipping_method",
            ]
        ],
        dedupe_label="po_history",
    )
    _log(f"po_history: prepared {len(out)} rows after normalization/dedupe.")
    return out


def load_chargebacks(path: Path = CHARGEBACKS_PATH) -> pd.DataFrame:
    _log(f"chargebacks: reading workbook {path.name} sheet 'Data - Deductions & Cause Code'.")
    df = pd.read_excel(path, sheet_name="Data - Deductions & Cause Code", dtype=object)
    _log(f"chargebacks: raw rows={len(df)}.")
    df["Cause Code"] = _strip_text(df["Cause Code"])
    df = df[df["Cause Code"].isin(CHARGEBACK_CAUSE_CODES)].copy()
    _log(f"chargebacks: rows after operational cause-code filter={len(df)}.")
    descriptions = (
        _strip_text(df["Item Description"]).dropna().drop_duplicates().sort_values().tolist()
    )
    description_map = _call_claude_for_penalty_categories(descriptions)

    out = pd.DataFrame(
        {
            "location_code": _to_nullable_int(df["Location Code"]),
            "salesperson_id": _strip_text(df["Salesperson ID"]),
            "customer_number": _strip_text(df["Customer Number"]),
            "city": _strip_text(df["City from Sales Transaction"]),
            "state": _normalize_state(df["State from Sales Transaction"]),
            "sop_type": _strip_text(df["SOP Type"]),
            "sop_number": _strip_text(df["SOP Number"]),
            "customer_po_number": _strip_text(df["Customer PO Number"]),
            "doc_date": _to_iso_date(df["Document Date"]),
            "cause_code": df["Cause Code"],
            "cause_code_desc": _strip_text(df["Cause Code Desc"]),
            "item_description": _strip_text(df["Item Description"]),
            "penalty_category": _strip_text(df["Item Description"]).map(description_map).fillna("OTHER"),
            "extended_price": _to_nullable_float(df["Extended Price"], 2),
        }
    )
    out = _add_source_row_hash(out, dedupe_label="chargebacks")
    _log(f"chargebacks: prepared {len(out)} rows after classification/normalization/dedupe.")
    return out


def load_transfer_cost_history(path: Path = CHARGEBACKS_PATH) -> pd.DataFrame:
    _log(f"transfer_cost_history: reading workbook {path.name} sheet 'Data-Transfer Cost'.")
    df = pd.read_excel(path, sheet_name="Data-Transfer Cost", dtype=object)
    _log(f"transfer_cost_history: raw rows={len(df)}.")
    out = pd.DataFrame(
        {
            "journal_entry": _to_nullable_int(df["Journal Entry"]),
            "trx_date": _to_iso_date(df["TRX Date"]),
            "account_number": _strip_text(df["Account Number"]),
            "account_description": _strip_text(df["Account Description"]),
            "dc": _strip_text(df["Account Description"]).map(TRANSFER_ACCOUNT_TO_DC),
            "amount": _to_nullable_float(df["Amount"], 2),
            "originating_master_name": _strip_text(df["Originating Master Name"]),
            "reference": _strip_text(df["Reference"]),
        }
    )
    out = _add_source_row_hash(out, dedupe_label="transfer_cost_history")
    _log(f"transfer_cost_history: prepared {len(out)} rows after normalization/dedupe.")
    return out


def load_penalty_history(path: Path = CHARGEBACKS_PATH) -> pd.DataFrame:
    _log(f"penalty_history: reading workbook {path.name} sheet 'Data-Penalty'.")
    df = pd.read_excel(path, sheet_name="Data-Penalty", dtype=object)
    _log(f"penalty_history: raw rows={len(df)}.")
    out = pd.DataFrame(
        {
            "salesperson_id": _strip_text(df["Salesperson ID"]),
            "customer_number": _strip_text(df["Customer Number"]),
            "customer_name": _strip_text(df["Customer Name"]),
            "city": _strip_text(df["City from Sales Transaction"]),
            "state": _normalize_state(df["State from Sales Transaction"]),
            "sop_number": _strip_text(df["SOP Number"]),
            "doc_date": _to_iso_date(df["Document Date"]),
            "sku_id": _strip_text(df["Item Number"]),
            "item_description": _strip_text(df["Item Description"]),
            "qty": _to_nullable_float(df["QTY"], 2),
            "uom": _strip_text(df["U Of M"]),
            "extended_price": _to_nullable_float(df["Extended Price"], 2),
            "market": _strip_text(df["MARKET"]),
        }
    )
    out = _add_source_row_hash(out, dedupe_label="penalty_history")
    _log(f"penalty_history: prepared {len(out)} rows after normalization/dedupe.")
    return out


def derive_transfer_cost_lookup(history_df: pd.DataFrame) -> pd.DataFrame:
    _log(f"transfer_cost_lookup: deriving from {len(history_df)} transfer_cost_history rows.")
    df = history_df.copy()
    df = df[df["dc"].notna() & df["amount"].notna()].copy()
    # Reversal rows can be negative; use cost magnitude for route-level lookup stats.
    df["cost_magnitude"] = df["amount"].abs()

    grouped = (
        df.groupby("dc", as_index=False)["cost_magnitude"]
        .agg(avg_cost="mean", min_cost="min", max_cost="max", sample_size="count")
        .sort_values("dc")
        .rename(columns={"dc": "dest_dc"})
    )
    for column in ["avg_cost", "min_cost", "max_cost"]:
        grouped[column] = grouped[column].round(2)
    grouped["sample_size"] = grouped["sample_size"].astype("Int64")
    grouped = grouped[["dest_dc", "avg_cost", "min_cost", "max_cost", "sample_size"]]
    _log(f"transfer_cost_lookup: prepared {len(grouped)} grouped rows.")
    return grouped


def derive_lead_time_lookup(path: Path = PO_PATH) -> pd.DataFrame:
    _log("lead_time_lookup: deriving from po_history source rows.")
    df = _load_po_source(path)
    df = df[df["po_date"].notna() & df["receipt_date"].notna()].copy()
    df["lead_days"] = (df["receipt_date"] - df["po_date"]).dt.days
    df = df[df["lead_days"].between(0, 200, inclusive="both")].copy()

    grouped = (
        df.groupby("dc", as_index=False)["lead_days"]
        .agg(median_days="median", avg_days="mean", sample_size="count")
        .sort_values("dc")
    )
    grouped["median_days"] = grouped["median_days"].round(1)
    grouped["avg_days"] = grouped["avg_days"].round(1)
    grouped["sample_size"] = grouped["sample_size"].astype("Int64")
    grouped = grouped[["dc", "median_days", "avg_days", "sample_size"]]
    _log(f"lead_time_lookup: prepared {len(grouped)} grouped rows after lead_days filters.")
    return grouped


def derive_customer_dc_mapping(sales_df: pd.DataFrame) -> pd.DataFrame:
    _log(f"customer_dc_mapping: deriving from {len(sales_df)} sales_history rows.")
    counts = (
        sales_df.groupby(["customer_number", "dc"], as_index=False)
        .size()
        .rename(columns={"size": "order_count"})
    )
    winners = (
        counts.sort_values(["customer_number", "order_count", "dc"], ascending=[True, False, True])
        .drop_duplicates(subset=["customer_number"], keep="first")
        .rename(columns={"dc": "primary_dc"})
    )

    customer_type = (
        sales_df.groupby("customer_number")["customer_type"]
        .agg(lambda values: values.dropna().mode().iloc[0] if not values.dropna().empty else None)
        .reset_index()
    )

    out = winners.merge(customer_type, on="customer_number", how="left")
    out["order_count"] = out["order_count"].astype("Int64")
    out = out[["customer_number", "primary_dc", "customer_type", "order_count"]]
    _log(f"customer_dc_mapping: prepared {len(out)} customer mappings.")
    return out


def build_datasets(targets: set[str]) -> dict[str, pd.DataFrame]:
    _log(f"build_datasets: requested targets={sorted(targets)}.")
    datasets: dict[str, pd.DataFrame] = {}

    if "inventory_snapshots" in targets:
        datasets["inventory_snapshots"] = load_inventory_snapshots()

    sales_df: pd.DataFrame | None = None
    if {"sales_history", "customer_dc_mapping"} & targets:
        sales_df = load_sales_history()
        if "sales_history" in targets:
            datasets["sales_history"] = sales_df
        if "customer_dc_mapping" in targets:
            datasets["customer_dc_mapping"] = derive_customer_dc_mapping(sales_df)

    if "chargebacks" in targets:
        datasets["chargebacks"] = load_chargebacks()

    if "penalty_history" in targets:
        datasets["penalty_history"] = load_penalty_history()

    transfer_cost_history_df: pd.DataFrame | None = None
    if {"transfer_cost_history", "transfer_cost_lookup"} & targets:
        transfer_cost_history_df = load_transfer_cost_history()
        if "transfer_cost_history" in targets:
            datasets["transfer_cost_history"] = transfer_cost_history_df
        if "transfer_cost_lookup" in targets:
            datasets["transfer_cost_lookup"] = derive_transfer_cost_lookup(transfer_cost_history_df)

    if {"po_history", "lead_time_lookup"} & targets:
        if "po_history" in targets:
            datasets["po_history"] = load_po_history()
        if "lead_time_lookup" in targets:
            datasets["lead_time_lookup"] = derive_lead_time_lookup()

    return datasets


def _count_open_po_rows(path: Path = PO_PATH) -> int:
    """Match open_po_history view: receipt_date is null or strictly after today."""
    df = _load_po_source(path)
    receipt = df["receipt_date"]
    today = pd.Timestamp(date.today())
    mask = receipt.isna() | (receipt > today)
    return int(mask.sum())


def write_dataset(table_name: str, df: pd.DataFrame) -> None:
    if table_name == "inventory_snapshots":
        upload_table(table_name, df, conflict_columns=["sku_id", "dc", "snapshot_date"])
    elif table_name in HASH_KEYED_TABLES:
        upload_table(table_name, df, conflict_columns=["source_row_hash"])
    elif table_name == "transfer_cost_lookup":
        upload_table(table_name, df, conflict_columns=["dest_dc"])
    elif table_name == "lead_time_lookup":
        upload_table(table_name, df, conflict_columns=["dc"])
    elif table_name == "customer_dc_mapping":
        upload_table(table_name, df, conflict_columns=["customer_number"])
    else:
        insert_table(table_name, df)


def run_targets(targets: Iterable[str], *, dry_run: bool = False) -> None:
    target_set = set(targets)
    _log(f"run_targets: starting for targets={sorted(target_set)} dry_run={dry_run}.")
    datasets = build_datasets(target_set)

    if "po_history" in target_set:
        open_po_count = _count_open_po_rows()
        print(
            "open_po_history is a database view over po_history (not ingested directly). "
            f"Rows that would appear as open today: {open_po_count}."
        )

    for table_name in ALL_TARGETS:
        if table_name not in target_set:
            continue
        df = datasets[table_name]
        _log(f"{table_name}: prepared {len(df)} rows")
        if dry_run:
            continue
        write_dataset(table_name, df)
    _log("run_targets: completed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load POP source files into Supabase tables.")
    parser.add_argument(
        "targets",
        nargs="*",
        choices=ALL_TARGETS + ["all"],
        default=["all"],
        help="Specific tables/lookups to load. Defaults to all targets.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build datasets and print row counts without uploading to Supabase.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = ALL_TARGETS if not args.targets or "all" in args.targets else args.targets
    _log(f"main: resolved targets={targets}.")
    run_targets(targets, dry_run=args.dry_run)


if __name__ == "__main__":
    main()