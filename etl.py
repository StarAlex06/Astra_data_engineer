"""Load the building-materials CSV sources into a normalized SQLite warehouse.

The loader is deliberately dependency-free: it uses Python's CSV, hashing and
SQLite libraries, so it can run locally without a service or package install.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any, Iterable


TRANSACTION_COLUMNS = (
    "date", "year", "month", "week_of_year", "region", "product_category",
    "sku", "channel", "customer_type", "units", "unit_price", "revenue",
    "housing_starts_index", "lumber_price_index", "mortgage_rate",
)
MACRO_COLUMNS = ("week", "housing_starts_index", "lumber_price_index", "mortgage_rate", "season_factor")
NUMERIC_COLUMNS = {
    "year": int, "month": int, "week_of_year": int, "units": int,
    "unit_price": float, "revenue": float, "housing_starts_index": float,
    "lumber_price_index": float, "mortgage_rate": float,
}
MACRO_NUMERIC = {"housing_starts_index": float, "lumber_price_index": float, "mortgage_rate": float, "season_factor": float}


def _as_num(value: str, converter: type) -> int | float:
    """Parse a numeric value and reject NaN or infinity for floating point fields."""
    result = converter(value)
    if isinstance(result, float) and not math.isfinite(result):
        raise ValueError("must be a finite number")
    return result


def _date(value: str) -> date:
    """Parse an ISO date; transaction weeks and macro weeks must be Mondays."""
    parsed = date.fromisoformat(value)
    if parsed.weekday() != 0:
        raise ValueError("week date must be a Monday")
    return parsed


def _hash(record: dict[str, Any]) -> str:
    """Return a stable SHA-256 fingerprint for a normalized source record."""
    encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _rows(path: Path, expected: Iterable[str], kind: str):
    """Yield CSV rows with line numbers and reject files with unexpected headers."""
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if tuple(reader.fieldnames or ()) != tuple(expected):
            raise ValueError(f"{path}: expected columns {tuple(expected)}, got {reader.fieldnames}")
        for line_no, row in enumerate(reader, start=2):
            yield line_no, row


def _connect(db_path: Path) -> sqlite3.Connection:
    """Open SQLite with foreign keys enabled and create the warehouse schema."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS dim_date (date_key TEXT PRIMARY KEY, year INTEGER NOT NULL, month INTEGER NOT NULL, week_of_year INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS dim_region (region_id INTEGER PRIMARY KEY, region TEXT NOT NULL UNIQUE);
        CREATE TABLE IF NOT EXISTS dim_product (product_id INTEGER PRIMARY KEY, sku TEXT NOT NULL UNIQUE, product_category TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS dim_channel (channel_id INTEGER PRIMARY KEY, channel TEXT NOT NULL UNIQUE);
        CREATE TABLE IF NOT EXISTS dim_customer_type (customer_type_id INTEGER PRIMARY KEY, customer_type TEXT NOT NULL UNIQUE);
        CREATE TABLE IF NOT EXISTS macro_weekly (week TEXT PRIMARY KEY REFERENCES dim_date(date_key), housing_starts_index REAL NOT NULL CHECK(housing_starts_index >= 0), lumber_price_index REAL NOT NULL CHECK(lumber_price_index >= 0), mortgage_rate REAL NOT NULL CHECK(mortgage_rate >= 0), season_factor REAL CHECK(season_factor >= 0));
        CREATE TABLE IF NOT EXISTS fact_transactions (
            transaction_key TEXT PRIMARY KEY, date_key TEXT NOT NULL REFERENCES dim_date(date_key),
            region_id INTEGER NOT NULL REFERENCES dim_region(region_id), product_id INTEGER NOT NULL REFERENCES dim_product(product_id),
            channel_id INTEGER NOT NULL REFERENCES dim_channel(channel_id), customer_type_id INTEGER NOT NULL REFERENCES dim_customer_type(customer_type_id),
            units INTEGER NOT NULL CHECK(units >= 0), unit_price REAL NOT NULL CHECK(unit_price >= 0), revenue REAL NOT NULL CHECK(revenue >= 0),
            housing_starts_index REAL NOT NULL CHECK(housing_starts_index >= 0), lumber_price_index REAL NOT NULL CHECK(lumber_price_index >= 0), mortgage_rate REAL NOT NULL CHECK(mortgage_rate >= 0),
            source_file TEXT NOT NULL, source_line INTEGER NOT NULL, loaded_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS rejected_rows (
            reject_id INTEGER PRIMARY KEY, source_file TEXT NOT NULL, source_line INTEGER NOT NULL,
            record_json TEXT NOT NULL, error_reason TEXT NOT NULL, rejected_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source_file, source_line, record_json));
        CREATE INDEX IF NOT EXISTS ix_fact_date ON fact_transactions(date_key);
        CREATE INDEX IF NOT EXISTS ix_fact_product ON fact_transactions(product_id);
        CREATE INDEX IF NOT EXISTS ix_fact_region_date ON fact_transactions(region_id, date_key);
    """)
    return conn


def _validate_transaction(row: dict[str, str]) -> dict[str, Any]:
    """Validate required fields, types, business ranges and internal arithmetic."""
    result: dict[str, Any] = dict(row)
    result["date"] = _date(row["date"]).isoformat()
    for name, converter in NUMERIC_COLUMNS.items():
        result[name] = _as_num(row[name], converter)
    for name in ("region", "product_category", "sku", "channel", "customer_type"):
        if not row[name].strip():
            raise ValueError(f"{name} is required")
        result[name] = row[name].strip()
    dt = date.fromisoformat(result["date"])
    iso_year, iso_week, _ = dt.isocalendar()
    checks = (
        (result["year"] == dt.year, "year does not match date"),
        (result["month"] == dt.month, "month does not match date"),
        (result["week_of_year"] == iso_week, "week_of_year does not match ISO date"),
        (1 <= result["month"] <= 12, "month outside 1..12"),
        (1 <= result["week_of_year"] <= 53, "week_of_year outside 1..53"),
        (result["units"] >= 0, "units must be non-negative"),
        (result["unit_price"] >= 0, "unit_price must be non-negative"),
        (result["revenue"] >= 0, "revenue must be non-negative"),
        (math.isclose(result["revenue"], result["units"] * result["unit_price"], rel_tol=1e-5, abs_tol=0.02), "revenue differs from units * unit_price"),
        (result["housing_starts_index"] >= 0 and result["lumber_price_index"] >= 0 and result["mortgage_rate"] >= 0, "macro indicators must be non-negative"),
    )
    for valid, message in checks:
        if not valid:
            raise ValueError(message)
    return result


def _validate_macro(row: dict[str, str]) -> dict[str, Any]:
    """Validate the weekly macro driver record and its numeric ranges."""
    result: dict[str, Any] = {"week": _date(row["week"]).isoformat()}
    for name, converter in MACRO_NUMERIC.items():
        result[name] = _as_num(row[name], converter)
    if any(result[name] < 0 for name in MACRO_NUMERIC):
        raise ValueError("macro values must be non-negative")
    return result


def _reject(conn: sqlite3.Connection, source: str, line: int, row: dict[str, str], reason: str) -> None:
    """Persist a bad row and its actionable validation reason without stopping the batch."""
    conn.execute("INSERT OR IGNORE INTO rejected_rows(source_file,source_line,record_json,error_reason) VALUES(?,?,?,?)",
                 (source, line, json.dumps(row, ensure_ascii=False, sort_keys=True), reason))


def load_transactions(conn: sqlite3.Connection, path: Path) -> tuple[int, int]:
    """Validate and upsert transactions; return (accepted rows, rejected rows)."""
    accepted = rejected = 0
    for line_no, raw in _rows(path, TRANSACTION_COLUMNS, "transactions"):
        try:
            row = _validate_transaction(raw)
        except (ValueError, TypeError, KeyError) as exc:
            _reject(conn, path.name, line_no, raw, str(exc)); rejected += 1; continue
        # No transaction ID is supplied. File name + stable CSV line number
        # distinguishes same-grain events and lets a corrected line overwrite
        # its prior version on the next run.
        key = _hash({"source_file": path.name, "source_line": line_no})
        conn.execute("INSERT INTO dim_date VALUES(?,?,?,?) ON CONFLICT(date_key) DO UPDATE SET year=excluded.year,month=excluded.month,week_of_year=excluded.week_of_year",
                     (row["date"], row["year"], row["month"], row["week_of_year"]))
        conn.execute("INSERT OR IGNORE INTO dim_region(region) VALUES(?)", (row["region"],))
        conn.execute("INSERT INTO dim_product(sku,product_category) VALUES(?,?) ON CONFLICT(sku) DO UPDATE SET product_category=excluded.product_category", (row["sku"], row["product_category"]))
        conn.execute("INSERT OR IGNORE INTO dim_channel(channel) VALUES(?)", (row["channel"],))
        conn.execute("INSERT OR IGNORE INTO dim_customer_type(customer_type) VALUES(?)", (row["customer_type"],))
        ids = conn.execute("SELECT (SELECT region_id FROM dim_region WHERE region=?),(SELECT product_id FROM dim_product WHERE sku=?),(SELECT channel_id FROM dim_channel WHERE channel=?),(SELECT customer_type_id FROM dim_customer_type WHERE customer_type=?)",
                           (row["region"], row["sku"], row["channel"], row["customer_type"])).fetchone()
        conn.execute("""INSERT INTO fact_transactions(transaction_key,date_key,region_id,product_id,channel_id,customer_type_id,units,unit_price,revenue,housing_starts_index,lumber_price_index,mortgage_rate,source_file,source_line)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(transaction_key) DO UPDATE SET
            date_key=excluded.date_key,region_id=excluded.region_id,product_id=excluded.product_id,channel_id=excluded.channel_id,customer_type_id=excluded.customer_type_id,
            units=excluded.units,unit_price=excluded.unit_price,revenue=excluded.revenue,housing_starts_index=excluded.housing_starts_index,
            lumber_price_index=excluded.lumber_price_index,mortgage_rate=excluded.mortgage_rate,source_file=excluded.source_file,source_line=excluded.source_line,loaded_at=CURRENT_TIMESTAMP""",
            (key, row["date"], *ids, row["units"], row["unit_price"], row["revenue"], row["housing_starts_index"], row["lumber_price_index"], row["mortgage_rate"], path.name, line_no))
        accepted += 1
    return accepted, rejected


def load_macro(conn: sqlite3.Connection, path: Path) -> tuple[int, int]:
    """Validate and upsert weekly macro rows; return (accepted rows, rejected rows)."""
    accepted = rejected = 0
    for line_no, raw in _rows(path, MACRO_COLUMNS, "macro"):
        try:
            row = _validate_macro(raw)
            if conn.execute("SELECT 1 FROM dim_date WHERE date_key=?", (row["week"],)).fetchone() is None:
                raise ValueError("macro week is not present in the transaction calendar")
        except (ValueError, TypeError, KeyError) as exc:
            _reject(conn, path.name, line_no, raw, str(exc)); rejected += 1; continue
        conn.execute("""INSERT INTO macro_weekly VALUES(?,?,?,?,?) ON CONFLICT(week) DO UPDATE SET
            housing_starts_index=excluded.housing_starts_index,lumber_price_index=excluded.lumber_price_index,
            mortgage_rate=excluded.mortgage_rate,season_factor=excluded.season_factor""",
            tuple(row[name] for name in MACRO_COLUMNS))
        accepted += 1
    return accepted, rejected


def run_pipeline(transactions: Path, macro: Path, database: Path) -> dict[str, int]:
    """Run extraction, validation, dimensional loading and facts in one atomic transaction."""
    conn = _connect(database)
    try:
        with conn:
            tx_ok, tx_bad = load_transactions(conn, transactions)
            macro_ok, macro_bad = load_macro(conn, macro)
        return {"transactions_accepted": tx_ok, "transactions_rejected": tx_bad,
                "macro_accepted": macro_ok, "macro_rejected": macro_bad,
                "fact_rows": conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0],
                "reject_rows": conn.execute("SELECT COUNT(*) FROM rejected_rows").fetchone()[0]}
    finally:
        conn.close()


def main() -> None:
    """Parse command-line paths, execute ETL and print a compact JSON summary."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--transactions", type=Path, default=Path("building_materials_transactions.csv"))
    parser.add_argument("--macro", type=Path, default=Path("macro_drivers_weekly.csv"))
    parser.add_argument("--database", type=Path, default=Path("data/warehouse.sqlite"))
    args = parser.parse_args()
    print(json.dumps(run_pipeline(args.transactions, args.macro, args.database), indent=2))


if __name__ == "__main__":
    main()
