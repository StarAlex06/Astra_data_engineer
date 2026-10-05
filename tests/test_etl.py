"""Focused tests for validation, row quarantine, idempotency and rollback."""

import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

import etl


class PipelineTests(unittest.TestCase):
    """Exercise the end-to-end contract using tiny temporary CSV files."""

    def setUp(self):
        """Create isolated transaction and macro source files for each test."""
        # Keep fixtures inside the writable project rather than the OS temp folder.
        self.tmp = tempfile.TemporaryDirectory(dir=Path(__file__).resolve().parents[1])
        self.root = Path(self.tmp.name)
        self.tx = self.root / "transactions.csv"
        self.macro = self.root / "macro.csv"
        self.db = self.root / "warehouse.sqlite"
        self.tx_rows = [dict(date="2024-01-01", year="2024", month="1", week_of_year="1", region="Pacific", product_category="lumber", sku="LUM-1", channel="pro_dealer", customer_type="contractor", units="2", unit_price="10.00", revenue="20.00", housing_starts_index="100", lumber_price_index="400", mortgage_rate="5.0")]
        self.macro_rows = [dict(week="2024-01-01", housing_starts_index="100", lumber_price_index="400", mortgage_rate="5.0", season_factor="0.5")]
        self._write(self.tx, etl.TRANSACTION_COLUMNS, self.tx_rows)
        self._write(self.macro, etl.MACRO_COLUMNS, self.macro_rows)

    def tearDown(self):
        """Remove the temporary database and source files."""
        self.tmp.cleanup()

    @staticmethod
    def _write(path, columns, rows):
        """Write records with the exact source schema expected by the loader."""
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader(); writer.writerows(rows)

    def test_validation_rejects_bad_revenue(self):
        """Revenue must agree with units multiplied by unit price."""
        bad = dict(self.tx_rows[0], revenue="19.00")
        with self.assertRaisesRegex(ValueError, "revenue differs"):
            etl._validate_transaction(bad)

    def test_bad_row_is_quarantined_and_good_row_loaded(self):
        """One corrupt row must not prevent a valid neighboring row from loading."""
        self._write(self.tx, etl.TRANSACTION_COLUMNS, [self.tx_rows[0], dict(self.tx_rows[0], revenue="0")])
        result = etl.run_pipeline(self.tx, self.macro, self.db)
        self.assertEqual((result["transactions_accepted"], result["transactions_rejected"], result["fact_rows"]), (1, 1, 1))
        conn = sqlite3.connect(self.db)
        self.assertIn("revenue differs", conn.execute("select error_reason from rejected_rows").fetchone()[0])
        conn.close()

    def test_repeat_run_is_idempotent(self):
        """Loading identical inputs twice must preserve one fact and one macro row."""
        etl.run_pipeline(self.tx, self.macro, self.db)
        result = etl.run_pipeline(self.tx, self.macro, self.db)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("select count(*) from fact_transactions").fetchone()[0], 1)
        self.assertEqual(conn.execute("select count(*) from macro_weekly").fetchone()[0], 1)
        conn.close()
        self.assertEqual(result["fact_rows"], 1)

    def test_corrected_historical_line_updates_existing_fact(self):
        """Correcting a source row in place must update, not duplicate, its fact."""
        etl.run_pipeline(self.tx, self.macro, self.db)
        corrected = dict(self.tx_rows[0], units="3", revenue="30.00")
        self._write(self.tx, etl.TRANSACTION_COLUMNS, [corrected])
        etl.run_pipeline(self.tx, self.macro, self.db)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("select count(*) from fact_transactions").fetchone()[0], 1)
        self.assertEqual(conn.execute("select units,revenue from fact_transactions").fetchone(), (3, 30.0))
        conn.close()

    def test_macro_week_outside_transaction_calendar_is_rejected(self):
        """An unmatched macro week should be quarantined instead of aborting the batch."""
        unmatched = dict(self.macro_rows[0], week="2024-01-08")
        self._write(self.macro, etl.MACRO_COLUMNS, [unmatched])
        result = etl.run_pipeline(self.tx, self.macro, self.db)
        self.assertEqual((result["transactions_accepted"], result["macro_accepted"], result["macro_rejected"]), (1, 0, 1))

    def test_fatal_source_schema_error_rolls_back(self):
        """A structural source failure must abort the transaction cleanly."""
        malformed = self.root / "malformed.csv"
        malformed.write_text("wrong,header\n1,2\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "expected columns"):
            etl.run_pipeline(self.tx, malformed, self.db)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("select count(*) from fact_transactions").fetchone()[0], 0)
        conn.close()


if __name__ == "__main__":
    unittest.main()
