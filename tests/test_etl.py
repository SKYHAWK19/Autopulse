import os
import tempfile
import unittest
import pandas as pd
from etl import coerce_dirty_numeric, run_etl


class TestAutoPulseETL(unittest.TestCase):
    def test_dirty_numeric_coercion(self):
        s = pd.Series(["$1,250.50", "₹ 400", "ERROR", "UNKNOWN", "25.0"])
        res = coerce_dirty_numeric(s)
        self.assertEqual(res.iloc[0], 1250.50)
        self.assertEqual(res.iloc[1], 400.0)
        self.assertTrue(pd.isna(res.iloc[2]))
        self.assertTrue(pd.isna(res.iloc[3]))
        self.assertEqual(res.iloc[4], 25.0)

    def test_universal_cleaning_integrity_and_conflict_preservation(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_csv = os.path.join(tmp, "raw.csv")
            clean_csv = os.path.join(tmp, "clean.csv")
            report_json = os.path.join(tmp, "report.json")

            pd.DataFrame(
                {
                    "Transaction ID": ["T1", "T1", "T2", "T3", "T4", "T5"],
                    "Item": ["Coffee", "Coffee", "Coffee", "Tea", "UNKNOWN", "ERROR"],
                    "Quantity": ["2", "2", "3", "4", "2", "100"],
                    "Price Per Unit": ["2.0", "2.0", "ERROR", "1.5", "2.0", "2.0"],
                    "Total Spent": ["4.0", "4.0", "ERROR", "99.0", "4.0", "200.0"],
                    "Transaction Date": ["2026-01-01", "2026-01-01", "ERROR", "2026-01-03", "2026-01-04", "2026-01-05"],
                }
            ).to_csv(raw_csv, index=False)

            report = run_etl(raw_csv, clean_csv, report_json)
            df = pd.read_csv(clean_csv)

            # Deduplication + invalid-date preservation
            self.assertEqual(len(df), 5)
            self.assertEqual(report["dropped_breakdown"]["duplicates"], 1)
            self.assertEqual(int(df["is_date_missing"].sum()), 1)

            # Lookup recovery (Coffee -> 2.0) + algebraic total (3 * 2.0 = 6.0)
            t2 = df[df["transaction_id"] == "T2"].iloc[0]
            self.assertEqual(t2["price_per_unit"], 2.0)
            self.assertEqual(t2["total_spent"], 6.0)

            # Conflict preservation (T3 total=99.0 preserved, math_conflict=True)
            t3 = df[df["transaction_id"] == "T3"].iloc[0]
            self.assertEqual(t3["total_spent"], 99.0)
            self.assertEqual(t3["calculated_total"], 6.0)
            self.assertTrue(bool(t3["math_conflict"]))

            # Distinct categorical normalization
            self.assertIn("UNKNOWN", df["item"].values)
            self.assertIn("INVALID_ENTRY", df["item"].values)


if __name__ == "__main__":
    unittest.main()