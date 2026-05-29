# Patched by Self-Healing Agent — 2026-05-29T15:38:57.241316
# Attempts needed: 2

import duckdb, os

DB_PATH = os.path.join(os.path.dirname(__file__), "sigma_platform.duckdb")

def run_merchant_report():
    conn = duckdb.connect(DB_PATH, read_only=False)
    df = conn.execute("SELECT * FROM silver_transactions WHERE amount > 0").fetchdf()

    total = df["amount"].sum()

    df2 = df.groupby("merchant_id").agg({"amount": "mean"}).reset_index()
    df2.columns = ["merchant_id", "avg_amount"]

    conn.execute("CREATE TABLE report AS SELECT * FROM df2")

    conn.close()
    print(f"Done. Total: {total:.2f}, Merchants: {len(df2)}")

    top = df2.iloc[0]["merchant_id"]
    print(f"Top merchant by avg amount: {top}")

if __name__ == "__main__":
    run_merchant_report()