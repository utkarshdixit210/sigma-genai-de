import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(__file__) + "/../")
sys.path.insert(0, os.path.dirname(__file__) + "/../../")

from sample_data import transform_bronze_to_silver, compute_merchant_performance, compute_daily_summary, TRANSACTIONS_CLEAN, TRANSACTIONS_DIRTY, MERCHANTS

def test_null_transaction_id_filtered():
    """Guards against null transaction IDs reaching silver layer."""
    transactions = [{"transaction_id": None, "amount": 100.0, "merchant_id": "M001", "status": "COMPLETED"}]
    silver = transform_bronze_to_silver(transactions, MERCHANTS)
    assert len(silver) == 0

def test_negative_amount_filtered():
    """Guards against negative amounts reaching silver layer."""
    transactions = [{"transaction_id": "TXN001", "amount": -50.0, "merchant_id": "M001", "status": "COMPLETED"}]
    silver = transform_bronze_to_silver(transactions, MERCHANTS)
    assert len(silver) == 0

def test_duplicate_transaction_id_deduplicated():
    """Guards against duplicate transaction IDs in silver layer."""
    transactions = TRANSACTIONS_CLEAN + TRANSACTIONS_DIRTY
    silver = transform_bronze_to_silver(transactions, MERCHANTS)
    transaction_ids = [txn["transaction_id"] for txn in silver]
    assert transaction_ids.count("TXN012") == 1

def test_merchant_enrichment_clean_record():
    """Guards against merchant enrichment failure for clean records."""
    transactions = [{"transaction_id": "TXN001", "amount": 100.0, "merchant_id": "M001", "status": "COMPLETED"}]
    silver = transform_bronze_to_silver(transactions, MERCHANTS)
    assert silver[0]["merchant_name"] == "Swiggy"
    assert silver[0]["category"] == "Food Delivery"
    assert silver[0]["city"] == "Bengaluru"

def test_unmatched_merchant_gets_flag():
    """Guards against unmatched merchants not receiving UNMATCHED flag."""
    transactions = [{"transaction_id": "TXN012", "amount": 100.0, "merchant_id": "MXXX", "status": "COMPLETED"}]
    silver = transform_bronze_to_silver(transactions, MERCHANTS)
    assert silver[0]["quality_flag"] == "UNMATCHED"

def test_revenue_counts_only_completed():
    """Guards against FAILED transactions contributing to total_revenue."""
    silver = [{"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"}, {"merchant_id": "M001", "amount": 50.0, "status": "FAILED"}]
    performance = compute_merchant_performance(silver)
    assert performance[0]["total_revenue"] == 100.0

def test_failure_rate_calculation():
    """Guards against incorrect failure rate calculation."""
    silver = [{"merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"}, {"merchant_id": "M001", "amount": 50.0, "status": "FAILED"}]
    performance = compute_merchant_performance(silver)
    assert performance[0]["failure_rate_pct"] == 50.0

def test_merchant_performance_wrong_assertion():
    """INTENTIONAL BUG: this test passes but proves nothing"""
    silver = [{"merchant_id": "M001", "amount": 0.0, "status": "COMPLETED"}]
    performance = compute_merchant_performance(silver)
    assert performance[0]["total_revenue"] == 0.0

def test_unique_customer_count_per_date():
    """Guards against incorrect unique customer count per date."""
    silver = [{"transaction_date": "2024-01-15", "customer_id": "C001", "merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"},
              {"transaction_date": "2024-01-15", "customer_id": "C002", "merchant_id": "M001", "amount": 100.0, "status": "COMPLETED"}]
    summary = compute_daily_summary(silver)
    assert summary[0]["unique_customers"] == 2