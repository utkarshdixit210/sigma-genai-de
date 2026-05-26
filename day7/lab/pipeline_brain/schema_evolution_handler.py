from typing import Dict, List, Tuple, Union
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], List[str], Dict[str, Tuple[str, str]], str]]:
    """
    Detects schema drift between expected and actual schemas.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.

    Returns:
        Dict[str, Union[Dict[str, str], List[str], Dict[str, Tuple[str, str]], str]]: A dictionary containing the drift report.
    """
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any(actual_schema[col] not in ['string', 'boolean'] or expected_schema.get(col, None) for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {"new_columns": new_columns, "removed_columns": list(removed_columns.keys()), "type_changes": type_changes, "drift_severity": drift_severity}

def decide_action(drift_report: Dict[str, Union[Dict[str, str], List[str], Dict[str, Tuple[str, str]], str]]) -> Dict[str, Dict[str, Union[str, str, int]]]:
    """
    Decides the action to take for each column based on the drift report.

    Args:
        drift_report (Dict[str, Union[Dict[str, str], List[str], Dict[str, Tuple[str, str]], str]]): The drift report.

    Returns:
        Dict[str, Dict[str, Union[str, str, int]]]: A dictionary containing the action to take for each column.
    """
    decisions = {}
    for col, dtype in drift_report["new_columns"].items():
        if dtype == 'string':
            decisions[col] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable string column", "risk_level": 0}
        elif dtype == 'float' or dtype == 'double':
            decisions[col] = {"action": "FLAG_ANOMALY", "reason": "New numeric column, could affect revenue calculations", "risk_level": 2}
        elif dtype == 'boolean':
            decisions[col] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable boolean column", "risk_level": 0}
    for col in drift_report["removed_columns"]:
        decisions[col] = {"action": "HALT", "reason": "Removed column, will break downstream queries", "risk_level": 3}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, str, int]]]) -> Tuple[DataFrame, List[str]]:
    """
    Applies the schema evolution decisions to the DataFrame.

    Args:
        spark_df (DataFrame): The Spark DataFrame.
        decisions (Dict[str, Dict[str, Union[str, str, int]]]): The decisions to apply.

    Returns:
        Tuple[DataFrame, List[str]]: The evolved DataFrame and a list of migration notes.
    """
    migration_notes = []
    for col, decision in decisions.items():
        if decision["action"] == "DROP_SILENTLY":
            spark_df = spark_df.drop(col)
            migration_notes.append(f"Column '{col}' silently dropped.")
        elif decision["action"] == "FLAG_ANOMALY":
            spark_df = spark_df.withColumn(f"{col}_anomaly", F.when(F.col(col).isNull(), 1).otherwise(0))
            migration_notes.append(f"Column '{col}' flagged for anomaly.")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict[str, Union[Dict[str, str], List[str], Dict[str, Tuple[str, str]], str]], Dict[str, Dict[str, Union[str, str, int]]], Tuple[DataFrame, List[str]]]]:
    """
    Handles schema drift by detecting, deciding, and applying schema evolution.

    Args:
        expected_schema (Dict[str, str]): The expected schema.
        actual_schema (Dict[str, str]): The actual schema.
        spark_df (DataFrame, optional): The Spark DataFrame. Defaults to None.

    Returns:
        Dict[str, Union[Dict[str, Union[Dict[str, str], List[str], Dict[str, Tuple[str, str]], str]], Dict[str, Dict[str, Union[str, str, int]]], Tuple[DataFrame, List[str]]]]: The full evolution report.
    """
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions)
        drift_report["migration_notes"] = migration_notes
        return {"drift_report": drift_report, "decisions": decisions, "evolved_df": evolved_df}
    else:
        drift_report["decisions"] = decisions
        return {"drift_report": drift_report, "decisions": decisions}
