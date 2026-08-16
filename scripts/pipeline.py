import os
import re
import sqlite3
import logging
from datetime import datetime
import pandas as pd

# Expected columns in the raw input dataset
REQUIRED_HEADERS = [
    "transaction_id",
    "order_id",
    "customer_id",
    "customer_email",
    "transaction_timestamp",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "payment_method",
    "status"
]

EMAIL_REGEX = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")

def setup_logger(log_file_path: str) -> logging.Logger:
    """Configures structured logging to both file and console."""
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    logger = logging.getLogger("DataQualityPipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    
    # File Handler
    fh = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    # Console Handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    return logger

def initialize_database(db_path: str):
    """Creates SQLite tables for clean data, quarantined data, and pipeline audit logs."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. clean_transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS clean_transactions (
            run_id TEXT NOT NULL,
            transaction_id TEXT PRIMARY KEY,
            order_id TEXT NOT NULL,
            customer_id TEXT NOT NULL,
            customer_email TEXT NOT NULL,
            transaction_timestamp TEXT NOT NULL,
            product_id TEXT NOT NULL,
            category TEXT,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            payment_method TEXT,
            status TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. quarantined_transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS quarantined_transactions (
            run_id TEXT NOT NULL,
            transaction_id TEXT,
            order_id TEXT,
            customer_id TEXT,
            customer_email TEXT,
            transaction_timestamp TEXT,
            product_id TEXT,
            category TEXT,
            quantity REAL,
            unit_price REAL,
            payment_method TEXT,
            status TEXT,
            rejection_reasons TEXT NOT NULL,
            quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 3. pipeline_audit_log
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_audit_log (
            run_id TEXT PRIMARY KEY,
            run_timestamp TEXT NOT NULL,
            source_file TEXT NOT NULL,
            total_records INTEGER NOT NULL,
            valid_records INTEGER NOT NULL,
            quarantined_records INTEGER NOT NULL,
            pass_rate REAL NOT NULL,
            status TEXT NOT NULL
        )
    """)
    
    conn.commit()
    conn.close()

def pre_validate_file(raw_csv_path: str, logger: logging.Logger) -> bool:
    """
    Performs structural pre-validation checks:
    1. Checks if file exists and is non-empty (>0 bytes).
    2. Verifies header completeness against REQUIRED_HEADERS.
    """
    if not os.path.exists(raw_csv_path):
        logger.critical(f"Pre-validation failed: Input file '{raw_csv_path}' does not exist.")
        return False
        
    if os.path.getsize(raw_csv_path) == 0:
        logger.critical(f"Pre-validation failed: Input file '{raw_csv_path}' is completely empty (0 bytes).")
        return False
        
    try:
        # Read header only
        header_df = pd.read_csv(raw_csv_path, nrows=0)
        present_headers = list(header_df.columns)
        missing_headers = [h for h in REQUIRED_HEADERS if h not in present_headers]
        
        if missing_headers:
            logger.critical(f"Pre-validation failed: Missing required CSV headers: {missing_headers}")
            return False
            
        logger.info(f"Structural pre-validation passed. All {len(REQUIRED_HEADERS)} required headers are present.")
        return True
    except Exception as e:
        logger.critical(f"Pre-validation failed due to CSV parsing error: {e}")
        return False

def validate_record(row: dict, seen_order_ids: set, current_time: datetime) -> list:
    """
    Applies Data Quality rules to a single record with safe NaN/Null handling.
    Returns a list of rejection reasons (empty if record is valid).
    """
    reasons = []
    
    # Rule 01: Non-Null Customer ID & Order ID
    cust_id = row.get("customer_id")
    order_id = row.get("order_id")
    if pd.isna(cust_id) or str(cust_id).strip() == "":
        reasons.append("DQ_RULE_01: customer_id is NULL or blank")
    if pd.isna(order_id) or str(order_id).strip() == "":
        reasons.append("DQ_RULE_01: order_id is NULL or blank")
        
    # Rule 02: Valid Email Syntax
    email = row.get("customer_email")
    if pd.isna(email) or not isinstance(email, str) or not EMAIL_REGEX.match(str(email).strip()):
        reasons.append(f"DQ_RULE_02: Invalid customer_email format ({email})")
        
    # Rule 03: Unit Price Bounds (Safe NaN/Null Check)
    price = row.get("unit_price")
    if pd.isna(price):
        reasons.append("DQ_RULE_03: unit_price is NULL/NaN")
    else:
        try:
            val_price = float(price)
            if val_price <= 0:
                reasons.append(f"DQ_RULE_03: unit_price ({val_price}) must be > 0")
        except (ValueError, TypeError):
            reasons.append(f"DQ_RULE_03: unit_price ({price}) is not numeric")

    # Rule 04: Quantity Bounds (Safe NaN/Null Check)
    qty = row.get("quantity")
    if pd.isna(qty):
        reasons.append("DQ_RULE_04: quantity is NULL/NaN")
    else:
        try:
            val_qty = float(qty)
            if val_qty <= 0:
                reasons.append(f"DQ_RULE_04: quantity ({val_qty}) must be > 0")
        except (ValueError, TypeError):
            reasons.append(f"DQ_RULE_04: quantity ({qty}) is not numeric")

    # Rule 05: Timestamp Validity
    ts_str = row.get("transaction_timestamp")
    if pd.isna(ts_str):
        reasons.append("DQ_RULE_05: transaction_timestamp is NULL")
    else:
        try:
            ts_dt = datetime.strptime(str(ts_str).strip(), "%Y-%m-%d %H:%M:%S")
            if ts_dt > current_time:
                reasons.append(f"DQ_RULE_05: Future timestamp detected ({ts_str})")
        except ValueError:
            reasons.append(f"DQ_RULE_05: Invalid timestamp format ({ts_str})")

    # Rule 06: Order ID Uniqueness Across Batch
    if order_id and not pd.isna(order_id):
        order_id_str = str(order_id).strip()
        if order_id_str in seen_order_ids:
            reasons.append(f"DQ_RULE_06: Duplicate order_id detected ({order_id_str})")
            
    return reasons

def run_pipeline(raw_csv_path: str, quarantine_csv_path: str, db_path: str, log_file_path: str, chunk_size: int = 250):
    """
    Main Data Quality pipeline execution routine.
    Involves pre-validation, chunked ingestion, rule evaluation, quarantining, and SQLite persistence.
    """
    logger = setup_logger(log_file_path)
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_id = f"RUN-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    logger.info(f"Starting Data Quality Pipeline execution [Run ID: {run_id}]")
    logger.info(f"Target Input File: {raw_csv_path}")
    
    initialize_database(db_path)
    
    # Step 1: Pre-Validation Check
    if not pre_validate_file(raw_csv_path, logger):
        logger.critical("Pipeline execution HALTED due to pre-validation failure.")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pipeline_audit_log (run_id, run_timestamp, source_file, total_records, valid_records, quarantined_records, pass_rate, status)
            VALUES (?, ?, ?, 0, 0, 0, 0.0, 'FAILED_PREVALIDATION')
        """, (run_id, run_timestamp, raw_csv_path))
        conn.commit()
        conn.close()
        return False

    # Step 2: Chunked Processing & Data Quality Checks
    seen_order_ids = set()
    total_records = 0
    clean_records = []
    quarantine_records = []
    current_time = datetime.now()
    
    logger.info(f"Ingesting dataset in chunks of size {chunk_size}...")
    
    chunk_idx = 0
    for chunk in pd.read_csv(raw_csv_path, chunksize=chunk_size):
        chunk_idx += 1
        logger.info(f"Processing Chunk #{chunk_idx} ({len(chunk)} rows)...")
        
        for _, row in chunk.iterrows():
            total_records += 1
            row_dict = row.to_dict()
            txn_id = row_dict.get("transaction_id", f"ROW-{total_records}")
            
            reasons = validate_record(row_dict, seen_order_ids, current_time)
            
            if reasons:
                reason_str = " | ".join(reasons)
                row_dict["run_id"] = run_id
                row_dict["rejection_reasons"] = reason_str
                quarantine_records.append(row_dict)
                logger.warning(f"[QUARANTINE] Record {txn_id} [Run: {run_id}] failed: {reason_str}")
            else:
                # Valid record
                row_dict["run_id"] = run_id
                clean_records.append(row_dict)
                if row_dict.get("order_id"):
                    seen_order_ids.add(str(row_dict["order_id"]).strip())

    valid_count = len(clean_records)
    quarantine_count = len(quarantine_records)
    pass_rate = round((valid_count / total_records * 100), 2) if total_records > 0 else 0.0

    # Step 3: Write Clean Records to SQLite
    conn = sqlite3.connect(db_path)
    if clean_records:
        clean_df = pd.DataFrame(clean_records)
        # Ensure order of columns matching table
        cols = ["run_id", "transaction_id", "order_id", "customer_id", "customer_email", 
                "transaction_timestamp", "product_id", "category", "quantity", "unit_price", 
                "payment_method", "status"]
        clean_df[cols].to_sql("clean_transactions", conn, if_exists="append", index=False)
        logger.info(f"Successfully loaded {valid_count} clean records into SQLite 'clean_transactions'.")

    # Step 4: Write Quarantined Records to CSV & SQLite
    if quarantine_records:
        os.makedirs(os.path.dirname(quarantine_csv_path), exist_ok=True)
        quarantine_df = pd.DataFrame(quarantine_records)
        
        # Write to Quarantine CSV
        file_exists = os.path.exists(quarantine_csv_path) and os.path.getsize(quarantine_csv_path) > 0
        quarantine_df.to_csv(quarantine_csv_path, mode="a", index=False, header=not file_exists)
        logger.info(f"Appended {quarantine_count} quarantined records to CSV: {quarantine_csv_path}")
        
        # Write to Quarantine SQLite Table
        q_cols = ["run_id", "transaction_id", "order_id", "customer_id", "customer_email", 
                  "transaction_timestamp", "product_id", "category", "quantity", "unit_price", 
                  "payment_method", "status", "rejection_reasons"]
        quarantine_df[q_cols].to_sql("quarantined_transactions", conn, if_exists="append", index=False)
        logger.info(f"Successfully loaded {quarantine_count} quarantined records into SQLite 'quarantined_transactions'.")

    # Step 5: Write Audit Summary to SQLite
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO pipeline_audit_log (run_id, run_timestamp, source_file, total_records, valid_records, quarantined_records, pass_rate, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'SUCCESS')
    """, (run_id, run_timestamp, raw_csv_path, total_records, valid_count, quarantine_count, pass_rate))
    conn.commit()
    conn.close()

    logger.info("=================================================================")
    logger.info(f"PIPELINE SUMMARY [Run ID: {run_id}]")
    logger.info(f"Total Processed: {total_records} | Valid: {valid_count} | Quarantined: {quarantine_count} | Pass Rate: {pass_rate}%")
    logger.info("=================================================================")
    return True

if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    raw_csv = os.path.join(base_dir, "data", "raw_transactions.csv")
    quarantine_csv = os.path.join(base_dir, "data", "quarantine_transactions.csv")
    db_file = os.path.join(base_dir, "database", "ecommerce_data.db")
    log_file = os.path.join(base_dir, "logs", "pipeline_errors.log")
    
    run_pipeline(raw_csv, quarantine_csv, db_file, log_file, chunk_size=250)
