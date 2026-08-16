import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd

def generate_mock_data(output_path: str, num_records: int = 1000, anomaly_ratio: float = 0.25):
    """
    Generates a synthetic e-commerce transaction dataset containing valid records
    and controlled business anomalies for data quality testing.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    categories = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Beauty", "Sports"]
    payment_methods = ["Credit Card", "PayPal", "Debit Card", "UPI"]
    statuses = ["Completed", "Pending", "Failed"]
    valid_domains = ["gmail.com", "yahoo.com", "outlook.com", "company.org"]
    
    records = []
    base_time = datetime(2026, 8, 1, 10, 0, 0)
    
    # Keep track of generated order IDs to explicitly inject duplicate order IDs
    existing_order_ids = []
    
    print(f"Generating {num_records} mock records (Anomaly ratio: {anomaly_ratio:.0%})...")
    
    for i in range(1, num_records + 1):
        txn_id = f"TXN-{10000 + i}"
        order_id = f"ORD-{50000 + i}"
        customer_id = f"CUST-{random.randint(100, 999)}"
        email = f"user_{random.randint(100, 999)}@{random.choice(valid_domains)}"
        
        # Generate valid timestamp within the last 7 days
        random_seconds = random.randint(0, 7 * 24 * 3600)
        txn_time = base_time - timedelta(seconds=random_seconds)
        txn_time_str = txn_time.strftime("%Y-%m-%d %H:%M:%S")
        
        product_id = f"PROD-{random.randint(1000, 9999)}"
        category = random.choice(categories)
        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5.0, 500.0), 2)
        payment_method = random.choice(payment_methods)
        status = random.choice(statuses)
        
        # Determine if this record should be an anomaly
        is_anomaly = (random.random() < anomaly_ratio)
        
        if is_anomaly:
            anomaly_type = random.choice([
                "negative_price",
                "zero_price",
                "null_price",
                "future_date",
                "missing_customer_id",
                "invalid_email_no_at",
                "invalid_email_no_tld",
                "duplicate_order_id",
                "negative_quantity",
                "zero_quantity",
                "null_quantity"
            ])
            
            if anomaly_type == "negative_price":
                unit_price = -round(random.uniform(10.0, 150.0), 2)
            elif anomaly_type == "zero_price":
                unit_price = 0.0
            elif anomaly_type == "null_price":
                unit_price = None
            elif anomaly_type == "future_date":
                future_time = datetime.now() + timedelta(days=random.randint(30, 365))
                txn_time_str = future_time.strftime("%Y-%m-%d %H:%M:%S")
            elif anomaly_type == "missing_customer_id":
                customer_id = None
            elif anomaly_type == "invalid_email_no_at":
                email = f"user_{random.randint(100, 999)}gmail.com"
            elif anomaly_type == "invalid_email_no_tld":
                email = f"user_{random.randint(100, 999)}@gmail"
            elif anomaly_type == "duplicate_order_id" and len(existing_order_ids) > 0:
                order_id = random.choice(existing_order_ids)
            elif anomaly_type == "negative_quantity":
                quantity = -random.randint(1, 5)
            elif anomaly_type == "zero_quantity":
                quantity = 0
            elif anomaly_type == "null_quantity":
                quantity = None
        else:
            existing_order_ids.append(order_id)
            
        record = {
            "transaction_id": txn_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "customer_email": email,
            "transaction_timestamp": txn_time_str,
            "product_id": product_id,
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "payment_method": payment_method,
            "status": status
        }
        records.append(record)

    df = pd.DataFrame(records)
    df.to_csv(output_path, index=False)
    print(f"Successfully generated mock dataset at: {output_path} ({len(df)} rows)")

if __name__ == "__main__":
    output_csv = os.path.join(os.path.dirname(__file__), "..", "data", "raw_transactions.csv")
    output_csv = os.path.abspath(output_csv)
    generate_mock_data(output_csv, num_records=1000, anomaly_ratio=0.25)
