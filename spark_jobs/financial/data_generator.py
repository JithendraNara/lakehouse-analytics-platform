"""
Generate simulated financial transaction data.
Creates 10M+ records for testing the pipeline.
"""

import csv
import random
import datetime
import argparse
from typing import List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Configuration
ACCOUNT_IDS = [f"ACC{str(i).zfill(8)}" for i in range(1, 100001)]  # 100k accounts
MERCHANT_CATEGORIES = [
    "grocery", "restaurant", "gas_station", "online_shopping", 
    "travel", "entertainment", "healthcare", "utilities",
    "atm_withdrawal", "transfer", "subscription", "retail"
]
LOCATIONS = [
    "New York, NY", "Los Angeles, CA", "Chicago, IL", "Houston, TX",
    "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
    "Dallas, TX", "San Jose, CA", "Austin, TX", "Seattle, WA",
    "Denver, CO", "Boston, MA", "Miami, FL", "Atlanta, GA"
]
CURRENCIES = ["USD"] * 95 + ["EUR", "GBP"] * 2 + ["JPY"]  # Mostly USD


class TransactionGenerator:
    def __init__(self, seed=42):
        random.seed(seed)
        self.account_balances = {acc: random.uniform(1000, 50000) for acc in ACCOUNT_IDS}
    
    def generate_transaction(self, txn_id: int) -> List:
        """Generate a single transaction record."""
        account_id = random.choice(ACCOUNT_IDS)
        
        # Generate timestamp (last 90 days, weighted toward recent)
        days_ago = random.expovariate(0.05)  # Exponential distribution
        days_ago = min(days_ago, 90)
        timestamp = datetime.datetime.now() - datetime.timedelta(days=days_ago)
        
        # Amount: exponential distribution, mean ~$150
        amount = random.expovariate(1/150)
        amount = round(max(1, min(amount, 50000)), 2)
        
        # Occasionally generate high-value transactions (anomalies)
        if random.random() < 0.01:  # 1% high-value
            amount = round(random.uniform(5000, 50000), 2)
        
        # Occasionally generate rapid transactions (velocity anomaly)
        if random.random() < 0.005:  # 0.5% rapid
            timestamp = timestamp - datetime.timedelta(seconds=random.randint(1, 30))
        
        return [
            f"TXN{str(txn_id).zfill(12)}",
            account_id,
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            amount,
            random.choice(MERCHANT_CATEGORIES),
            random.choice(LOCATIONS),
            random.choice(CURRENCIES)
        ]


def generate_csv(output_file: str, count: int, batch_size: int = 100000):
    """Generate transactions in CSV format."""
    logger.info(f"Generating {count:,} transactions...")
    
    generator = TransactionGenerator()
    header = ["transaction_id", "account_id", "timestamp", "amount", 
              "merchant_category", "location", "currency"]
    
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        
        for batch in range(0, count, batch_size):
            batch_count = min(batch_size, count - batch)
            for i in range(batch_count):
                txn = generator.generate_transaction(batch + i + 1)
                writer.writerow(txn)
            
            logger.info(f"Generated {batch + batch_count:,}/{count:,} transactions")
    
    logger.info(f"Done! Written to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Generate financial transaction data")
    parser.add_argument("--count", type=int, default=10000000, 
                        help="Number of transactions to generate (default: 10M)")
    parser.add_argument("--output", type=str, default="transactions.csv",
                        help="Output CSV file")
    parser.add_argument("--batch-size", type=int, default=100000,
                        help="Batch size for writing")
    
    args = parser.parse_args()
    
    generate_csv(args.output, args.count, args.batch_size)


if __name__ == "__main__":
    main()
