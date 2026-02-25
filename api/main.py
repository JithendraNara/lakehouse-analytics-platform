"""
FastAPI application for real-time transaction ingestion.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional
import logging
import uuid
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Financial Data Ingestion API")

# In-memory storage (replace with Redis/S3 in production)
transactions_store = {}
anomalies_store = []


class Transaction(BaseModel):
    transaction_id: str
    account_id: str
    timestamp: str
    amount: float = Field(gt=0)
    merchant_category: Optional[str] = None
    location: Optional[str] = None
    currency: Optional[str] = "USD"


class BatchTransactionRequest(BaseModel):
    transactions: List[Transaction]


class TransactionResponse(BaseModel):
    transaction_id: str
    status: str
    message: Optional[str] = None


class AnomalyResponse(BaseModel):
    transaction_id: str
    account_id: str
    amount: float
    anomaly_type: str
    timestamp: str


def detect_anomaly(txn: Transaction) -> Optional[str]:
    """Simple anomaly detection for real-time ingestion."""
    # High amount
    if txn.amount > 10000:
        return "very_high_amount"
    # Add more rules as needed
    return None


@app.get("/health")
def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


@app.post("/api/v1/ingest", response_model=TransactionResponse)
def ingest_transaction(txn: Transaction, background_tasks: BackgroundTasks):
    """Ingest a single transaction."""
    logger.info(f"Received transaction: {txn.transaction_id}")
    
    # Check for anomaly
    anomaly_type = detect_anomaly(txn)
    
    # Store transaction
    transactions_store[txn.transaction_id] = txn.dict()
    
    if anomaly_type:
        anomaly = AnomalyResponse(
            transaction_id=txn.transaction_id,
            account_id=txn.account_id,
            amount=txn.amount,
            anomaly_type=anomaly_type,
            timestamp=txn.timestamp
        )
        anomalies_store.append(anomaly.dict())
        logger.warning(f"Anomaly detected: {txn.transaction_id} - {anomaly_type}")
    
    return TransactionResponse(
        transaction_id=txn.transaction_id,
        status="processed"
    )


@app.post("/api/v1/ingest/batch", response_model=List[TransactionResponse])
def ingest_batch(request: BatchTransactionRequest):
    """Ingest multiple transactions."""
    logger.info(f"Received batch of {len(request.transactions)} transactions")
    
    responses = []
    for txn in request.transactions:
        try:
            anomaly_type = detect_anomaly(txn)
            transactions_store[txn.transaction_id] = txn.dict()
            
            if anomaly_type:
                anomalies_store.append({
                    "transaction_id": txn.transaction_id,
                    "account_id": txn.account_id,
                    "amount": txn.amount,
                    "anomaly_type": anomaly_type,
                    "timestamp": txn.timestamp
                })
            
            responses.append(TransactionResponse(
                transaction_id=txn.transaction_id,
                status="processed"
            ))
        except Exception as e:
            responses.append(TransactionResponse(
                transaction_id=getattr(txn, 'transaction_id', 'unknown'),
                status="error",
                message=str(e)
            ))
    
    return responses


@app.get("/api/v1/transactions/{transaction_id}")
def get_transaction(transaction_id: str):
    """Get a specific transaction."""
    if transaction_id not in transactions_store:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transactions_store[transaction_id]


@app.get("/api/v1/anomalies", response_model=List[AnomalyResponse])
def get_anomalies(limit: int = 100):
    """Get detected anomalies."""
    return anomalies_store[-limit:]


@app.get("/api/v1/stats")
def get_stats():
    """Get pipeline statistics."""
    return {
        "total_transactions": len(transactions_store),
        "total_anomalies": len(anomalies_store),
        "timestamp": datetime.utcnow().isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
