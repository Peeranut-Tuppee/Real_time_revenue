from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Dict, Any
import psycopg2
from datetime import datetime, timedelta
import logging
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Trading Data API", version="1.0.0")

class DatabaseConnection:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="trading_db",
            user="postgres",
            password="12345"
        )
    
    def get_cursor(self):
        return self.conn.cursor()
    
    def close(self):
        self.conn.close()

db = DatabaseConnection()

class TransactionResponse(BaseModel):
    transaction_id: str
    user_id: str
    country: str
    original_currency: str
    original_amount: float
    usd_amount: float
    fx_rate: float
    timestamp: datetime

class RevenueBreakdown(BaseModel):
    category: str
    value: str
    usd_amount: float
    percentage: float

@app.get("/")
async def root():
    return {"message": "Trading Data API", "version": "1.0.0"}

@app.get("/api/revenue/total")
async def get_total_revenue(hours: int = Query(24, ge=1, le=168)):
    """ดึงยอดรวมรายได้ใน USD ย้อนหลัง N ชั่วโมง"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT COALESCE(SUM(usd_amount), 0) as total_revenue
            FROM processed_transactions 
            WHERE timestamp >= %s
        """, (datetime.now() - timedelta(hours=hours),))
        
        result = cursor.fetchone()
        return {
            "total_revenue_usd": float(result[0]),
            "period_hours": hours,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error getting total revenue: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/revenue/breakdown/country")
async def get_revenue_by_country(hours: int = Query(24, ge=1, le=168)):
    """ดึงยอดรายได้แยกตามประเทศ"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT country, SUM(usd_amount) as revenue
            FROM processed_transactions 
            WHERE timestamp >= %s
            GROUP BY country
            ORDER BY revenue DESC
        """, (datetime.now() - timedelta(hours=hours),))
        
        results = cursor.fetchall()
        total = sum(row[1] for row in results)
        
        breakdown = []
        for country, revenue in results:
            breakdown.append({
                "category": "country",
                "value": country,
                "usd_amount": float(revenue),
                "percentage": round((float(revenue) / total * 100) if total > 0 else 0, 2)
            })
        
        return {"breakdown": breakdown, "period_hours": hours}
    except Exception as e:
        logger.error(f"Error getting country breakdown: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/revenue/breakdown/currency")
async def get_revenue_by_currency(hours: int = Query(24, ge=1, le=168)):
    """ดึงยอดรายได้แยกตามสกุลเงิน"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT original_currency, SUM(usd_amount) as revenue
            FROM processed_transactions 
            WHERE timestamp >= %s
            GROUP BY original_currency
            ORDER BY revenue DESC
        """, (datetime.now() - timedelta(hours=hours),))
        
        results = cursor.fetchall()
        total = sum(row[1] for row in results)
        
        breakdown = []
        for currency, revenue in results:
            breakdown.append({
                "category": "currency",
                "value": currency,
                "usd_amount": float(revenue),
                "percentage": round((float(revenue) / total * 100) if total > 0 else 0, 2)
            })
        
        return {"breakdown": breakdown, "period_hours": hours}
    except Exception as e:
        logger.error(f"Error getting currency breakdown: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/revenue/breakdown/user")
async def get_revenue_by_user(hours: int = Query(24, ge=1, le=168), limit: int = Query(10, ge=1, le=100)):
    """ดึงยอดรายได้แยกตาม user (top users)"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT user_id, SUM(usd_amount) as revenue
            FROM processed_transactions 
            WHERE timestamp >= %s
            GROUP BY user_id
            ORDER BY revenue DESC
            LIMIT %s
        """, (datetime.now() - timedelta(hours=hours), limit))
        
        results = cursor.fetchall()
        
        # Get total for percentage calculation
        cursor.execute("""
            SELECT SUM(usd_amount) as total
            FROM processed_transactions 
            WHERE timestamp >= %s
        """, (datetime.now() - timedelta(hours=hours),))
        total = float(cursor.fetchone()[0] or 0)
        
        breakdown = []
        for user_id, revenue in results:
            breakdown.append({
                "category": "user",
                "value": user_id,
                "usd_amount": float(revenue),
                "percentage": round((float(revenue) / total * 100) if total > 0 else 0, 2)
            })
        
        return {"breakdown": breakdown, "period_hours": hours, "limit": limit}
    except Exception as e:
        logger.error(f"Error getting user breakdown: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/fx-rates/current")
async def get_current_fx_rates():
    """ดึงอัตราแลกเปลี่ยนปัจจุบัน"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT DISTINCT ON (currency_pair) currency_pair, rate, timestamp
            FROM fx_rates 
            ORDER BY currency_pair, timestamp DESC
        """)
        
        results = cursor.fetchall()
        rates = []
        for currency_pair, rate, timestamp in results:
            rates.append({
                "currency_pair": currency_pair,
                "rate": float(rate),
                "timestamp": timestamp.isoformat()
            })
        
        return {"fx_rates": rates}
    except Exception as e:
        logger.error(f"Error getting FX rates: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/fx-rates/trends")
async def get_fx_trends(currency_pair: str, hours: int = Query(24, ge=1, le=168)):
    """ดึงแนวโน้มอัตราแลกเปลี่ยน"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT rate, timestamp
            FROM fx_rates 
            WHERE currency_pair = %s AND timestamp >= %s
            ORDER BY timestamp
        """, (currency_pair, datetime.now() - timedelta(hours=hours)))
        
        results = cursor.fetchall()
        trends = []
        for rate, timestamp in results:
            trends.append({
                "rate": float(rate),
                "timestamp": timestamp.isoformat()
            })
        
        return {"currency_pair": currency_pair, "trends": trends, "period_hours": hours}
    except Exception as e:
        logger.error(f"Error getting FX trends: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/transactions/hourly")
async def get_hourly_activity(hours: int = Query(24, ge=1, le=168)):
    """ดึงกิจกรรมการขายรายชั่วโมง"""
    try:
        cursor = db.get_cursor()
        cursor.execute("""
            SELECT 
                DATE_TRUNC('hour', timestamp) as hour,
                COUNT(*) as transaction_count,
                SUM(usd_amount) as revenue
            FROM processed_transactions 
            WHERE timestamp >= %s
            GROUP BY DATE_TRUNC('hour', timestamp)
            ORDER BY hour
        """, (datetime.now() - timedelta(hours=hours),))
        
        results = cursor.fetchall()
        activity = []
        for hour, count, revenue in results:
            activity.append({
                "hour": hour.isoformat(),
                "transaction_count": int(count),
                "revenue_usd": float(revenue)
            })
        
        return {"hourly_activity": activity, "period_hours": hours}
    except Exception as e:
        logger.error(f"Error getting hourly activity: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/transactions")
async def get_transactions(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    country: Optional[str] = None,
    currency: Optional[str] = None
):
    """ดึงรายการ transactions"""
    try:
        cursor = db.get_cursor()
        
        where_conditions = []
        params = []
        
        if country:
            where_conditions.append("country = %s")
            params.append(country)
        
        if currency:
            where_conditions.append("original_currency = %s")
            params.append(currency)
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        query = f"""
            SELECT transaction_id, user_id, country, original_currency, 
                   original_amount, usd_amount, fx_rate, timestamp
            FROM processed_transactions 
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
        """
        
        params.extend([limit, offset])
        cursor.execute(query, params)
        
        results = cursor.fetchall()
        transactions = []
        for row in results:
            transactions.append({
                "transaction_id": row[0],
                "user_id": row[1],
                "country": row[2],
                "original_currency": row[3],
                "original_amount": float(row[4]),
                "usd_amount": float(row[5]),
                "fx_rate": float(row[6]),
                "timestamp": row[7].isoformat()
            })
        
        return {"transactions": transactions, "limit": limit, "offset": offset}
    except Exception as e:
        logger.error(f"Error getting transactions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)