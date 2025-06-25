import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database_schema():
    """สร้าง database schema สำหรับเก็บข้อมูล transaction และ FX rates"""
    
    # Database connection
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="trading_db",
        user="postgres",
        password="12345"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    # สร้างตาราง FX Rates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fx_rates (
            id SERIAL PRIMARY KEY,
            currency_pair VARCHAR(7) NOT NULL,
            rate DECIMAL(10,6) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(currency_pair, timestamp)
        )
    """)
    
    # สร้างตาราง Raw Transactions
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_transactions (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(50) UNIQUE NOT NULL,
            user_id VARCHAR(20) NOT NULL,
            country VARCHAR(3) NOT NULL,
            currency VARCHAR(3) NOT NULL,
            amount DECIMAL(15,2) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # สร้างตาราง Processed Transactions (USD converted)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS processed_transactions (
            id SERIAL PRIMARY KEY,
            transaction_id VARCHAR(50) UNIQUE NOT NULL,
            user_id VARCHAR(20) NOT NULL,
            country VARCHAR(3) NOT NULL,
            original_currency VARCHAR(3) NOT NULL,
            original_amount DECIMAL(15,2) NOT NULL,
            usd_amount DECIMAL(15,2) NOT NULL,
            fx_rate DECIMAL(10,6) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # สร้าง Index สำหรับ query performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_transactions_timestamp ON raw_transactions(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_transactions_timestamp ON processed_transactions(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_fx_rates_timestamp ON fx_rates(timestamp)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_processed_transactions_country ON processed_transactions(country)")
    
    logger.info("Database schema created successfully")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_database_schema()