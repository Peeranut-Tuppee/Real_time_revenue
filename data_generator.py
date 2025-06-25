import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import psycopg2
from faker import Faker
import logging
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

fake = Faker()

class DataGenerator:
    def __init__(self):
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Database connection
        self.db_conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="trading_db",
            user="postgres",
            password="12345"
        )
        self.db_conn.autocommit = True
        
        # Currency pairs และ countries
        self.currencies = ['USD', 'EUR', 'GBP', 'JPY', 'THB', 'SGD', 'AUD', 'CAD']
        self.countries = ['US', 'GB', 'DE', 'JP', 'TH', 'SG', 'AU', 'CA', 'FR', 'IT']
        
        # Base FX rates (relative to USD)
        self.base_fx_rates = {
            'USD': 1.0,
            'EUR': 0.85,
            'GBP': 0.73,
            'JPY': 150.0,
            'THB': 35.0,
            'SGD': 1.35,
            'AUD': 1.52,
            'CAD': 1.38
        }
        
    def generate_fx_rates(self):
        """สร้าง FX rates แบบ real-time"""
        cursor = self.db_conn.cursor()
        
        while True:
            try:
                for currency in self.currencies:
                    if currency != 'USD':  # USD is base currency
                        # สร้างความผันผวนของอัตราแลกเปลี่ยน
                        base_rate = self.base_fx_rates[currency]
                        volatility = random.uniform(-0.02, 0.02)  # 2% volatility
                        current_rate = base_rate * (1 + volatility)
                        
                        fx_data = {
                            'currency_pair': f'{currency}/USD',
                            'rate': round(current_rate, 6),
                            'timestamp': datetime.now().isoformat()
                        }
                        
                        # เก็บลง database
                        cursor.execute("""
                            INSERT INTO fx_rates (currency_pair, rate, timestamp) 
                            VALUES (%s, %s, %s)
                        """, (fx_data['currency_pair'], fx_data['rate'], fx_data['timestamp']))
                        
                        # ส่งไป Kafka
                        self.producer.send('fx_rates', fx_data)
                        
                logger.info(f"Generated {len(self.currencies)-1} FX rates")
                time.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Error generating FX rates: {e}")
                time.sleep(5)
    
    def generate_transactions(self):
        """สร้าง transaction data แบบ hourly batches"""
        cursor = self.db_conn.cursor()
        
        while True:
            try:
                # สร้าง batch ของ transactions (10-50 transactions per batch)
                batch_size = random.randint(10, 50)
                transactions = []
                
                for _ in range(batch_size):
                    transaction = {
                        'transaction_id': str(uuid.uuid4()),
                        'user_id': f'user_{random.randint(1000, 9999)}',
                        'country': random.choice(self.countries),
                        'currency': random.choice(self.currencies),
                        'amount': round(random.uniform(10, 5000), 2),
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    # เก็บลง database
                    cursor.execute("""
                        INSERT INTO raw_transactions (transaction_id, user_id, country, currency, amount, timestamp) 
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (transaction['transaction_id'], transaction['user_id'], 
                          transaction['country'], transaction['currency'], 
                          transaction['amount'], transaction['timestamp']))
                    
                    transactions.append(transaction)
                
                # ส่ง batch ไป Kafka
                batch_data = {
                    'batch_id': str(uuid.uuid4()),
                    'transactions': transactions,
                    'batch_timestamp': datetime.now().isoformat()
                }
                
                self.producer.send('transaction_batches', batch_data)
                logger.info(f"Generated transaction batch with {batch_size} transactions")
                
                # รอ 1-5 นาที ก่อน batch ถัดไป
                time.sleep(random.randint(60, 300))
                
            except Exception as e:
                logger.error(f"Error generating transactions: {e}")
                time.sleep(5)
    
    def start_generation(self):
        """เริ่ม generate ข้อมูลใน separate threads"""
        fx_thread = threading.Thread(target=self.generate_fx_rates)
        tx_thread = threading.Thread(target=self.generate_transactions)
        
        fx_thread.daemon = True
        tx_thread.daemon = True
        
        fx_thread.start()
        tx_thread.start()
        
        logger.info("Data generation started...")
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopping data generation...")

if __name__ == "__main__":
    generator = DataGenerator()
    generator.start_generation()