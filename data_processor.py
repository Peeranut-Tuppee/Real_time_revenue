import json
import logging
from kafka import KafkaConsumer
import psycopg2
from datetime import datetime, timedelta
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CurrencyProcessor:
    def __init__(self):
        # Database connection
        self.db_conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="trading_db",
            user="postgres",
            password="12345"
        )
        self.db_conn.autocommit = True
        
        # Kafka Consumer
        self.consumer = KafkaConsumer(
            'transaction_batches',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='currency_processor'
        )
        
        # Cache for latest FX rates
        self.fx_rates_cache = {}
        self.update_fx_cache()
        
        # Start FX rates updater
        fx_updater = threading.Thread(target=self.fx_rates_updater)
        fx_updater.daemon = True
        fx_updater.start()
    
    def update_fx_cache(self):
        """อัพเดท FX rates cache จาก database"""
        cursor = self.db_conn.cursor()
        
        # ดึง FX rates ล่าสุดของแต่ละ currency pair
        cursor.execute("""
            SELECT DISTINCT ON (currency_pair) currency_pair, rate, timestamp
            FROM fx_rates 
            ORDER BY currency_pair, timestamp DESC
        """)
        
        rates = cursor.fetchall()
        for currency_pair, rate, timestamp in rates:
            self.fx_rates_cache[currency_pair] = {
                'rate': float(rate),
                'timestamp': timestamp
            }
        
        logger.info(f"Updated FX cache with {len(self.fx_rates_cache)} rates")
    
    def fx_rates_updater(self):
        """อัพเดท FX cache ทุก 30 วินาที"""
        import time
        while True:
            time.sleep(30)
            self.update_fx_cache()
    
    def get_usd_rate(self, currency):
        """ดึงอัตราแลกเปลี่ยนเป็น USD"""
        if currency == 'USD':
            return 1.0
        
        currency_pair = f"{currency}/USD"
        if currency_pair in self.fx_rates_cache:
            rate_data = self.fx_rates_cache[currency_pair]
            # ตรวจสอบว่า rate ไม่เก่าเกิน 1 ชั่วโมง
            if (datetime.now() - rate_data['timestamp']).seconds < 3600:
                return rate_data['rate']
        
        # ถ้าไม่มี rate หรือ rate เก่าเกินไป ใช้ default rate
        default_rates = {
            'EUR': 0.85, 'GBP': 0.73, 'JPY': 150.0, 
            'THB': 35.0, 'SGD': 1.35, 'AUD': 1.52, 'CAD': 1.38
        }
        return default_rates.get(currency, 1.0)
    
    def convert_to_usd(self, amount, currency):
        """แปลงจำนวนเงินเป็น USD"""
        if currency == 'USD':
            return float(amount), 1.0
        
        fx_rate = self.get_usd_rate(currency)
        usd_amount = float(amount) / fx_rate
        return usd_amount, fx_rate
    
    def process_transaction_batch(self, batch_data):
        """ประมวลผล transaction batch และแปลงเป็น USD"""
        cursor = self.db_conn.cursor()
        processed_count = 0
        
        for transaction in batch_data['transactions']:
            try:
                # แปลงเป็น USD
                usd_amount, fx_rate = self.convert_to_usd(
                    transaction['amount'], 
                    transaction['currency']
                )
                
                # เก็บลง processed_transactions table
                cursor.execute("""
                    INSERT INTO processed_transactions 
                    (transaction_id, user_id, country, original_currency, 
                     original_amount, usd_amount, fx_rate, timestamp) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (transaction_id) DO NOTHING
                """, (
                    transaction['transaction_id'],
                    transaction['user_id'],
                    transaction['country'],
                    transaction['currency'],
                    transaction['amount'],
                    round(usd_amount, 2),
                    fx_rate,
                    transaction['timestamp']
                ))
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing transaction {transaction['transaction_id']}: {e}")
        
        logger.info(f"Processed {processed_count} transactions from batch {batch_data['batch_id']}")
    
    def start_processing(self):
        """เริ่มประมวลผล transaction batches จาก Kafka"""
        logger.info("Starting currency processor...")
        
        try:
            for message in self.consumer:
                batch_data = message.value
                self.process_transaction_batch(batch_data)
                
        except KeyboardInterrupt:
            logger.info("Stopping currency processor...")
        finally:
            self.consumer.close()
            self.db_conn.close()

if __name__ == "__main__":
    processor = CurrencyProcessor()
    processor.start_processing()