#!/usr/bin/env python3
"""
การติดตั้งและเริ่มต้นระบบ Trading Pipeline
รันสคริปต์นี้เพื่อเริ่มต้นระบบทั้งหมด
"""

import subprocess
import time
import os
import psycopg2
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_dependencies():
    """ตรวจสอบ dependencies ที่จำเป็น"""
    required_packages = [
        'psycopg2', 'kafka-python', 'pandas', 'numpy', 
        'streamlit', 'plotly', 'requests', 'fastapi', 
        'uvicorn', 'sqlalchemy', 'faker'
    ]
    
    missing_packages = []
    for package in required_packages:
        try:
            __import__(package if package != 'kafka-python' else 'kafka')
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        logger.error(f"Missing packages: {missing_packages}")
        logger.info("Please install missing packages using:")
        logger.info(f"pip install {' '.join(missing_packages)}")
        return False
    
    logger.info("All dependencies are installed")
    return True


def setup_kafka_topics():
    """สร้าง Kafka topics ที่จำเป็น"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=['localhost:9092'],
            client_id='setup_script'
        )
        
        topics = [
            NewTopic(name='transaction_batches', num_partitions=3, replication_factor=1),
            NewTopic(name='fx_rates', num_partitions=1, replication_factor=1)
        ]
        
        # ลบ topics เก่าถ้ามี
        existing_topics = admin_client.list_topics()
        topic_names = [topic.name for topic in topics]
        
        for topic_name in topic_names:
            if topic_name in existing_topics:
                admin_client.delete_topics([topic_name])
                logger.info(f"Deleted existing topic: {topic_name}")
        
        time.sleep(2)  # รอให้ topics ถูกลบ
        
        # สร้าง topics ใหม่
        admin_client.create_topics(topics)
        logger.info("Kafka topics created successfully")
        
    except Exception as e:
        logger.error(f"Error setting up Kafka topics: {e}")
        return False
    
    return True

def wait_for_services():
    """รอให้ PostgreSQL และ Kafka พร้อมใช้งาน"""
    logger.info("Waiting for services to be ready...")
    
    # รอ PostgreSQL
    postgres_ready = False
    for i in range(30):
        try:
            conn = psycopg2.connect(
                host="localhost",
                port="5432",
                database="trading_db",
                user="postgres",
                password="12345"
            )
            conn.close()
            postgres_ready = True
            logger.info("PostgreSQL is ready")
            break
        except:
            time.sleep(2)
    
    if not postgres_ready:
        logger.error("PostgreSQL is not ready")
        return False
    
    # รอ Kafka
    kafka_ready = False
    for i in range(30):
        try:
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            producer.close()
            kafka_ready = True
            logger.info("Kafka is ready")
            break
        except:
            time.sleep(2)
    
    if not kafka_ready:
        logger.error("Kafka is not ready")
        return False
    
    return True

def setup_database():
    """ติดตั้ง database schema"""
    try:
        from database_setup import create_database_schema
        create_database_schema()
        logger.info("Database schema created successfully")
        return True
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False

def start_services():
    """เริ่มต้นบริการต่างๆ"""
    logger.info("Starting services...")
    
    # เริ่ม Data Generator
    logger.info("Starting data generator...")
    data_gen_process = subprocess.Popen(['python', 'data_generator.py'])
    
    time.sleep(5)  # รอให้ generator เริ่มทำงาน
    
    # เริ่ม Data Processor
    logger.info("Starting data processor...")
    processor_process = subprocess.Popen(['python', 'data_processor.py'])
    
    time.sleep(3)  # รอให้ processor เริ่มทำงาน
    
    # เริ่ม API Server
    logger.info("Starting API server...")
    api_process = subprocess.Popen(['python', 'api_server.py'])
    
    time.sleep(5)  # รอให้ API server เริ่มทำงาน
    
    logger.info("All services started successfully!")
    logger.info("API Server: http://localhost:8000")
    logger.info("API Docs: http://localhost:8000/docs")
    
    return {
        'data_generator': data_gen_process,
        'data_processor': processor_process,
        'api_server': api_process
    }

def main():
    """เริ่มต้นระบบทั้งหมด"""
    logger.info("=== Trading Pipeline Setup ===")
    
    # 1. ตรวจสอบ dependencies
    if not check_dependencies():
        return
    
    # 2. รอให้บริการพร้อม
    if not wait_for_services():
        logger.error("Services are not ready. Please check Docker containers.")
        return
    
    # 3. ติดตั้ง Kafka topics
    if not setup_kafka_topics():
        logger.error("Failed to setup Kafka topics")
        return
    
    # 4. ติดตั้ง database
    if not setup_database():
        logger.error("Failed to setup database")
        return
    
    # 5. เริ่มต้นบริการ
    processes = start_services()
    
    logger.info("\n=== System Status ===")
    logger.info("✅ Database: Ready")
    logger.info("✅ Kafka: Ready")
    logger.info("✅ Data Generator: Running")
    logger.info("✅ Data Processor: Running")
    logger.info("✅ API Server: Running")
    
    logger.info("\n=== Next Steps ===")
    logger.info("1. Open new terminal and run: streamlit run dashboard.py")
    logger.info("2. Dashboard will be available at: http://localhost:8501")
    logger.info("3. API documentation at: http://localhost:8000/docs")
    
    logger.info("\n=== Monitoring ===")
    logger.info("Press Ctrl+C to stop all services")
    
    try:
        # รอและ monitor processes
        while True:
            time.sleep(10)
            
            # ตรวจสอบว่า processes ยังทำงานอยู่หรือไม่
            for name, process in processes.items():
                if process.poll() is not None:
                    logger.warning(f"{name} has stopped unexpectedly")
            
    except KeyboardInterrupt:
        logger.info("\nStopping all services...")
        
        for name, process in processes.items():
            process.terminate()
            logger.info(f"Stopped {name}")
        
        logger.info("All services stopped")

if __name__ == "__main__":
    main()