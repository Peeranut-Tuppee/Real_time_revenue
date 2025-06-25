"# Real_time_revenue" 
1. Set Up the Environment
    conda create -n trading-pipeline python=3.9  
    conda activate trading-pipeline
    pip install psycopg2-binary kafka-python pandas numpy streamlit plotly requests fastapi uvicorn sqlalchemy asyncpg faker schedule

2. Set Up Infrastructure
    Copy the file docker-compose.yml to your project folder.
    Then run:
    docker-compose up -d
  
3. Create the Database
    Run this file to create a database and tables:
    python database_setup.py

4. Generate Sample Data
    Run the data generator script:
    python data_generator.py
   Check if the data is inserted correctly using SQL queries in PostgreSQL:
    SELECT * FROM raw_transactions;
    SELECT * FROM fx_rates;

5. Process the Data
    Run the processor script to convert currencies to USD:
      python data_processor.py
    Check the processed data:
      SELECT * FROM processed_transactions;

6. Run the API Server
    python api_server.py

7. Run the Dashboard
    streamlit run dashboard.py

✅ Alternative Quick Start (2 Terminals)
In Terminal 1:
  python startup_guide.py
In Terminal 2:
  streamlit run dashboard.py
  

Access the System
Dashboard: http://localhost:8501
API Documentation: http://localhost:8000/docs
API Endpoints: http://localhost:8000/api/

Dashboard
Total revenue in USD (last 24 hours)
Revenue breakdown by country, currency, user
FX rate changes and trends
Hourly sales activity
Real-time auto-refresh

API Endpoints
GET /api/revenue/total — Get total revenue
GET /api/revenue/breakdown/{type} — Get revenue breakdown by country, currency, or user
GET /api/fx-rates/current — Get current foreign exchange rates
GET /api/fx-rates/trends — Get foreign exchange rate trends
GET /api/transactions/hourly — Get hourly transaction activity
GET /api/transactions — Get list of transactions

System architecture diagram 
Transaction Generator , FX Rates Generator ──▶ PostgreSQL Data Base ──▶ Data Pipeline ──▶ API Server ──▶ Streamlit Dashboard

