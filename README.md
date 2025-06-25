"# Real_time_revenue" 
1. Set Up the Environment
conda create -n trading-pipeline python=3.9  
conda activate trading-pipeline
pip install psycopg2-binary kafka-python pandas numpy streamlit plotly requests fastapi uvicorn sqlalchemy asyncpg faker schedule  

2.Run the following command to start services
docker-compose up -d  

3. Start the System in 2 Terminals
In the first terminal, run:  python startup_guide.py  
In the second terminal, run:  streamlit run dashboard.py

4. Access the System
Dashboard: http://localhost:8501
API Documentation: http://localhost:8000/docs
API Endpoints: http://localhost:8000/api/

5. Dashboard
Total revenue in USD (last 24 hours)
Revenue breakdown by country, currency, user
FX rate changes and trends
Hourly sales activity
Real-time auto-refresh

6. API Endpoints
GET /api/revenue/total — Get total revenue
GET /api/revenue/breakdown/{type} — Get revenue breakdown by country, currency, or user
GET /api/fx-rates/current — Get current foreign exchange rates
GET /api/fx-rates/trends — Get foreign exchange rate trends
GET /api/transactions/hourly — Get hourly transaction activity
GET /api/transactions — Get list of transactions
