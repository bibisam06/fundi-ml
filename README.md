💶 FUNDI - Funding Rate & Price Analysis
📌 Overview
FUNDI is a data-driven project that analyzes the relationship between cryptocurrency futures funding rates and market price movements.
The goal of this project is to explore whether funding rate signals can provide meaningful insights into market sentiment and future price trends.
⚙️ Tech Stack
Python
Pandas / NumPy
Binance Futures API
Matplotlib / Plotly (for visualization)
📂 Features
🔹 Data Collection
Collects funding rate history from Binance Futures
Collects candlestick (kline) price data
Handles pagination for large time ranges
Stores data in CSV format
🔹 Data Processing
Converts timestamps to UTC datetime format
Cleans and normalizes numeric fields
Removes duplicate records
Aligns funding rate data with price data
📊 Project Structure
project-root/
├── data/
│   └── raw/
│       ├── btcusdt_funding_rate.csv
│       └── btcusdt_1h_klines.csv
├── src/
│   └── data_collector.py
└── README.md
🧠 Key Idea
Funding rates reflect the imbalance between long and short positions in the market.
Positive funding rate → More long positions
Negative funding rate → More short positions
This project investigates:
Can extreme funding rates signal potential price reversals?
🚀 Future Work
Merge funding rate and price datasets
Feature engineering (lagged returns, volatility, etc.)
Backtesting trading strategies
Machine learning models for prediction
📈 Example Use Case
Detect over-leveraged market conditions
Identify potential trend reversals
Build quantitative trading signals
⚠️ Disclaimer
This project is for research and educational purposes only.
It does not constitute financial advice.
👤 Author
Bibi (Backend Developer & Quant Enthusiast)
