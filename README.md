📊 PhonePe Pulse Data Dashboard
A full-stack data visualization project that extracts, processes, and displays rich insights from the PhonePe Pulse dataset using MySQL, Python, and Streamlit.

🚀 Project Description
This project provides an interactive dashboard to explore financial and insurance data such as:

Transaction trends

User distribution by device

Insurance activity

State and district-level insights

The pipeline parses JSON data from the PhonePe Pulse GitHub repository, loads it into a MySQL database, and provides an easy-to-use front-end via a Streamlit app.

📦 Features
✅ Clean and modular data ingestion using batch inserts

✅ Dynamic filtering by year, quarter, and state

✅ Charts using Plotly (bar, pie, and choropleths)

✅ Metrics for:

Transactions (count and amount)

Registered users and mobile brands

Insurance policies and value

✅ Top states/districts/pincodes visualization

🌐 Extensible structure for geographic map layers (commented in dashboard for future enhancements)

🛠️ Tech Stack
Layer	Technology
Backend DB	MySQL
Data Parsing	Python (JSON + os, tqdm)
Dashboard	Streamlit
Visualization	Plotly

📁 Project Structure
bash
Copy
Edit
📦 phonepe-dashboard/
├── pulse/data/                    # Raw JSON data (PhonePe Pulse clone)
├── corrected.py                  # Data loading and MySQL ingestion
├── streamlet.py                  # Streamlit dashboard app
├── README.md                     # Project documentation
