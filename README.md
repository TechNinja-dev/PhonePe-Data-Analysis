# 📊 PhonePe Pulse Data Dashboard

An end-to-end data visualization project built with Python, MySQL, and Streamlit that leverages the PhonePe Pulse dataset to showcase insights into digital payments, user adoption, and insurance penetration across India.

---

## 📌 Overview

This project extracts JSON data from the [PhonePe Pulse GitHub](https://github.com/PhonePe/pulse), loads it into a MySQL database, and visualizes it using an interactive Streamlit dashboard. Users can explore aggregated data related to transactions, users, and insurance metrics across states, years, and quarters.

---

## 🧰 Tech Stack

- **Database:** MySQL  
- **Backend (ETL):** Python (with `os`, `json`, `tqdm`, `mysql-connector-python`)  
- **Frontend (Dashboard):** Streamlit  
- **Visualization:** Plotly & Plotly Express  
- **Data Source:**  https://github.com/PhonePe/pulse.git

---

## 📁 Project Structure

```
project/
├── corrected.py         # Data loader script to populate MySQL tables
├── streamlet.py         # Streamlit app for interactive dashboard
├── pulse/data/          # Folder containing PhonePe Pulse JSON data
├── README.md            # Project documentation (this file)
```

---

## ⚙️ Setup Instructions

### 1. Clone Repository and Prepare Data

```bash
git clone https://github.com/TechNinja-dev/PhonePe-Data-Analysis.git
cd PhonePe-Data-Analysis
```

Clone PhonePe Pulse data:

```bash
git clone https://github.com/PhonePe/pulse.git
mv pulse/data ./pulse/data
```

### 2. Setup MySQL

Login to MySQL and run:

```sql
CREATE DATABASE phonepe;
```

Update the DB credentials in both `corrected.py` and `streamlet.py` if necessary.

### 3. Install Python Dependencies

```bash
pip install streamlit mysql-connector-python pandas plotly tqdm
```

### 4. Load Data into MySQL

```bash
python corrected.py
```

This will:
- Drop old tables (if any)
- Create new tables
- Load all JSON data into MySQL using efficient batch inserts

### 5. Launch the Dashboard

```bash
streamlit run streamlet.py
```

---

## ✅ Features

- 📅 Dynamic filtering by Year, Quarter, and State  
- 📈 Transaction metrics: count, amount, and type breakdown  
- 👥 User insights: device brand share and top districts  
- 🛡️ Insurance data: policy count, amount, and type split  
- 📊 Rich, interactive visualizations using Plotly  

---



