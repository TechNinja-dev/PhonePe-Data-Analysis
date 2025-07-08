# 📊 PhonePe Pulse Data Dashboard

An end-to-end data visualization project using PhonePe Pulse data, MySQL, and Streamlit to analyze and display financial and insurance metrics across India.

---

## 📌 Overview

This project extracts structured JSON data from the [PhonePe Pulse](https://www.phonepe.com/pulse/) GitHub repository, stores it in a MySQL database, and visualizes it through an interactive Streamlit dashboard.

---

## 🧰 Tech Stack

- **Backend:** MySQL
- **Scripting:** Python
- **Data Parsing:** JSON, `os`, `tqdm`
- **Visualization:** Plotly, Streamlit
- **Dashboard:** Streamlit UI with filters and tabs

---

## 📁 Project Structure

project/
├── corrected.py # Data loader script to populate MySQL tables
├── streamlet.py # Streamlit app for interactive dashboard
├── pulse/data/ # Folder containing PhonePe Pulse JSON data
├── README.md # Project documentation (this file)