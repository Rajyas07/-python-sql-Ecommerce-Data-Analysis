# -python-sql-Ecommerce-Data-Analysis
This project demonstrates end-to-end handling of an E-commerce dataset. - Load raw CSV data into a MySQL database using Python (ETL process). - Perform SQL queries for business insights (cumulative sales, YoY growth, top customers, etc.). - Visualize results using Matplotlib and Seaborn

file details
📄 ecommerce_dataset_link.txt       # Raw CSV files from kaggle (customers, orders, products, order_items, payments, geolocations)
📄 Questions              #question of sql qurries 
 -> csv_to_sql.py     # Python scripts for loading csv to sql  
📄 project_2.py     # python scripts for sql qurries and visualization
📄solution_qurries  # sql qurries in mysql   
┣ 📂 outputs/ # Graphs & visualizations
┣ 📄 README.md # Project documentation


⚡ Features
- Load CSV data into **MySQL** using Python (ETL process).
- Perform SQL queries for business insights:
  - ✅ Cumulative sales by month/year  
  - ✅ Year-over-Year growth rate  
  - ✅ Moving average of order values  
  - ✅ Top 3 customers per year  
- Visualize results using **Matplotlib & Seaborn**.

---

## 🛠️ Technologies Used
- **Python** → pandas, matplotlib, seaborn, mysql-connector  
- **MySQL** → database storage & queries  
- **Pandas** → data manipulation  
- **Seaborn / Matplotlib** → data visualization  

---

## 📊 Example Visualizations
- 📈 **Cumulative Sales (Bar Plot)**  
- 📉 **YoY Growth Rate (Line + KDE)**  
- 🏆 **Top Customers (Ranking by Year)**  

*(see `outputs/` folder for saved graphs)*

---

## 🚀 Setup & Usage

### 1️⃣ Clone the Repository
```bash
git clone https://github.com/<your-username>/Ecommerce-Analysis.git
cd Ecommerce-Analysis
