# Import required libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
import numpy as np

# ------------------- Database Connection -------------------
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password",
    database="ecommerce"
)

cur = db.cursor()

# ------------------- 1. Fetch distinct customer cities -------------------
query = "SELECT DISTINCT customer_city FROM customers"
cur.execute(query)
data = cur.fetchall()
df_cities = pd.DataFrame(data, columns=["customer_city"])
print(df_cities.head())

# ------------------- 2. Count total orders placed in 2017 -------------------
query = "SELECT COUNT(order_id) FROM orders WHERE YEAR(order_purchase_timestamp) = 2017"
cur.execute(query)
data = cur.fetchall()
print("Total orders placed in 2017:", data[0][0])

# ------------------- 3. Sales by product category -------------------
query = """
SELECT UPPER(products.product_category) AS category, 
       ROUND(SUM(payments.payment_value), 2) AS sales
FROM products 
JOIN order_items ON products.product_id = order_items.product_id
JOIN payments ON payments.order_id = order_items.order_id
GROUP BY category
"""
cur.execute(query)
data = cur.fetchall()
df_sales = pd.DataFrame(data, columns=["Category", "Sales"])
print(df_sales)

# ------------------- 4. Percentage of orders paid in installments -------------------
query = """
SELECT ((SUM(CASE WHEN payment_installments >= 1 THEN 1 ELSE 0 END)) / COUNT(*)) * 100
FROM payments
"""
cur.execute(query)
data = cur.fetchall()
print("Percentage of orders paid in installments:", data[0][0])

# ------------------- 5. Count of customers by state -------------------
query = "SELECT customer_state, COUNT(customer_id) FROM customers GROUP BY customer_state"
cur.execute(query)
data = cur.fetchall()
df_state = pd.DataFrame(data, columns=["State", "Customer_Count"])
df_state = df_state.sort_values(by="Customer_Count", ascending=False)

# Plot bar chart
plt.figure(figsize=(10, 4))
plt.bar(df_state["State"], df_state["Customer_Count"])
plt.xticks(rotation=90)
plt.xlabel("States")
plt.ylabel("Customer Count")
plt.title("Count of Customers by States")
plt.tight_layout()
plt.savefig("customers_by_state.png")
plt.close()

# ------------------- 6. Monthly orders in 2018 -------------------
query = """
SELECT MONTHNAME(order_purchase_timestamp) AS months, COUNT(order_id) AS order_count
FROM orders
WHERE YEAR(order_purchase_timestamp) = 2018
GROUP BY months
"""
cur.execute(query)
data = cur.fetchall()
df_monthly = pd.DataFrame(data, columns=["Months", "Order_Count"])

# Ensure correct month order
month_order = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

plt.figure(figsize=(10, 4))
ax = sns.barplot(x="Months", y="Order_Count", data=df_monthly, order=month_order, color="red")
plt.xticks(rotation=45)
ax.bar_label(ax.containers[0])
plt.title("Count of Orders by Month in 2018")
plt.tight_layout()
plt.savefig("monthly_orders_2018.png")
plt.close()

# ------------------- 7. Average products per order per city -------------------
query = """
WITH count_per_order AS (
    SELECT orders.order_id, orders.customer_id, COUNT(order_items.order_id) AS oc
    FROM orders 
    JOIN order_items ON orders.order_id = order_items.order_id
    GROUP BY orders.order_id, orders.customer_id
)
SELECT customers.customer_city, ROUND(AVG(count_per_order.oc), 2) AS average_orders
FROM customers 
JOIN count_per_order ON customers.customer_id = count_per_order.customer_id
GROUP BY customers.customer_city
ORDER BY average_orders DESC
"""
cur.execute(query)
data = cur.fetchall()
df_avg_order = pd.DataFrame(data, columns=["Customer City", "Average Products/Order"])
print(df_avg_order.head(10))

# ------------------- 8. Sales percentage by category -------------------
query = """
SELECT UPPER(products.product_category) AS category, 
ROUND((SUM(payments.payment_value) / (SELECT SUM(payment_value) FROM payments)) * 100, 2) AS sales_percentage
FROM products 
JOIN order_items ON products.product_id = order_items.product_id
JOIN payments ON payments.order_id = order_items.order_id
GROUP BY category 
ORDER BY sales_percentage DESC
"""
cur.execute(query)
data = cur.fetchall()
df_sales_pct = pd.DataFrame(data, columns=["Category", "Percentage Distribution"])
print(df_sales_pct.head())

# ------------------- 9. Correlation between order count and price -------------------
query = """
SELECT products.product_category, COUNT(order_items.product_id) AS order_count,
ROUND(AVG(order_items.price), 2) AS price
FROM products 
JOIN order_items ON products.product_id = order_items.product_id
GROUP BY products.product_category
"""
cur.execute(query)
data = cur.fetchall()
df_corr = pd.DataFrame(data, columns=["Category", "Order_Count", "Price"])

arr1 = df_corr["Order_Count"]
arr2 = df_corr["Price"]
corr = np.corrcoef(arr1, arr2)[0, 1]
print("Correlation between order count and price:", corr)

# ------------------- 10. Top sellers by revenue -------------------
query = """
SELECT *, DENSE_RANK() OVER (ORDER BY revenue DESC) AS rn 
FROM (
    SELECT order_items.seller_id, SUM(payments.payment_value) AS revenue 
    FROM order_items 
    JOIN payments ON order_items.order_id = payments.order_id
    GROUP BY order_items.seller_id
) AS a
"""
cur.execute(query)
data = cur.fetchall()
df_sellers = pd.DataFrame(data, columns=["Seller_ID", "Revenue", "Rank"]).head(10)

plt.figure(figsize=(10, 4))
sns.barplot(x="Seller_ID", y="Revenue", data=df_sellers)
plt.xticks(rotation=90)
plt.title("Top Sellers by Revenue")
plt.tight_layout()
plt.savefig("top_sellers.png")
plt.close()
# ------------------- 11. Moving average of order values for each customer -------------------
query = """
SELECT order_id,
       order_purchase_timestamp,
       pay,
       AVG(pay) OVER (
           PARTITION BY order_id
           ORDER BY order_purchase_timestamp
           ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS mov_avg
FROM (
    SELECT od.order_id, od.order_purchase_timestamp, py.payment_value AS pay
    FROM orders od
    INNER JOIN payments py ON od.order_id = py.order_id
) AS a
"""
cur.execute(query)
data = cur.fetchall()
df_mov_avg = pd.DataFrame(data, columns=["Order_ID", "Order_Timestamp", "Payment", "Moving_Avg"])
print("\n--- Moving Average of Order Values ---")
print(df_mov_avg.head(10))


# ------------------- 12. Cumulative sales per month for each year -------------------
query = """
SELECT *,
       ROUND(SUM(paym) OVER (ORDER BY years, months), 2) AS cumulative_sales
FROM (
    SELECT YEAR(od.order_purchase_timestamp) AS years,
           MONTH(od.order_purchase_timestamp) AS month_num,
           MONTHNAME(od.order_purchase_timestamp) AS months,
           ROUND(SUM(pay.payment_value), 2) AS paym
    FROM orders od
    INNER JOIN payments pay ON od.order_id = pay.order_id
    GROUP BY years, month_num, months
    ORDER BY years, month_num
) AS a
"""
cur.execute(query)
data = cur.fetchall()
df_cum_sales = pd.DataFrame(data, columns=["Year", "Month_Num", "Month", "Sales", "Cumulative_Sales"])
print("\n--- Cumulative Sales ---")
print(df_cum_sales.head(12))

plt.figure(figsize=(12, 5))
sns.lineplot(x="Month_Num", y="Cumulative_Sales", hue="Year", data=df_cum_sales, marker="o")
plt.xticks(ticks=range(1, 13), labels=[
    "Jan","Feb","Mar","Apr","May","Jun",
    "Jul","Aug","Sep","Oct","Nov","Dec"
])
plt.title("Cumulative Sales per Month by Year")
plt.xlabel("Months")
plt.ylabel("Cumulative Sales")
plt.tight_layout()
plt.savefig("cumulative_sales.png")
plt.close()
 

# ------------------- 13. Year-over-Year Growth Rate of Total Sales -------------------
query = """
WITH a AS (
    SELECT YEAR(od.order_purchase_timestamp) AS years,
           ROUND(SUM(pay.payment_value), 3) AS total_sales
    FROM orders od
    INNER JOIN payments pay ON od.order_id = pay.order_id
    GROUP BY years
    ORDER BY years
)
SELECT *,
       LAG(total_sales, 1) OVER (ORDER BY years) AS previous_sale,
       ROUND(((total_sales - LAG(total_sales, 1) OVER (ORDER BY years)) 
             / LAG(total_sales, 1) OVER (ORDER BY years)) * 100, 3) AS percentage
FROM a
"""
cur.execute(query)
data = cur.fetchall()
df_yoy = pd.DataFrame(data, columns=["Year", "Total_Sales", "Previous_Sale", "Growth_Percentage"])



# ------------------- 14. Top 3 customers who spent the most in each year -------------------
query = """
WITH ranked AS (
    SELECT 
        YEAR(od.order_purchase_timestamp) AS years,
        od.customer_id,
        ROUND(SUM(pay.payment_value), 2) AS money_spent,
        DENSE_RANK() OVER (
            PARTITION BY YEAR(od.order_purchase_timestamp)
            ORDER BY SUM(pay.payment_value) DESC
        ) AS d_rank
    FROM orders od
    INNER JOIN payments pay 
        ON od.order_id = pay.order_id
    GROUP BY YEAR(od.order_purchase_timestamp), od.customer_id
)
SELECT *
FROM ranked
WHERE d_rank <= 3
ORDER BY years, d_rank, money_spent DESC
"""
cur.execute(query)
data = cur.fetchall()
df_top_customers = pd.DataFrame(data, columns=["Year", "Customer_ID", "Money_Spent", "Rank"])
print("\n--- Top 3 Customers by Year ---")
print(df_top_customers.head(15))


# ------------------- Close DB connection -------------------
cur.close()
db.close()
