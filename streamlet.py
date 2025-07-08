import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Database connection
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="phonepe"
    )

# Page configuration
st.set_page_config(
    page_title="PhonePe Pulse Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        background-color: #F5F5F5;
    }
    .stButton>button {
        background-color: #5E35B1;
        color: white;
    }
    .stSelectbox>div>div>select {
        background-color: #EDE7F6;
    }
    .metric-card {
        background-color: #FFFFFF;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    </style>
    """, unsafe_allow_html=True)

# Title and description
st.title("📊 PhonePe Pulse Data Dashboard")
st.markdown("""
    This dashboard provides insights into PhonePe transactions, users, and insurance data across India.
    Explore the visualizations below to understand payment trends, user behavior, and regional patterns.
    """)

# Sidebar filters
st.sidebar.header("Filters")
conn = connect_db()
cursor = conn.cursor()

# Get unique years and quarters
cursor.execute("SELECT DISTINCT year FROM aggregated_transaction ORDER BY year")
years = [row[0] for row in cursor.fetchall()]
selected_year = st.sidebar.selectbox("Select Year", years)

cursor.execute("SELECT DISTINCT quarter FROM aggregated_transaction WHERE year = %s ORDER BY quarter", (selected_year,))
quarters = [row[0] for row in cursor.fetchall()]
selected_quarter = st.sidebar.selectbox("Select Quarter", quarters)

# Get available states
cursor.execute("SELECT DISTINCT state FROM aggregated_transaction ORDER BY state")
states = ["All States"] + [row[0] for row in cursor.fetchall()]
selected_state = st.sidebar.selectbox("Select State", states)

# Main tabs
tab1, tab2, tab3= st.tabs(["📈 Transactions", "👥 Users", "🛡️ Insurance"])

with tab1:
    st.header("Transaction Insights")
    
    # Transaction metrics with state filter
    query = """
        SELECT SUM(transaction_count), SUM(transaction_amount)
        FROM aggregated_transaction
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    cursor.execute(query, params)
    total_count, total_amount = cursor.fetchone()
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Transactions", f"{total_count:,}" if total_count else "0")
    with col2:
        st.metric("Total Transaction Value", f"₹{total_amount:,.2f}" if total_amount else "₹0.00")
    
    # Transaction type breakdown with state filter
    st.subheader("Transaction Type Breakdown")
    query = """
        SELECT transaction_type, SUM(transaction_count), SUM(transaction_amount)
        FROM aggregated_transaction
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    query += " GROUP BY transaction_type"
    
    cursor.execute(query, params)
    trans_data = cursor.fetchall()
    
    if trans_data:
        trans_df = pd.DataFrame(trans_data, columns=["Type", "Count", "Amount"])
        
        fig1 = px.pie(trans_df, values="Count", names="Type", 
                     title="Transaction Count by Type",
                     hole=0.4)
        fig1.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig1, use_container_width=True)
        
        fig2 = px.bar(trans_df, x="Type", y="Amount",
                     title="Transaction Amount by Type",
                     color="Type")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No transaction data available for selected filters")
    
    # State-wise transactions with state filter
    st.subheader("State-wise Transactions")
    query = """
        SELECT state, SUM(transaction_count) as count, SUM(transaction_amount) as amount
        FROM aggregated_transaction
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    query += " GROUP BY state ORDER BY amount DESC LIMIT 10"
    
    cursor.execute(query, params)
    state_data = cursor.fetchall()
    
    if state_data:
        state_df = pd.DataFrame(state_data, columns=["State", "Count", "Amount"])
        
        fig3 = px.bar(state_df, x="State", y="Amount", 
                     title="Top States by Transaction Amount",
                     color="Amount", color_continuous_scale="Viridis")
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("No state data available for selected filters")

with tab2:
    st.header("User Insights")
    
    # User metrics with state filter
    query = """
        SELECT SUM(user_count)
        FROM aggregated_user
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    cursor.execute(query, params)
    user_result = cursor.fetchone()
    total_users = user_result[0] if user_result and user_result[0] is not None else 0
    
    st.metric("Total Users", f"{total_users:,}" if total_users else "0")
    
    # Brand share with state filter
    st.subheader("Mobile Brand Share")
    query = """
        SELECT brand, SUM(user_count) as users
        FROM aggregated_user
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    query += " GROUP BY brand ORDER BY users DESC LIMIT 10"
    
    cursor.execute(query, params)
    brand_data = cursor.fetchall()
    
    if brand_data:
        brand_df = pd.DataFrame(brand_data, columns=["Brand", "Users"])
        fig4 = px.pie(brand_df, values="Users", names="Brand", 
                     title="Top 10 Mobile Brands by User Count",
                     hole=0.3)
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.warning("No brand data available for selected filters")
    
    # Top districts with state filter
    st.subheader("Top Districts by Registered Users")
    query = """
        SELECT district, SUM(registered_users) as users
        FROM map_user
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    query += " GROUP BY district ORDER BY users DESC LIMIT 10"
    
    cursor.execute(query, params)
    district_data = cursor.fetchall()
    
    if district_data:
        district_df = pd.DataFrame(district_data, columns=["District", "Users"])
        fig5 = px.bar(district_df, x="District", y="Users",
                     title="Top Districts by Registered Users",
                     color="Users", color_continuous_scale="Blues")
        st.plotly_chart(fig5, use_container_width=True)
    else:
        st.warning("No district data available for selected filters")

with tab3:
    st.header("Insurance Insights")
    
    # Insurance metrics with state filter
    query = """
        SELECT SUM(insurance_count), SUM(insurance_amount)
        FROM aggregated_insurance
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    cursor.execute(query, params)
    insurance_result = cursor.fetchone()
    
    insurance_count = insurance_result[0] if insurance_result and insurance_result[0] is not None else 0
    insurance_amount = insurance_result[1] if insurance_result and insurance_result[1] is not None else 0.0
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Insurance Policies", f"{insurance_count:,}")
    with col2:
        st.metric("Total Insurance Value", f"₹{insurance_amount:,.2f}")
    
    # Insurance type breakdown with state filter
    st.subheader("Insurance Type Breakdown")
    query = """
        SELECT insurance_type, SUM(insurance_count) as count, SUM(insurance_amount) as amount
        FROM aggregated_insurance
        WHERE year = %s AND quarter = %s
    """
    params = (selected_year, selected_quarter)
    
    if selected_state != "All States":
        query += " AND state = %s"
        params += (selected_state,)
    
    query += " GROUP BY insurance_type"
    
    cursor.execute(query, params)
    insurance_data = cursor.fetchall()
    
    if insurance_data:
        insurance_df = pd.DataFrame(insurance_data, columns=["Type", "Count", "Amount"])
        fig6 = px.pie(insurance_df, values="Count", names="Type",
                     title="Insurance Count by Type",
                     hole=0.4)
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.warning("No insurance data available for selected filters")

# with tab4:
#     st.header("Geographical Insights")
    
#     # Map visualization with state filter
#     st.subheader("State-wise Transaction Amount")
#     query = """
#         SELECT state, SUM(transaction_amount) as amount
#         FROM aggregated_transaction
#         WHERE year = %s AND quarter = %s
#     """
#     params = (selected_year, selected_quarter)
    
#     if selected_state != "All States":
#         query += " AND state = %s"
#         params += (selected_state,)
    
#     query += " GROUP BY state"
    
#     cursor.execute(query, params)
#     map_data = cursor.fetchall()
    
#     if map_data:
#         map_df = pd.DataFrame(map_data, columns=["State", "Amount"])
        
#         fig7 = px.choropleth(
#             map_df,
#             geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
#             featureidkey='properties.ST_NM',
#             locations='State',
#             color='Amount',
#             color_continuous_scale='Blues',
#             title='Transaction Amount by State'
#         )
#         fig7.update_geos(fitbounds="locations", visible=False)
#         st.plotly_chart(fig7, use_container_width=True)
#     else:
#         st.warning("No geographic data available for selected filters")
    
#     # Top pincodes with state filter
#     st.subheader("Top Locations by Transactions")
#     query = """
#         SELECT location_name, SUM(transaction_amount) as amount
#         FROM top_map
#         WHERE year = %s AND quarter = %s AND location_type = 'pincode'
#     """
#     params = (selected_year, selected_quarter)
    
#     if selected_state != "All States":
#         query += " AND state = %s"
#         params += (selected_state,)
    
#     query += " GROUP BY location_name ORDER BY amount DESC LIMIT 10"
    
#     cursor.execute(query, params)
#     pincode_data = cursor.fetchall()
    
#     if pincode_data:
#         pincode_df = pd.DataFrame(pincode_data, columns=["Pincode", "Amount"])
#         fig8 = px.bar(pincode_df, x="Pincode", y="Amount",
#                      title="Top Pincodes by Transaction Amount",
#                      color="Amount", color_continuous_scale="Greens")
#         st.plotly_chart(fig8, use_container_width=True)
#     else:
#         st.warning("No pincode data available for selected filters")
    
#     # Top districts with state filter
#     st.subheader("Top Districts by Transactions")
#     query = """
#         SELECT location_name, SUM(transaction_amount) as amount
#         FROM top_map
#         WHERE year = %s AND quarter = %s AND location_type = 'district'
#     """
#     params = (selected_year, selected_quarter)
    
#     if selected_state != "All States":
#         query += " AND state = %s"
#         params += (selected_state,)
    
#     query += " GROUP BY location_name ORDER BY amount DESC LIMIT 10"
    
#     cursor.execute(query, params)
#     district_data = cursor.fetchall()
    
#     if district_data:
#         district_df = pd.DataFrame(district_data, columns=["District", "Amount"])
#         fig9 = px.bar(district_df, x="District", y="Amount",
#                      title="Top Districts by Transaction Amount",
#                      color="Amount", color_continuous_scale="Oranges")
#         st.plotly_chart(fig9, use_container_width=True)
#     else:
#         st.warning("No district transaction data available for selected filters")

# Close connection
cursor.close()
conn.close()

# Footer
st.markdown("---")
st.markdown("""
    **Data Source:** PhonePe Pulse | **Dashboard Created with Streamlit**
    """)