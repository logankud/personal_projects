#!/usr/bin/env python
# coding: utf-8


# ------ Streamlit Dashboard 
import streamlit as st

# Use the full page instead of a narrow central column
st.set_page_config(layout="wide")

import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
from itertools import repeat
import re
import numpy as np
import datetime
from datetime import timedelta


# IMPORT DATA (FROM CSV)
# ---------------

# use glob to get all the csv files in the folder
path = str(r'C:\Users\logan\Documents\OneDrive BackUp\My Personal Stuff\Python\shopify_api_data')
csv_files = glob.glob(os.path.join(path, "*.csv"))

# Create blank dfs to store orders and lineitem csv's
shopify_order_df = pd.DataFrame()
shopify_line_item_df = pd.DataFrame()

# loop over the list of csv files
for f in csv_files:
    
    # If the file name contains 'order', store in teh order_df
    if 'order' in str(f):

        # Read csv file as pandas df
        df = pd.read_csv(f)
        # Append to full order df
        shopify_order_df = shopify_order_df.append(df)

    else: # store in the line_item_df
        
        # Read csv file as pandas df
        df = pd.read_csv(f)
        # Append to full order df
        shopify_line_item_df = shopify_line_item_df.append(df)
        
shopify_order_df['order_total'] = (shopify_order_df['shipping_fees'].astype(float) +shopify_order_df['product_revenue'].astype(float)-shopify_order_df['total_discounts'].astype(float))         

order_df = shopify_order_df.copy()
shopify_log_line_item = shopify_line_item_df.copy()



# FORMAT DATA
# ---------------

# Calulate 'order_total' column
order_df['order_total'] = (order_df['shipping_fees'].astype(float) +order_df['product_revenue'].astype(float)-order_df['total_discounts'].astype(float))         


# Calculate current (month of today) & previous month (most recent full month for reporting) 
todays_date = datetime.date.today().strftime('%Y-%m-%d')

if pd.to_datetime(todays_date).strftime('%m') == '1':
    prev_month = str('12-')+str(int(pd.to_datetime(todays_date).strftime('%Y'))-1)
else:
    end_of_prev_month = pd.to_datetime(datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    prev_month = pd.to_datetime(end_of_prev_month).strftime('%Y-%m')



# Calculate first order date
first_order_dt = order_df.groupby('email',as_index=False)['order_date'].min()
first_order_dt.columns = ['email','first_order_dt']
order_log = pd.merge(order_df,first_order_dt,how='left')

# Calculate First ORder Date (Month)
order_log['first_order_month'] = pd.to_datetime(order_log['first_order_dt']).dt.strftime('%Y-%m')

# Calculate age at purchase
order_log['age_at_purchase'] = (pd.to_datetime(order_log['order_date']) - pd.to_datetime(order_log['first_order_dt'])).dt.days

# Create order_log_current_month for the orders for this current month
order_log_current_month = order_log.loc[order_log['month_year'] == prev_month].copy()




# # ALL TIME RFM
# # -----------

# # Calculate recency, frequency and monetary values
# frequency = order_log.groupby('email',as_index=False)['order_id'].nunique()
# frequency.columns = ['email','frequency']

# recency = order_log.groupby('email',as_index=False)['order_date'].max()
# recency.columns = ['email','recency']

# monetary = order_log.groupby('email',as_index=False)['order_total'].sum()
# monetary.columns = ['email','monetary']

# rfm_df = pd.merge(frequency, recency, how='left')
# rfm_df = pd.merge(rfm_df, monetary, how='left')

# rfm_df['recency_days'] =  (pd.to_datetime(rfm_df['recency'].max()) - pd.to_datetime(rfm_df['recency'])).dt.days

# rfm_df = rfm_df.loc[rfm_df['email']!='']


st.title('Prymal KPI Dashboard')
# st.dataframe(rfm_df.sort_values('monetary',ascending=False).reset_index(drop=True))


# -------------
# PLOTTING
# -------------


col1, col2, col3 = st.beta_columns(3)


# !!! ROW 1 !!!!


col1.header('AOV')
col1.subheader('Average (Median) Order Value, calculated using the order total (inclusive of shipping revenue, discounts & tax)')


# KPI CARD - AOV
# --------------------

aov_median = order_log['order_total'].median()

# KPI Card - AOV (median)
fig = go.Figure(go.Indicator(
    mode = "number",
    value = aov_median,
    domain = {'x': [0, 1], 'y': [0, 1]}))

fig.update_layout(paper_bgcolor = "white",title='All-Time AOV (Median)')

# fig.show()
col1.plotly_chart(fig)


# =============================================================================================


col2.header('Customer Acquisition')
col2.subheader('New customers who have made a purchase')

# KPI CARD - TOtal Unique Custs 
# --------------------

unique_cust_cnt = order_log['email'].nunique()

# KPI Card - Total Customer Count
fig = go.Figure(go.Indicator(
    mode = "number",
    value = unique_cust_cnt,
    domain = {'x': [0, 1], 'y': [0, 1]}))

fig.update_layout(paper_bgcolor = "white",title='Cumulative Unique Customers')

# fig.show()
col2.plotly_chart(fig)

# =============================================================================================


col3.header('Repeat Customer Rate')
col3.subheader('Percentage of customers who repurchase within 60 days of first purchase')


# =============================
# KPI CARD: REPEAT CUST RATE

subset_df = order_log.loc[pd.to_datetime(order_log['first_order_dt'])<=pd.to_datetime(pd.to_datetime(order_log['order_date'].max()) - timedelta(60))]

repeat_cust_list = subset_df.loc[pd.to_datetime(subset_df['order_date'])>=pd.to_datetime(pd.to_datetime(subset_df['first_order_dt'])+ timedelta(60)),'email'].unique()
one_time_cust_list = subset_df.loc[~subset_df['email'].isin(repeat_cust_list),'email'].unique()
total_cust_list = subset_df['email'].unique()

repeat_customer_rate = (len(repeat_cust_list) / len(total_cust_list)) * 100 


# KPI Card - Repeat Customer Rate
fig = go.Figure(go.Indicator(
    mode = "number",
    value = repeat_customer_rate,
    domain = {'x': [0, 1], 'y': [0, 1]}),
#     valueformat='.2%'
               )

fig.update_layout(paper_bgcolor = "white",title='Repeat Customer Rate')

# fig.show()
col3.plotly_chart(fig)


#   ----------------- !!!! ROW 2 !!!! --------------------------------------

# Monthly Trend in AOV

monthly_aov_median = order_log.groupby('month_year',as_index=False)['order_total'].median()


fig = px.line(monthly_aov_median,
             x='month_year',
             y='order_total',
             title='Monthly AOV (Median)')

fig.update_xaxes(title_text='Order Month', type='category')
fig.update_yaxes(title_text='AOV (Median)')
fig.update_layout(yaxis_range=[0,monthly_aov_median['order_total'].max()*1.1])
col1.plotly_chart(fig)
    
# ===================================
# MONTHLY TREND: NEW CUSTOMERS ACQUIRED

monthly_new_customers = order_log.groupby('first_order_month',as_index=False)['email'].nunique()
monthly_new_customers.columns = ['month', 'n_new_customers']

fig = px.bar(monthly_new_customers,
             x='month',
             y='n_new_customers',
             title='Monthly Trend - New Customers Acquired (Shopify)')

fig.update_xaxes(title_text='Month', type='category')
fig.update_yaxes(title_text='New Customers Acquired')

fig.update_layout(yaxis_range=[0,monthly_new_customers['n_new_customers'].max()*1.1])

# fig.show()
col2.plotly_chart(fig)

# ===================================
# MONTHLY TREND: REPEAT PURCHASE RATE


monthly_repeat_df = pd.DataFrame(columns=['month','n_one_time', 'n_repeat', 'n_total', 'repeat_cust_rate'])

for month in subset_df['first_order_month'].unique():
    
    n_repeat = len(subset_df.loc[(subset_df['first_order_month']==month)&(subset_df['email'].isin(repeat_cust_list)),'email'].unique())  
    n_one_time = len(subset_df.loc[(subset_df['first_order_month']==month)&~(subset_df['email'].isin(repeat_cust_list)),'email'].unique()) 
    n_total = subset_df.loc[(subset_df['first_order_month']==month),'email'].nunique()
    
    repeat_cust_rate = (n_repeat / n_total ) * 100
    
    monthly_repeat_df.loc[len(monthly_repeat_df)] = [month,n_one_time, n_repeat, n_total, repeat_cust_rate]


fig = px.line(monthly_repeat_df,
             x='month',
             y='repeat_cust_rate',
             title='Repeat Customer Rate')

fig.update_xaxes(title_text='(First Order Date) Order Month', type='category')
fig.update_yaxes(title_text='% of Repeat Customers')
fig.update_layout(yaxis_range=[0,monthly_repeat_df['repeat_cust_rate'].max()*1.1])

# fig.show()
col3.plotly_chart(fig)

#   ----------------- !!!! ROW 3 !!!! --------------------------------------



col1.header('Repeat Purchase Cycle Time')
col1.subheader('Average (median) number of days between purchases for repeat customers')


#  ///////////////////////
# DATA AGGREGATION

subset_df['repeat_purchase_cycle_time'] = 0


subset_df_sorted = subset_df.sort_values(['email','order_date']).reset_index(drop=True)

previous_email = ''

for index, rows in subset_df_sorted.iterrows():
    
    
    if rows['email'] == previous_email:
                
        cycle_time = (pd.to_datetime(rows['order_date']) - pd.to_datetime(subset_df_sorted.loc[index-1,'order_date'])).days
        
        subset_df_sorted.loc[index,'repeat_purchase_cycle_time'] = cycle_time
        
        
    # Reset previous_email with current row    
    previous_email = rows['email']
    

#  ///////////////////////



# ===================================
# KPI CARD: REPEAT PURCHASE CYCLE TIME

median_cycle_time = subset_df_sorted.loc[subset_df_sorted['repeat_purchase_cycle_time']>0,'repeat_purchase_cycle_time'].median()

# KPI Card - Repeat Customer Rate
fig = go.Figure(go.Indicator(
    mode = "number",
    value = median_cycle_time,
    domain = {'x': [0, 1], 'y': [0, 1]})
               )

fig.update_layout(paper_bgcolor = "white",title='Average (Median) Repeat Purchase Cycle Time in Days')


# fig.show()
col1.plotly_chart(fig)

col2.header('Repeat Customer Lifetime Spend')
col2.subheader('Average (median) lifetime spend for repeat customers, calculated using the order total (inclusive of shipping revenue, discounts & tax)')


# ===================================
# KPI CARD: AVERAGE REPEAT CUSTOMER LIFETIME SPEND


customer_lifetime_spend = subset_df.loc[subset_df['email'].isin(repeat_cust_list)].groupby('email',as_index=False)['order_total'].sum()
customer_lifetime_spend.columns = ['email','lifetime_spend']

median_customer_value = customer_lifetime_spend['lifetime_spend'].median()

# KPI Card - Median Lifetime Spend
fig = go.Figure(go.Indicator(
    mode = "number",
    value = median_customer_value,
    domain = {'x': [0, 1], 'y': [0, 1]},
    number = {'prefix': '$'}
)
               )

fig.update_layout(paper_bgcolor = "white",title='Average (Median) Repeat Customer Lifetime Spend')


# fig.show()
col2.plotly_chart(fig)






#   ----------------- !!!! ROW 4 !!!! --------------------------------------

# ===================================
# MONTHLY TREND: REPEAT PURCHASE CYCLE TIME


cycle_times_df = subset_df_sorted.loc[subset_df_sorted['repeat_purchase_cycle_time']>0]


monthly_cycle_times = cycle_times_df.groupby('month_year',as_index=False)['repeat_purchase_cycle_time'].median()

fig = px.line(monthly_cycle_times,
             x='month_year',
             y='repeat_purchase_cycle_time',
             title='Monthly Trend - Repeat Purchase Cycle Time (Days)')

fig.update_xaxes(title_text='Order Month', type='category')
fig.update_yaxes(title_text='Avg. (Median) Cycle Time in Days')

# fig.show()
col1.plotly_chart(fig)

# ===================================
# MONTHLY TREND: REPEAT CUSTOMER LIFETIME SPEND BY COHORT

cust_cohorts = subset_df.groupby('email',as_index=False)['first_order_month'].min()

customer_lifetime_spend_with_cohort = customer_lifetime_spend.merge(cust_cohorts,how='left',on='email')
customer_lifetime_spend_with_cohort

cohort_median_customer_value = customer_lifetime_spend_with_cohort.groupby('first_order_month',as_index=False)['lifetime_spend'].median()

fig = px.line(cohort_median_customer_value,
             x='first_order_month',
             y='lifetime_spend',
             title='Average (Median) Lifetime Spend by Cohort')

fig.update_xaxes(title_text='Cohort Month', type='category')
fig.update_yaxes(title_text='Avg. (Median) Lifetime Spend')

# fig.show()
col2.plotly_chart(fig)

#   ----------------- !!!! ROW 3 !!!! --------------------------------------
#   ----------------- !!!! ROW 3 !!!! --------------------------------------
#   ----------------- !!!! ROW 3 !!!! --------------------------------------
#   ----------------- !!!! ROW 3 !!!! --------------------------------------




