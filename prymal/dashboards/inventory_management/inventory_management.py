import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

import boto3
import pandas as pd
import plotly.graph_objects as go
import os

# Transformation SQL Query as code (path)
QUERY_PATH = 'prymal/transformations/shopify_qty_sold_by_sku_daily/shopify_qty_sold_by_sku_daily.sql'

# AWS Credentials
AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY=os.environ['AWS_ACCESS_SECRET']

# Create a boto3 client for the Athena service.
athena_client = boto3.client('athena', 
                                 region_name='us-east-1',
                                 aws_access_key_id=AWS_ACCESS_KEY_ID,
                                 aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# Create a dropdown menu with the available values for the WHERE clause.
dropdown_options = [
    {'label': 'All', 'value': None},
    {'label': 'Value 1', 'value': 'value1'},
    {'label': 'Value 2', 'value': 'value2'},
]

# Create a Dash app.
app = dash.Dash()

# Create a layout for the app.
app.layout = html.Div([

    # Dropdown menu.
    dcc.Dropdown(
        id='dropdown',
        options=dropdown_options,
        value=None,
    ),

    # Plotly line chart.
    dcc.Graph(
        id='linechart',
    ),

])

# Callback function to update the line chart when the selected value in the dropdown menu changes.
@app.callback(
    Output('linechart', 'figure'),
    [Input('dropdown', 'value')],
)
def update_linechart(where_clause):

    # Generate the Athena query.
    query = f"""
    SELECT
        order_date
        , SUM(qty_sold) as qty_sold
    FROM
        "prymal-analytics"."shopify_qty_sold_by_sku_daily"
    WHERE
        {where_clause}
    GROUP BY 
        order_date  
    """ 

    response = athena_client.start_query_execution(  
            QueryString=query,
            QueryExecutionContext={
                'Database': 'prymal-analytics'
            },
            ResultConfiguration={
                'OutputLocation': 's3://prymal-ops/athena_query_results/'  # Specify your S3 bucket for query results
            }
        )
    
    # Wait for the query to complete.
    waiter = athena_client.get_waiter('query-execution-succeeded')
    waiter.wait(QueryExecutionId=response['QueryExecutionId'])

    # Get the query results.
    results = athena_client.get_query_results(QueryExecutionId=response['QueryExecutionId'])

    # Convert the query results to a Pandas DataFrame.
    df = pd.DataFrame(results['ResultSet']['Rows'][1:], columns=results['ResultSet']['ResultSetMetadata']['ColumnInfo'])

    # Create a Plotly line chart.
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=df.iloc[:,0],
            y=df.iloc[:,1],
            mode='lines',
        )
    )

    # Return the Plotly line chart.
    return figure

# Start the Dash app.
if __name__ == '__main__':
    app.run_server(debug=True)
