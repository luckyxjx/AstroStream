# dashboard/app.py
import os
import dash
# Optional: Use Bootstrap components for better styling
# import dash_bootstrap_components as dbc
# from dash_bootstrap_templates import load_figure_template
from dash import dcc, html, Input, Output, dash_table, State
import plotly.express as px
import pandas as pd
from pymongo import MongoClient, DESCENDING
from datetime import datetime, timedelta
import logging
import time # Import time for sleep

# --- Configuration ---
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/') # Use 'localhost' if running outside Docker for testing
MONGO_DB = 'astrostream'
MONGO_COLLECTION = 'solar_flares'
REFRESH_INTERVAL_MS = 30000 # Refresh data every 30 seconds (30000 ms)
PAGE_SIZE = 15 # Number of items per table page

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Connect to MongoDB ---
# Add retry logic for connection robustness
mongo_client = None
retry_delay = 5
max_retries = 5
retries = 0
while mongo_client is None and retries < max_retries:
    try:
        mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # Timeout connection attempt
        # The ismaster command is cheap and does not require auth.
        mongo_client.admin.command('ismaster')
        db = mongo_client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        logging.info(f"Successfully connected to MongoDB at {MONGO_URI}")
    except Exception as e:
        retries += 1
        logging.warning(f"Error connecting to MongoDB (attempt {retries}/{max_retries}): {e}. Retrying in {retry_delay}s...")
        mongo_client = None # Ensure client is None if connection failed
        collection = None
        db = None
        if retries < max_retries:
            time.sleep(retry_delay)
        else:
            logging.error(f"Failed to connect to MongoDB after {max_retries} attempts.")


# --- Initialize Dash App ---
# Optional: Load a Bootstrap theme for better visuals
# load_figure_template("cerulean")
# app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CERULEAN])

app = dash.Dash(__name__)
app.title = "AstroStream Dashboard"

# --- App Layout ---
app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif'}, children=[
    html.H1(children='AstroStream - Solar Flare Dashboard', style={'textAlign': 'center', 'color': '#2c3e50'}),

    html.Div(children='''
        Displaying near real-time solar flare data fetched from NASA DONKI, processed via Kafka & Spark, stored in MongoDB.
    ''', style={'textAlign': 'center', 'marginBottom': '20px'}),

    # Interval component for periodic updates
    dcc.Interval(
        id='interval-component',
        interval=REFRESH_INTERVAL_MS,
        n_intervals=0
    ),

    # Store component to hold data for sharing between callbacks if needed
    dcc.Store(id='flare-data-store'),

    # Placeholder for status messages (e.g., connection errors)
    html.Div(id='status-message', style={'color': 'red', 'textAlign': 'center', 'marginBottom': '10px'}),

    # Data Table Section
    html.H3("Recent Solar Flares", style={'marginTop': '30px', 'marginBottom': '10px'}),
    html.Div(id='live-update-table-container', children=[
        # Loading component wrapper for better UX
        dcc.Loading(
            id="loading-table",
            type="circle", # or "default", "cube", "dot"
            children=html.Div(id='live-update-table')
        )
    ]),

    # Chart Section (Optional)
    # html.H3("Flare Class Distribution", style={'marginTop': '40px', 'marginBottom': '10px'}),
    # dcc.Graph(id='flare-class-chart')

])

# --- Callback to Fetch Data Periodically ---
@app.callback(
    [Output('flare-data-store', 'data'),
     Output('status-message', 'children')],
    Input('interval-component', 'n_intervals')
)
def fetch_data(n):
    if collection is None:
        error_msg = f"Error: Could not connect to MongoDB at {MONGO_URI}. Please check connection."
        logging.error(error_msg)
        return [], error_msg # Return empty data and error message

    try:
        # Fetch recent data, sort by peak time descending
        # Limit results for performance, handle potential large datasets
        flare_data = list(collection.find(
            {}, # No filter, get all recent data
            {'_id': 0} # Exclude the MongoDB ObjectId from results
        ).sort('peakTime', DESCENDING).limit(200)) # Limit to latest 200 entries for performance

        if not flare_data:
            logging.info("No flare data found in MongoDB collection.")
            return [], "No recent flare data found." # Return empty data and status message

        # Convert datetime objects to ISO format strings for JSON serialization
        for item in flare_data:
            for key, value in item.items():
                if isinstance(value, datetime):
                    item[key] = value.isoformat()

        logging.info(f"Fetched {len(flare_data)} records from MongoDB.")
        return flare_data, "" # Return fetched data and clear status message

    except Exception as e:
        error_msg = f"An error occurred during data fetch from MongoDB: {e}"
        logging.exception(error_msg) # Log the full traceback
        return [], error_msg # Return empty data and error message

# --- Callback to Update Data Table ---
@app.callback(
    Output('live-update-table', 'children'),
    Input('flare-data-store', 'data') # Triggered when data store updates
)
def update_table(flare_data_json):
    if not flare_data_json:
        # If store is empty (due to fetch error or no data), display nothing or a message
        return html.Div("Waiting for data or no data available...")

    try:
        df = pd.DataFrame(flare_data_json)

        # --- Data Cleaning / Selection for Display ---
        # Select and rename columns for better readability
        display_columns_map = {
            "flrID": "Flare ID",
            "beginTime": "Begin Time (UTC)",
            "peakTime": "Peak Time (UTC)",
            "endTime": "End Time (UTC)",
            "classType": "Class",
            "sourceLocation": "Location",
            "activeRegionNum": "Active Region",
            "processingTimestamp": "Processed At (UTC)"
            # Add/remove columns as desired
        }

        # Filter DataFrame to only include desired columns, handling missing ones
        cols_to_display = [col for col in display_columns_map.keys() if col in df.columns]
        if not cols_to_display:
             return html.Div("No displayable columns found in the fetched data.")

        df_display = df[cols_to_display].copy() # Create a copy to avoid SettingWithCopyWarning
        df_display.rename(columns=display_columns_map, inplace=True)

        # Attempt to format timestamp columns nicely, handling potential errors
        for col_original, col_display in display_columns_map.items():
            if col_display in df_display.columns and ('Time' in col_display or 'Processed' in col_display):
                 try:
                     # Convert to datetime, coerce errors to NaT (Not a Time)
                     df_display[col_display] = pd.to_datetime(df_display[col_display], errors='coerce')
                     # Format valid datetimes, leave NaT as is (will appear blank or as 'NaT')
                     # Check if column is not all NaT before formatting
                     if not df_display[col_display].isnull().all():
                         df_display[col_display] = df_display[col_display].dt.strftime('%Y-%m-%d %H:%M:%S')
                     else:
                         df_display[col_display] = '' # Or some placeholder for completely invalid columns
                 except Exception as e:
                     logging.warning(f"Could not format column '{col_display}': {e}")
                     # Keep original string representation if formatting fails


        # Create Dash DataTable
        return dash_table.DataTable(
            id='datatable-flares',
            columns=[{"name": i, "id": i} for i in df_display.columns],
            data=df_display.to_dict('records'),
            page_size=PAGE_SIZE,
            style_table={'overflowX': 'auto', 'minWidth': '100%'}, # Enable horizontal scroll
            style_cell={
                'height': 'auto',
                'minWidth': '100px', 'width': '150px', 'maxWidth': '250px',
                'whiteSpace': 'normal',
                'textAlign': 'left',
                'padding': '5px'
            },
             style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold',
                'border': '1px solid black'
            },
            style_data={
                'border': '1px solid grey'
            },
            sort_action="native", # Enable frontend sorting
            filter_action="native", # Enable frontend filtering
        )

    except Exception as e:
        logging.exception(f"Error updating table from stored data: {e}")
        return html.Div(f"An error occurred displaying the data: {e}")


# --- Optional: Callback for a Chart ---
# @app.callback(
#     Output('flare-class-chart', 'figure'),
#     Input('flare-data-store', 'data') # Triggered when data store updates
# )
# def update_chart(flare_data_json):
#     if not flare_data_json:
#         return {} # Return empty figure if no data
#
#     try:
#         df = pd.DataFrame(flare_data_json)
#         if 'classType' not in df.columns or df['classType'].isnull().all():
#             return {} # Return empty if classType column is missing or all null
#
#         # Simple bar chart of flare classes (using first letter: A, B, C, M, X)
#         # Handle potential NaN/None values before string operations
#         df_filtered = df.dropna(subset=['classType'])
#         if df_filtered.empty:
#             return {}
#
#         class_counts = df_filtered['classType'].str[0].value_counts().reset_index()
#         class_counts.columns = ['Class', 'Count']
#         # Define order for classes
#         class_order = ['A', 'B', 'C', 'M', 'X']
#         class_counts['Class'] = pd.Categorical(class_counts['Class'], categories=class_order, ordered=True)
#         class_counts = class_counts.sort_values('Class')
#
#         fig = px.bar(class_counts, x='Class', y='Count', title="Flare Counts by Class (Recent Data)")
#         fig.update_layout(xaxis_title="Flare Class", yaxis_title="Count")
#         return fig
#
#     except Exception as e:
#         logging.exception(f"Error updating chart: {e}")
#         return {}


# --- Run the App ---
if __name__ == '__main__':
    # Use app.run() instead of app.run_server() for newer Dash versions
    app.run(debug=False, host='0.0.0.0', port=8050) # <-- CORRECTED LINE

