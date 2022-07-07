import json
import pandas as pd
import streamlit as st
import datetime
# from datetime import date, timedelta
import httplib2
from googleapiclient.discovery import build
# from oauth2client.client import OAuth2WebServerFlow
from collections import defaultdict
import argparse
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from urllib.parse import urlparse, parse_qs
import os

# Make sure we have a temp directory to dump cred files for multiple users:
if not os.path.exists("tempDir"):
    os.makedirs("tempDir")

# Global Variables:
can_download = False # Handle Download Button State
csv = None
uploaded_creds = None

# Check if we are logged into Google
def authorize_creds(fullTmpClientSecretPath):
    print("entering authorize_creds...")
    # Variable parameter that controls the set of resources that the access token permits.
    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']
    # Create a parser to be able to open browser for Authorization
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, parents=[tools.argparser])
    flags = parser.parse_args([])
    flow = client.flow_from_clientsecrets(fullTmpClientSecretPath, scope = SCOPES, message = tools.message_if_missing(fullTmpClientSecretPath))
    # Prepare credentials and authorize HTTP
    # If authenticated credentials don't exist, open Browser to authenticate
    print("---test start---")
    print(flow)
    print("---test end---")
    credentials = tools.run_flow(flow, flags)
    http = credentials.authorize(http=httplib2.Http())
    webmasters_service = build('searchconsole', 'v1', http=http)
    return webmasters_service
 
# Convert datetime to string
def dt_to_str(date, fmt='%Y-%m-%d'):
    """
    Converts a datetime object to a string.
    """
    return date.strftime(fmt)

# Parse request data
def parse_request(start_date, end_date, rowLimit, startRow, webmasters_service, my_property, scDict, page_operator, page_expression, query_operator, query_expression, output):
    # Extract this information from GSC
    request = {}
    if (page_operator != 'None' or query_operator != 'None'):
        if (page_operator != 'None' and query_operator != 'None'):
            request = {
                'startDate': dt_to_str(start_date),     # Get today's date (while loop)
                'endDate': dt_to_str(end_date),         # Get today's date (while loop)
                'dimensions': ['date','page','query'],  # Extract This information
                'dimensionFilterGroups': [
                    {
                    "filters": [
                        {
                        "dimension": "QUERY",
                        "operator": query_operator,
                        "expression": query_expression
                        },
                        {
                        "dimension": "PAGE",
                        "operator": page_operator,
                        "expression": page_expression
                        }
                    ]
                    }
                ],
                'rowLimit': rowLimit,                    # Set number of rows to extract at once (min 1 , max 25k)
                'startRow': startRow                           # Start at row 0
            }
        elif(page_operator != 'None'):
            request = {
                'startDate': dt_to_str(start_date),     # Get today's date (while loop)
                'endDate': dt_to_str(end_date),         # Get today's date (while loop)
                'dimensions': ['date','page','query'],  # Extract This information
                'dimensionFilterGroups': [
                    {
                    "filters": [
                        {
                        "dimension": "PAGE",
                        "operator": page_operator,
                        "expression": page_expression
                        }
                    ]
                    }
                ],
                'rowLimit': rowLimit,                    # Set number of rows to extract at once (min 1 , max 25k)
                'startRow': startRow                           # Start at row 0
            }
        elif(query_operator != 'None'):
            request = {
                'startDate': dt_to_str(start_date),     # Get today's date (while loop)
                'endDate': dt_to_str(end_date),         # Get today's date (while loop)
                'dimensions': ['date','page','query'],  # Extract This information
                'dimensionFilterGroups': [
                    {
                    "filters": [
                        {
                        "dimension": "QUERY",
                        "operator": query_operator,
                        "expression": query_expression
                        }
                    ]
                    }
                ],
                'rowLimit': rowLimit,                    # Set number of rows to extract at once (min 1 , max 25k)
                'startRow': startRow                           # Start at row 0
            }
    else:
        request = {
            'startDate': dt_to_str(start_date),     # Get today's date (while loop)
            'endDate': dt_to_str(end_date),         # Get today's date (while loop)
            'dimensions': ['date','page','query'],  # Extract This information
            'rowLimit': rowLimit,                    # Set number of rows to extract at once (min 1 , max 25k)
            'startRow': startRow                           # Start at row 0
        }
    response = webmasters_service.searchanalytics().query(siteUrl=my_property, body=request).execute()
    # Check for row limit
    if (len(response['rows']) == 0):
        #st.write("Reached the end, No more data from the api to save..") #DEBUG
        return 0
    #Process the response
    try:
        for row in response['rows']:
            scDict['date'].append(row['keys'][0] or 0)    
            scDict['page'].append(row['keys'][1] or 0)
            scDict['query'].append(row['keys'][2] or 0)
            scDict['clicks'].append(row['clicks'] or 0)
            scDict['ctr'].append(row['ctr'] or 0)
            scDict['impressions'].append(row['impressions'] or 0)
            scDict['position'].append(row['position'] or 0)
        #st.write('Added %i to the CSV file.' % len(response['rows'])) #DEBUG
    except:
        st.error('error occurred at %i' % rowLimit)
    # Add response to dataframe 
    df = pd.DataFrame(data = scDict)
    df['clicks'] = df['clicks'].astype('int')
    df['ctr'] = df['ctr'] * 100
    df['impressions'] = df['impressions'].astype('int')
    df['position'] = df['position'].round(2)#df.sort_values('clicks',ascending=False)
    # Preview Data (DEBUG)
    # st.dataframe(df)
    # Write this chunk of page to the CSV
    if not os.path.isfile(output):
        df.to_csv(output)
    else:
        df.to_csv(output, mode='a', header=False)
    # st.write('chunk written to CSV', output, 'inserted rows:', len(response['rows'])) #DEBUG
    return len(response['rows'])

# Connect the web interface to Google
def scan_website(fullTmpClientSecretPath, my_property, max_rows, start_date, end_date, page_operator, page_expression, query_operator, query_expression):
    current_time = str(datetime.datetime.now())
    current_time = "_".join(current_time.split()).replace(":","-")
    current_time = current_time[:-7]
    rowLimit = int(max_rows)
    output = os.path.join("tempDir", 'gsc_api_' + current_time + '.csv')
    # Create function to extract all the data
    webmasters_service = authorize_creds(fullTmpClientSecretPath)     # Get credentials to log in the api
    scDict = defaultdict(list)                      # initialize empty Dict to store data
    overall_limit = 25000
    startRow = 0
    tmp_rowLimit = 0
    if (rowLimit > overall_limit): # 52000 # 50000
        # st.write("Requesting %i rows above overall limit.." % rowLimit) #DEBUG
        tmp_rowLimit = overall_limit
        while (True):
            request_count = parse_request(start_date, end_date, tmp_rowLimit, startRow, webmasters_service, my_property, scDict, page_operator, page_expression, query_operator, query_expression, output)
            if (request_count == 0):
                # st.write("Finished writing to CSV file: No more results") #DEBUG
                break
            else:
                if (request_count != overall_limit):
                    # st.write("Finished writing to CSV file: Not enough results (%i != %i)" % (request_count, overall_limit)) #DEBUG
                    break
                else: # We got 25k results, ask for more
                    startRow += overall_limit
                    rowLimit -= overall_limit
                    # st.write("We got 25k results! Scanning the next page..") #DEBUG
                    if (rowLimit == 0):
                        # st.write("Finished writing to CSV file: Exactly enough results") #DEBUG
                        break
                    if (rowLimit < overall_limit):
                        # st.write("We only want %i more results, Scanning the next page with excatly that amount.." % rowLimit) #DEBUG
                        tmp_rowLimit = rowLimit 
    else:
        # st.write("Requesting %i rows.." % rowLimit) #DEBUG
        parse_request(start_date, end_date, rowLimit, startRow, webmasters_service, my_property, scDict, page_operator, page_expression, query_operator, query_expression, output)
    return output

# Convert DF's to CSV's somehow
@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

# Streamlit title
st.title("Google Search Console 🎃")

# Upload Client Json File
uploaded_file = st.file_uploader("📄 Choose an .json file", "json")
st.write("Note: The file is deleted after use.")
st.warning('Personal Use Only!')
have_file = False
if uploaded_file is not None:
    uploaded_creds = []
    for line in uploaded_file:
        uploaded_creds.append(json.loads(line))
    have_file = True
else:
    have_file = False

# Streamlit Form
with st.form("form"):
    # Get Input from Users:
    property = st.text_input("Property Name (required)")
    # Number of Rows
    numberOfRows = st.number_input('Number of Rows:', 1, None, 25000)
    # Date: Start Date + End Date
    st.write('--------------------')
    st.write('__Default:__ `Last 28 days`')
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
        "Start Date:",
        datetime.date.today() - datetime.timedelta(28))
    with col2:
        end_date = st.date_input(
        "End Date:",
        datetime.date.today() - datetime.timedelta(2))
    # Page
    st.write('--------------------')
    col1, col2 = st.columns(2)
    with col1:
        page_expression = st.text_input('Page Expression')
    with col2:
        page_operator = st.selectbox('Page Operator', ('CONTAINS', 'EQUALS', 'NOT_CONTAINS', 'NOT_EQUALS', 'INCLUDING_REGEX', 'EXCLUDING_REGEX'), 0)
    # Query
    col1, col2 = st.columns(2)
    with col1:
        query_expression = st.text_input('Query Expression')
    with col2:
        query_operator = st.selectbox('Query Operator', ('CONTAINS', 'EQUALS', 'NOT_CONTAINS', 'NOT_EQUALS', 'INCLUDING_REGEX', 'EXCLUDING_REGEX'), 0)
    # Submit button
    submitted = st.form_submit_button("Submit")
    if submitted:
        if property == '':
            st.error("Please fill out the property field")
        elif uploaded_file is None:
            st.error('Please select a file first!')
        else:
            # Validate Inputs
            if page_expression == '':
                page_operator = 'None'
            if query_operator == '':
                query_operator = 'None'
            # Debug
            # st.write('Uploaded File: ', uploaded_file.name)
            # st.write('Property: ', property)
            # st.write('Start Date: ', start_date)
            # st.write('End Date: ', end_date)
            # st.write('Page Expression: ', page_expression)
            # st.write('Page Operator: ', page_operator)
            # st.write('Query Expression: ', query_expression)
            # st.write('Query Operator: ', query_operator)
            # Upload the creds file
            current_time = str(datetime.datetime.now())
            current_time = "_".join(current_time.split()).replace(":","-")
            current_time = current_time[:-7]
            tmpClientSecret = 'client_secret_' + current_time + '.json'
            fullTmpClientSecretPath = os.path.join("tempDir", tmpClientSecret)
            with open(fullTmpClientSecretPath,"wb") as f:
                f.write(uploaded_file.getbuffer())
            # Scan website using google:
            csv_file = scan_website(fullTmpClientSecretPath, property, numberOfRows, start_date, end_date, page_operator, page_expression, query_operator, query_expression)
            st.write("CSV:", csv_file)
            # Generate CSV
            csv_file_data = pd.read_csv(csv_file)
            # Preview CSV
            st.dataframe(csv_file_data)
            # Convert CSV to DF
            csv = convert_df(csv_file_data)
            can_download = True
            # Delete the creds file after usage (cleanup)
            os.remove(fullTmpClientSecretPath)

# Show CSV Download Button
if can_download and csv is not None:
    st.download_button("Download CSV",csv,"file.csv","text/csv",key='download-csv')
