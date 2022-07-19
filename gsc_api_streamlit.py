import json
import pandas as pd
import streamlit as st
from streamlit.components.v1 import html
import datetime
import httplib2
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from collections import defaultdict
from oauth2client import client
from oauth2client import tools
import os
from plotly import figure_factory as ff

# Client configuration for an OAuth 2.0 web server application
# (cf. https://developers.google.com/identity/protocols/OAuth2WebServer)
CLIENT_CONFIG = {'web': {
    'client_id': st.secrets["google_secrets"]["GOOGLE_CLIENT_ID"],
    'project_id': st.secrets["google_secrets"]["GOOGLE_PROJECT_ID"],
    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
    'token_uri': 'https://www.googleapis.com/oauth2/v3/token',
    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
    'client_secret': st.secrets["google_secrets"]["GOOGLE_CLIENT_SECRET"],
    'redirect_uris': st.secrets["google_secrets"]["GOOGLE_REDIRECT_URIS"],
    'javascript_origins': st.secrets["google_secrets"]["GOOGLE_JAVASCRIPT_ORIGINS"]}}

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly'] # Variable parameter that controls the set of resources that the access token permits.

# Make sure we have a temp directory to dump cred files for multiple users:
if not os.path.exists("tempDir"):
    os.makedirs("tempDir")

# Global Variables:
can_download = False # Handle Download Button State
csv = None
 
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
def scan_website(webmasters_service, my_property, max_rows, start_date, end_date, page_operator, page_expression, query_operator, query_expression):
    current_time = str(datetime.datetime.now())
    current_time = "_".join(current_time.split()).replace(":","-")
    current_time = current_time[:-7]
    rowLimit = int(max_rows)
    output = os.path.join("tempDir", 'gsc_api_' + current_time + '.csv')
    # Create function to extract all the data
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
st.warning('Personal Use Only!!')

# Show streamlit forms
webmasters_service = None
placeholder = st.empty()
if 'webmasters_service' not in st.session_state:
    with st.form("login"):
        # Use the information in the client_secret.json to identify the application requesting authorization.
        # flow = client.from_client_config(client_config=CLIENT_CONFIG, scopes=SCOPES)
        flow = google_auth_oauthlib.flow.Flow.from_client_config(client_config=CLIENT_CONFIG, scopes=SCOPES)
        # Indicate where the API server will redirect the user after the user completes
        # the authorization flow. The redirect URI is required.
        flow.redirect_uri = 'https://ilaygranot-gsc-api-gsc-api-streamlit-9r965y.streamlitapp.com'
        # Generate URL for request to Google's OAuth 2.0 server.
        # Use kwargs to set optional request parameters.
        authorization_url, state = flow.authorization_url(
            # Enable offline access so that you can refresh an access token without
            # re-prompting the user for permission. Recommended for web server apps.
            access_type='offline',
            # Enable incremental authorization. Recommended as a best practice.
            include_granted_scopes='true')
        # Handle Code Submit
        code_submitted = st.form_submit_button("Login via Google")
        if code_submitted:
            my_js = """
            window.open('{authorization_url}');
            window.close()
            """.format(authorization_url=authorization_url)
            my_html = f"<script>{my_js}</script>"
            html(my_html)
        #st.markdown('<a onclick="window.close();" href="' + authorization_url + '" target="_blank">Login via Google</a>', unsafe_allow_html=True)
    if 1==0:
        # Send the code to get the credentials
        try:
            credentials = flow.step2_exchange(code)
            http = credentials.authorize(http=httplib2.Http())
            webmasters_service = build('searchconsole', 'v1', http=http)
            if 'webmasters_service' not in st.session_state:
                st.session_state.webmasters_service = webmasters_service
            # Get Properties
            site_list = webmasters_service.sites().list().execute()
            # Filter for verified websites
            verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                                if s['permissionLevel'] != 'siteUnverifiedUser'
                                    and s['siteUrl'][:4] == 'http']
            if 'verified_sites_urls' not in st.session_state:
                st.session_state.verified_sites_urls = verified_sites_urls
                placeholder.empty()
        except:
            st.error('Invalid Verification Code')
            st.success('Go to the following link in your browser and try again:\n'+str(authorization_url))
if 'verified_sites_urls' in st.session_state:
    # Streamlit Form
    with st.form("form"):
        properties = None
        # Show Properties
        property = st.selectbox("Select a property (required)", st.session_state.verified_sites_urls)
        # Number of Rows
        numberOfRows = st.number_input('Number of Rows:', 1, None, 25000)
        # branded kw 
        branded_kw = st.text_input('Branded Keyword')
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
            if 'webmasters_service' not in st.session_state:
                st.error('Please validate your credentials first!')
            else:
                # Validate Inputs
                if page_expression == '':
                    page_operator = 'None'
                if query_operator == '':
                    query_operator = 'None'
                # Debug
                # st.write('Property: ', property)
                # st.write('Start Date: ', start_date)
                # st.write('End Date: ', end_date)
                # st.write('Page Expression: ', page_expression)
                # st.write('Page Operator: ', page_operator)
                # st.write('Query Expression: ', query_expression)
                # st.write('Query Operator: ', query_operator)
                # st.write("CSV:", csv_file)
                # Scan website using google:
                csv_file = scan_website(st.session_state.webmasters_service, property, numberOfRows, start_date, end_date, page_operator, page_expression, query_operator, query_expression)
                # Generate CSV
                csv_file_data = pd.read_csv(csv_file)
                # Add Branded Column
                csv_file_data['Branded'] = csv_file_data['query'].str.contains(branded_kw)
                # If branded_kw is empty then drop branded column
                if branded_kw == '':
                    csv_file_data = csv_file_data.drop(columns=['Branded'])
                # Preview CSV
                st.write("Preview:")
                st.dataframe(csv_file_data)
                st.success("Successfully found " + str(len(csv_file_data)) + " records.")
                # Convert CSV to DF
                csv = convert_df(csv_file_data)
                can_download = True

# Show CSV Download Button
if can_download and csv is not None:
    st.download_button("Download CSV",csv,"file.csv","text/csv",key='download-csv')
