import pandas as pd # from plotly import figure_factory as ff
import streamlit as st
import datetime
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from collections import defaultdict

# Global Variables:
can_download = False # Handle Download Button State
csv = None
webmasters_service = None
overall_limit = 25000
SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly'] # Variable parameter that controls the set of resources that the access token permits.
CLIENT_CONFIG = {'web': { # Client configuration for an OAuth 2.0 web server application (cf. https://developers.google.com/identity/protocols/OAuth2WebServer)
    'client_id': st.secrets["google_secrets"]["GOOGLE_CLIENT_ID"],
    'project_id': st.secrets["google_secrets"]["GOOGLE_PROJECT_ID"],
    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
    'token_uri': 'https://www.googleapis.com/oauth2/v3/token',
    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
    'client_secret': st.secrets["google_secrets"]["GOOGLE_CLIENT_SECRET"],
    'redirect_uris': st.secrets["google_secrets"]["GOOGLE_REDIRECT_URIS"],
    'javascript_origins': st.secrets["google_secrets"]["GOOGLE_JAVASCRIPT_ORIGINS"]}}
 
# Converts a datetime object to a string.
def dt_to_str(date, fmt='%Y-%m-%d'):
    return date.strftime(fmt)

# Convert DF's to CSV
@st.cache # IMPORTANT: Cache the conversion to prevent computation on every rerun
def convert_df(df):
    return df.to_csv().encode('utf-8')

# Send a request and parse it's data
def parse_request(start_date, end_date, rowLimit, startRow, webmasters_service, my_property, page_operator, page_expression, query_operator, query_expression):
    # initialize empty Dict to store data
    data = defaultdict(list)
    # Extract data from GSC API
    request = {}
    # Set request parameters
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
                'startRow': startRow                     # Start at row 0
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
                'startRow': startRow                     # Start at row 0
            }
        elif(query_operator != 'None'):
            request = {
                'startDate': dt_to_str(start_date),      # Get today's date (while loop)
                'endDate': dt_to_str(end_date),          # Get today's date (while loop)
                'dimensions': ['date','page','query'],   # Extract This information
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
                'startRow': startRow                     # Start at row 0
            }
    else:
        request = {
            'startDate': dt_to_str(start_date),          # Get today's date (while loop)
            'endDate': dt_to_str(end_date),              # Get today's date (while loop)
            'dimensions': ['date','page','query'],       # Extract This information
            'rowLimit': rowLimit,                        # Set number of rows to extract at once (min 1 , max 25k)
            'startRow': startRow                         # Start at row 0
        }
    response = webmasters_service.searchanalytics().query(siteUrl=my_property, body=request).execute()
    # Check for row limit
    if (len(response['rows']) == 0):
        #st.write("Reached the end, No more data from the api to save..") #DEBUG
        return 0
    #Process the response
    try:
        for row in response['rows']:
            data['date'].append(row['keys'][0] or 0)    
            data['page'].append(row['keys'][1] or 0)
            data['query'].append(row['keys'][2] or 0)
            data['clicks'].append(row['clicks'] or 0)
            data['ctr'].append(row['ctr'] or 0)
            data['impressions'].append(row['impressions'] or 0)
            data['position'].append(row['position'] or 0)
        #st.write('Added %i to the CSV file.' % len(response['rows'])) #DEBUG
    except:
        st.error('error occurred at %i' % rowLimit)
    # Add response to dataframe 
    df = pd.DataFrame(data)
    df['clicks'] = df['clicks'].astype('int')
    df['ctr'] = df['ctr'] * 100
    df['impressions'] = df['impressions'].astype('int')
    df['position'] = df['position'].round(2)
    return len(response['rows']), df

# Apply the function on the streamlit UI
def scan_website(webmasters_service, my_property, max_rows, start_date, end_date, page_operator, page_expression, query_operator, query_expression): # Note: Using different variable names to avoid conflicts with streamlit global variables.
    rowLimit = int(max_rows)
    frames = []
    final_df = pd.DataFrame() # Initialize empty dataframe incase more than 25k rows are requested.
    startRow = 0
    tmp_rowLimit = 0
    if (rowLimit > overall_limit): # Check if the number of rows entered in the streamlit interface is bigger than the overall limit of the google search console api (per page, 25000)
        tmp_rowLimit = overall_limit # st.write("Requesting %i rows above overall limit.." % rowLimit) #DEBUG
        # Loop through multiple requests, 25k each.
        while (True):
            request_count, df = parse_request(start_date, end_date, tmp_rowLimit, startRow, webmasters_service, my_property, page_operator, page_expression, query_operator, query_expression)
            frames.append(df)
            if (request_count == 0): # st.write("Finished writing to CSV file: No more results") #DEBUG
                break
            else:
                if (request_count != overall_limit): # st.write("Finished writing to CSV file: Not enough results (%i != %i)" % (request_count, overall_limit)) #DEBUG
                    break
                else: 
                    startRow += overall_limit
                    rowLimit -= overall_limit # st.write("We got 25k results! Scanning the next page..") #DEBUG
                    if (rowLimit == 0): # st.write("Finished writing to CSV file: Exactly enough results") #DEBUG
                        break
                    if (rowLimit < overall_limit): # st.write("We only want %i more results, Scanning the next page with exactly that amount.." % rowLimit) #DEBUG
                        tmp_rowLimit = rowLimit
        # Combine all data frames into a single dataframe
        final_df = pd.concat(frames)
    else: # st.write("Requesting %i rows.." % rowLimit) #DEBUG
        request_count, final_df = parse_request(start_date, end_date, rowLimit, startRow, webmasters_service, my_property, page_operator, page_expression, query_operator, query_expression)
    return final_df # Either provide a single data frame or provides multiple data frames

# Streamlit interface (Notes: Globally declared variables)
st.title("Google Search Console API Explorer") # Streamlit title
if 'webmasters_service' not in st.session_state: # Login Form
    # Use the information in the client_secret.json to identify the application requesting authorization.
    # flow = client.from_client_config(client_config=CLIENT_CONFIG, scopes=SCOPES)
    flow = google_auth_oauthlib.flow.Flow.from_client_config(client_config=CLIENT_CONFIG, scopes=SCOPES)
    # Indicate where the API server will redirect the user after the user completes
    # the authorization flow. The redirect URI is required.
    flow.redirect_uri = st.secrets["google_secrets"]["GOOGLE_REDIRECT_URIS"]
    # Generate URL for request to Google's OAuth 2.0 server.
    # Use kwargs to set optional request parameters.
    authorization_url, state = flow.authorization_url(
        # Enable offline access so that you can refresh an access token without
        # re-prompting the user for permission. Recommended for web server apps.
        access_type='offline',
        # Enable incremental authorization. Recommended as a best practice.
        include_granted_scopes='true')
    # Handle Code Submit
    google_parms = st.experimental_get_query_params()
    has_code = False
    code = ''
    try:
        code = google_parms['code'][0]
        has_code = True
    except:
        has_code = False
        pass
    if has_code:
        # Send the code to get the credentials
        try:
            #credentials = flow.run_console()
            flow.fetch_token(code=code)
            webmasters_service = build('searchconsole', 'v1', credentials=flow.credentials)
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
        except Exception as e:
            st.error('Invalid Verification Code:\n'+str(e))
    else:
        with st.form("login_form"):
            login_btn = st.form_submit_button("Submit")
            if login_btn:
                html_string = "<img src=\"\" onerror=\"alert('hey')\">"
                st.markdown(html_string, unsafe_allow_html=True)
        #st.markdown('<a style="' + button_style + '" href="' + authorization_url + '" target="_blank">Login via Google</a>', unsafe_allow_html=True)
if 'verified_sites_urls' in st.session_state: # GSC Form
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
                if branded_kw != '': # If branded_kw is empty then drop branded column
                    csv_file_data['Branded'] = csv_file_data['query'].str.contains(branded_kw) # Add Branded Column
                # Preview CSV
                st.write("Preview:")
                st.dataframe(csv_file_data)
                st.success("Successfully found " + str(len(csv_file_data)) + " records.")
                # Convert CSV to DF
                csv = convert_df(csv_file_data)
                can_download = True
if can_download and csv is not None: # Show CSV Download Button
    st.download_button("Download CSV",csv,"file.csv","text/csv",key='download-csv')
