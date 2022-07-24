## 1. Imports: -----------------------------------------------------

import pandas as pd # from plotly import figure_factory as ff
import streamlit as st
import datetime
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from collections import defaultdict

## 2. Global Variables: --------------------------------------------

CSV = None
CSV_DOWNLOADABLE = False
BUTTON_STYLE = """
background-color:#4CAF50;
border:none;
color:white;
padding:15px 32px;
text-align:center;
text-decoration:none;
display:inline-block;
font-size:16px;
"""
API_LIMIT = 25000
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
 
## 3. Function Declarations: ----------------------------------------

# Converts a datetime object to a string:
def dt_to_str(date, fmt='%Y-%m-%d'):
    return date.strftime(fmt)

# Send a request and parse it's data:
def parse_request(type_selectbox, selected_country, country_operator, selected_device, device_operator, start_date, end_date, rowLimit, startRow, my_property, page_operator, page_expression, query_operator, query_expression):
    # Initialize empty dictionary to store data
    data = defaultdict(list)
    # Set request parameters
    request = {}
    request['startDate'] =  dt_to_str(start_date) # Get today's date (while loop)
    request['endDate'] =  dt_to_str(end_date) # Get today's date (while loop)
    request['dimensions'] = ['DATE','PAGE','QUERY'] # Extract This information
    request['rowLimit'] = rowLimit # Set number of rows to extract at once (min 1 , max 25k)
    request['startRow'] = startRow # Start at row 0
    request['type'] = type_selectbox # Filter results to the following type
    # Optionally add page/query/countries/devices operators to the request, depending what the user has selected in the interface
    country_id = 3
    device_id = 3
    if page_operator != 'None' or query_operator != 'None' or country_operator != 'None' or device_operator != 'None':
        request['dimensionFilterGroups'] = [{ 'filters': [] }]
    if page_operator != 'None':
        request['dimensionFilterGroups'][0]['filters'].append({
                    "dimension": "PAGE",
                    "operator": page_operator,
                    "expression": page_expression
                    })
    if query_operator != 'None':
        request['dimensionFilterGroups'][0]['filters'].append({
                    "dimension": "QUERY",
                    "operator": query_operator,
                    "expression": query_expression
                    })
    if country_operator != 'None':
        device_id = 4
        request['dimensions'].append('COUNTRY')
        request['dimensionFilterGroups'][0]['filters'].append({
                    "dimension": "COUNTRY",
                    "operator": country_operator,
                    "expression": selected_country
                    })
    if device_operator != 'None':
        request['dimensions'].append('DEVICE')
        request['dimensionFilterGroups'][0]['filters'].append({
                    "dimension": "DEVICE",
                    "operator": device_operator,
                    "expression": selected_device
                    })
    # Send the request to GSC API
    response = st.session_state.webmasters_service.searchanalytics().query(siteUrl=my_property, body=request).execute()
    st.write('Request:')#debug
    st.write(request)#debug
    st.write('Response:')#debug
    st.write(response)#debug
    # Check for row limit
    try:
        if (len(response['rows']) == 0): # st.write("Reached the end, No more data from the api to save..") #DEBUG
            return 0, pd.DataFrame()
    except KeyError: # 0 Results found
        return 0, pd.DataFrame()
    # Process the response
    try:
        for row in response['rows']:
            data['date'].append(row['keys'][0] or 0)
            data['page'].append(row['keys'][1] or 0)
            data['query'].append(row['keys'][2] or 0)
            if country_operator != 'None':
                data['country'].append(row['keys'][country_id] or 0)
            if device_operator != 'None':
                data['device'].append(row['keys'][device_id] or 0)
            data['clicks'].append(row['clicks'] or 0)
            data['ctr'].append(row['ctr'] or 0)
            data['impressions'].append(row['impressions'] or 0)
            data['position'].append(row['position'] or 0)
    except:
        st.error('Invalid response from GSC API at row %i' % rowLimit) # Handle errors
    # Add response to dataframe
    df = pd.DataFrame(data)
    df['clicks'] = df['clicks'].astype('int')
    df['ctr'] = df['ctr'] * 100
    df['impressions'] = df['impressions'].astype('int')
    df['position'] = df['position'].round(2)
    return len(response['rows']), df

# Apply the function on the streamlit UI:
def scan_website(my_property, max_rows, type_selectbox, selected_country, country_operator, selected_device, device_operator, start_date, end_date, page_operator, page_expression, query_operator, query_expression): # Note: Using different variable names to avoid conflicts with streamlit global variables.
    rowLimit = int(max_rows)
    frames = []
    final_df = pd.DataFrame() # Initialize empty dataframe incase more than 25k rows are requested.
    startRow = 0
    tmp_rowLimit = 0
    if (rowLimit > API_LIMIT): # Check if the number of rows entered in the streamlit interface is bigger than the overall limit of the google search console api (per page, 25000)
        tmp_rowLimit = API_LIMIT # st.write("Requesting %i rows above overall limit.." % rowLimit) #DEBUG
        # Loop through multiple requests, 25k each.
        while (True):
            request_count, df = parse_request(type_selectbox, selected_country, country_operator, selected_device, device_operator, start_date, end_date, tmp_rowLimit, startRow, my_property, page_operator, page_expression, query_operator, query_expression)
            frames.append(df)
            if (request_count == 0): # st.write("Finished writing to CSV file: No more results") #DEBUG
                break
            else:
                if (request_count != API_LIMIT): # st.write("Finished writing to CSV file: Not enough results (%i != %i)" % (request_count, API_LIMIT)) #DEBUG
                    break
                else: 
                    startRow += API_LIMIT
                    rowLimit -= API_LIMIT # st.write("We got 25k results! Scanning the next page..") #DEBUG
                    if (rowLimit == 0): # st.write("Finished writing to CSV file: Exactly enough results") #DEBUG
                        break
                    if (rowLimit < API_LIMIT): # st.write("We only want %i more results, Scanning the next page with exactly that amount.." % rowLimit) #DEBUG
                        tmp_rowLimit = rowLimit
        # Combine all data frames into a single dataframe
        final_df = pd.concat(frames)
    else: # st.write("Requesting %i rows.." % rowLimit) #DEBUG
        request_count, final_df = parse_request(type_selectbox, selected_country, country_operator, selected_device, device_operator, start_date, end_date, rowLimit, startRow, my_property, page_operator, page_expression, query_operator, query_expression)
    return final_df # Either provide a single data frame or provides multiple data frames

## 4. Streamlit Interface: -------------------------------------------

# A. Set Streamlit Page Title:
st.title("Google Search Console API Explorer")

# B. Show Login Form:
if 'webmasters_service' not in st.session_state:
    # Handle Google Login Flow [GET request]:
    google_parms = st.experimental_get_query_params() # Parse the GET Request Parameters provided by Google via the return_uri
    token_exists = False
    code = ''
    try:
        code = google_parms['code'][0]
        token_exists = True
    except: # Error handling
        token_exists = False
        pass
    flow = google_auth_oauthlib.flow.Flow.from_client_config(client_config=CLIENT_CONFIG, scopes=SCOPES) # Use the information in the client_secret.json to identify the application requesting authorization.
    flow.redirect_uri = st.secrets["google_secrets"]["GOOGLE_REDIRECT_URIS"] # Indicate where the API server will redirect the user after the user completes the authorization flow. The redirect URI is required.
    # Either show a login button or try to connect into the API and load the user's GSC properties, depending if a 'code' GET parameter was provided or not:
    if token_exists:
        try: # try to connect into the API
            # Validate the provided Google Token Code:
            flow.fetch_token(code=code)
            # Connect to the API:
            if 'webmasters_service' not in st.session_state: # store the connection in the session
                st.session_state.webmasters_service = build('searchconsole', 'v1', credentials=flow.credentials)
            # Get User Properties Site Addresses:
            site_list = st.session_state.webmasters_service.sites().list().execute()
            verified_sites_urls = [s['siteUrl'] for s in site_list['siteEntry']
                                if s['permissionLevel'] != 'siteUnverifiedUser'
                                    and s['siteUrl'][:4] == 'http'] # Filter for verified websites
            if 'verified_sites_urls' not in st.session_state:
                st.session_state.verified_sites_urls = verified_sites_urls
        except Exception as e: # Invalidate the session (TODO: Redirect to the home page / remove GET parameters from the URL)
            st.error('Invalid Verification Code:\n'+str(e))
    else: # show a login button
        # Generate Google Authentication URL:
        authorization_url, state = flow.authorization_url( # Generate URL for request to Google's OAuth 2.0 server. Use kwargs to set optional request parameters.
        access_type='offline', # Enable offline access so that you can refresh an access token without re-prompting the user for permission. Recommended for web server apps.
        include_granted_scopes='true') # Enable incremental authorization. Recommended as a best practice.
        # Show Google Login Button:
        st.markdown('<a style="' + BUTTON_STYLE + '" href="' + authorization_url + '" target="_blank">Login via Google</a>', unsafe_allow_html=True)

# C. Show GSC Streamlit Form when the user is logged in:
if 'verified_sites_urls' in st.session_state: # Check if we have the user's verified properties list, meaning they are logged in
    # Streamlit Form:
    with st.form("form"):
        properties = None
        # Show Properties Dropdown:
        property = st.selectbox("Select a property (required)", st.session_state.verified_sites_urls)
        # Show Number of Rows Field:
        numberOfRows = st.number_input('Number of Rows:', 1, None, 25000)
        # Show Branded Keyword Field:
        branded_kw = st.text_input('Branded Keyword')
        # Show Country Dropdown Field:
        st.write('--------------------')
        col1, col2 = st.columns(2)
        with col1:
            selected_country = st.selectbox(
                'Choose Country',
                ['isr', 'usa', 'gbr', 'qat', 'plw', 'fro', 'twn', 'chn', 'lca', 'mmr', 'uga', 'xkk', 'bhs', 'grl', 'blm', 'zwe', 'msr', 'srb', 'col', 'com', 'mhl', 'bes', 'cmr', 'glp', 'sxm', 'gtm', 'nic', 'cpv', 'bfa', 'kaz', 'tls', 'tza', 'gum', 'dnk', 'ton', 'fsm', 'mli', 'tjk', 'zmb', 'tto', 'sen', 'moz', 'lbr', 'tha', 'can', 'bhr', 'niu', 'ukr', 'blr', 'mlt', 'shn', 'nzl', 'kwt', 'aus', 'gin', 'ken', 'bel', 'stp', 'syr', 'slv', 'tun', 'prt', 'aze', 'reu', 'tkl', 'kor', 'deu', 'svk', 'prk', 'rwa', 'dma', 'wsm', 'yem', 'mne', 'pak', 'ita', 'dji', 'flk', 'cri', 'mrt', 'tca', 'ala', 'tcd', 'est', 'caf', 'jam', 'egy', 'ecu', 'guy', 'pyf', 'ner', 'irl', 'ltu', 'sle', 'gha', 'khm', 'and', 'swz', 'rus', 'mdg', 'grd', 'mac', 'mco', 'iot', 'aut', 'civ', 'gmb', 'sjm', 'cuw', 'tkm', 'asm', 'esp', 'zaf', 'brn', 'cog', 'npl', 'gab', 'bol', 'kgz', 'lie', 'cze', 'bwa', 'som', 'omn', 'lbn', 'uzb', 'mda', 'lao', 'pan', 'gnq', 'vnm', 'lso', 'ssd', 'maf', 'umi', 'atg', 'mus', 'chl', 'fin', 'bra', 'irn', 'guf', 'gnb', 'mkd', 'ncl', 'jey', 'dom', 'btn', 'mnp', 'nfk', 'phl', 'geo', 'hnd', 'eth', 'tgo', 'slb', 'nru', 'vut', 'blz', 'hrv', 'wlf', 'spm', 'tuv', 'ven', 'lka', 'zzz', 'sur', 'mwi', 'gib', 'dza', 'abw', 'myt', 'per', 'lby', 'bdi', 'cod', 'mdv', 'tur', 'nga', 'grc', 'mng', 'pol', 'alb', 'idn', 'ind', 'hkg', 'sgp', 'nld', 'aia', 'irq', 'kir', 'arg', 'bgd', 'nor', 'vir', 'swe', 'ago', 'svn', 'cym', 'arm', 'cyp', 'kna', 'smr', 'pry', 'cub', 'sdn', 'che', 'hti', 'vct', 'mex', 'lva', 'rou', 'isl', 'eri', 'cxr', 'sau', 'ben', 'fra', 'bgr', 'cok', 'pri', 'hun', 'brb', 'are', 'fji', 'jor', 'vgb', 'lux', 'mys', 'afg', 'mar', 'ata', 'bih', 'esh', 'ggy', 'pse', 'imn', 'ury', 'bmu', 'nam', 'syc', 'jpn', 'mtq', 'png'],
                1)
        with col2:
            country_operator = st.selectbox('Page Operator', ('CONTAINS', 'EQUALS', 'NOT_CONTAINS', 'NOT_EQUALS'), 0)
        # Show Device Dropdown Field:
        st.write('--------------------')
        col1, col2 = st.columns(2)
        with col1:
            selected_device = st.selectbox(
                'Choose Device',
                ['DESKTOP','MOBILE','TABLET'],
                0)
        with col2:
            device_operator = st.selectbox('Device Operator', ('CONTAINS', 'EQUALS', 'NOT_CONTAINS', 'NOT_EQUALS'), 0)
        # Filter results to the following type::
        st.write('--------------------')
        st.write('Filter results to the following type:')
        type_selectbox = st.selectbox('Type', ('DISCOVER', 'GOOGLE_NEWS', 'NEWS', 'IMAGE', 'VIDEO', 'WEB'), 5)
        # Show Start Date + End Date Fields:
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
        # Show the Page Expression/Operator Fields:
        st.write('--------------------')
        col1, col2 = st.columns(2)
        with col1:
            page_expression = st.text_input('Page Expression')
        with col2:
            page_operator = st.selectbox('Page Operator', ('CONTAINS', 'EQUALS', 'NOT_CONTAINS', 'NOT_EQUALS', 'INCLUDING_REGEX', 'EXCLUDING_REGEX'), 0)
        # Show the Query Field:
        col1, col2 = st.columns(2)
        with col1:
            query_expression = st.text_input('Query Expression')
        with col2:
            query_operator = st.selectbox('Query Operator', ('CONTAINS', 'EQUALS', 'NOT_CONTAINS', 'NOT_EQUALS', 'INCLUDING_REGEX', 'EXCLUDING_REGEX'), 0)
        # Show the Submit button:
        submitted = st.form_submit_button("Submit")
        # On Submit Clicked:
        if submitted:
            # Validate Inputs:
            if page_expression == '':
                page_operator = 'None'
            if query_expression == '':
                query_operator = 'None'
            if selected_country == []:
                country_operator = 'None'
            if selected_device == []:
                device_operator = 'None'
            # Scan website using Google:
            final_df = scan_website(property, numberOfRows, type_selectbox, selected_country, country_operator, selected_device, device_operator, start_date, end_date, page_operator, page_expression, query_operator, query_expression)
            if len(final_df) == 0:
                st.warning("Did not find any records. (" + str(len(final_df)) + ")")
            else:
                # Optionally add the Branded Column if the user has provided a branded keyword:
                if branded_kw != '': # If branded_kw is empty then drop branded column
                    final_df['Branded'] = final_df['query'].str.contains(branded_kw) # Add Branded Column
                # Preview CSV Data:
                st.write("Preview:")
                st.dataframe(final_df)
                st.success("Successfully found " + str(len(final_df)) + " records.")
                # Convert DF to CSV and pass it to a global variable used by the download CSV button:
                CSV = final_df.to_csv().encode('utf-8')
                CSV_DOWNLOADABLE = True # Streamlit forms can't contain multiple buttons

# D. Show CSV Download Button when CSV data exists:
if CSV_DOWNLOADABLE and CSV is not None:
    # Generate a file timestamp:
    current_time = str(datetime.datetime.now())
    current_time = "_".join(current_time.split()).replace(":","-")
    current_time = current_time[:-7]
    # Show the CSV Button:
    st.download_button("Download CSV", CSV, "GSC_API" + "_" + str(numberOfRows) + "_" + current_time + ".csv", "text/csv", key='download-csv')

## --------------------------------------------------------------------
