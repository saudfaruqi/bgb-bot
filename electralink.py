import streamlit as st
import pandas as pd
import requests
import time
from io import BytesIO
import base64

# Page config
st.set_page_config(
    page_title="Electralink EAC Extractor",
    page_icon="⚡",
    layout="wide"
)

# Constants
API_KEY = "2Y4W3zaGAPZqHVw8B2uc6d6fZ"
API_PASSWORD = "FBVfPCKhZFaKRz5v"
API_URL = "https://api.electralink.co.uk/v2/eac"
MAX_REQUESTS = 100

# Session state for API testing
if 'api_working' not in st.session_state:
    st.session_state.api_working = None

# Styling
st.markdown("""
    <style>
        .main {
            padding: 2rem;
        }
        .stTitle {
            color: #1f77b4;
        }
    </style>
""", unsafe_allow_html=True)

# Title
st.title("⚡ Electralink EAC Data Extractor")
st.markdown("Extract data from Electralink API for up to 20 MPANs")

# Sidebar info
with st.sidebar:
    st.header("ℹ️ Information")
    st.info(f"""
    **API Limit:** {MAX_REQUESTS} requests per run
    
    **Required:** Excel file with MPAN column
    
    **Output Fields:**
    - Site Building Name
    - Site Street No
    - Site Street 1 & 2
    - Site Town
    - Site Postcode
    - MPAN & Meter Number
    - Usage & AQ
    - Supplier Info
    """)
    
    st.divider()
    st.header("🔧 API Configuration")
    
    with st.expander("Advanced Settings"):
        st.write("**Current API Endpoint:**")
        st.code(API_URL)
        
        custom_url = st.text_input(
            "Override API URL (leave blank to use default)",
            value="",
            placeholder=API_URL
        )
        
        if st.button("🔍 Test API Connection"):
            try:
                headers = {
                    'api-key': API_KEY,
                    'api-password': API_PASSWORD,
                    'Content-Type': 'application/json'
                }
                test_url = custom_url or API_URL
                if '?' in test_url:
                    full_url = f"{test_url}1200036684781"
                else:
                    full_url = f"{test_url}?mpan=1200036684781"
                    
                response = requests.get(
                    full_url,
                    headers=headers,
                    timeout=5
                )
                if response.status_code in [200, 400, 404, 422]:
                    st.success("✅ API is reachable!")
                    st.info(f"Response status: {response.status_code}")
                    st.json(response.json() if response.text else {})
                    st.session_state.api_working = True
                else:
                    st.warning(f"⚠️ API returned status: {response.status_code}")
                    st.session_state.api_working = False
            except requests.exceptions.ConnectionError as e:
                st.error(f"❌ Cannot reach API")
                st.error("**Possible causes:**")
                st.error("• Domain doesn't exist (api.electralink.co.uk)")
                st.error("• Network/firewall blocking access")
                st.error("• Incorrect API URL")
                st.session_state.api_working = False
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")
                st.session_state.api_working = False

def format_results(api_data):
    """Format API response to output fields"""
    if not api_data:
        return None
    
    # Extract address lines
    lines = {
        'line1': api_data.get('metering_point_address_line_1', ''),
        'line2': api_data.get('metering_point_address_line_2', ''),
        'line3': api_data.get('metering_point_address_line_3', ''),
        'line4': api_data.get('metering_point_address_line_4', ''),
        'line5': api_data.get('metering_point_address_line_5', ''),
        'line6': api_data.get('metering_point_address_line_6', ''),
        'line7': api_data.get('metering_point_address_line_7', ''),
        'line8': api_data.get('metering_point_address_line_8', ''),
        'line9': api_data.get('metering_point_address_line_9', ''),
    }
    
    # Building Name (lines 1-2)
    site_building_name = ', '.join(filter(None, [lines['line1'], lines['line2']])) or ''
    
    # Site Street No (lines 3-4)
    site_street_no = ', '.join(filter(None, [lines['line3'], lines['line4']])) or ''

    # Site Street 1 (line 5)
    site_street_1 = lines['line5'] or ''

    # Site Street 2 (lines 6-7)
    site_street_2 = ', '.join(filter(None, [lines['line6'], lines['line7']])) or ''

    # Site Town (lines 8-9)
    site_town = ', '.join(filter(None, [lines['line8'], lines['line9']])) or ''
    
    return {
        'Site Building Name': site_building_name,
        'Site Street No': site_street_no,
        'Site Street 1': site_street_1,
        'Site Street 2': site_street_2,
        'Site Town': site_town,
        'Site Postcode': api_data.get('post_code', ''),
        'MPAN Topline': api_data.get('', ''),
        'Meter Number': api_data.get('mpan', ''),
        'Usage': api_data.get('total_eac', ''),
        'last updated': api_data.get('eac_efd', ''),
        'Proposed Start Date': api_data.get('supplier_efd', ''),
        'Energization Status': 'No' if api_data.get('et') == 'FALSE' else 'Yes',
        'Annualized AQ': api_data.get('', ''),
        'Supplier': api_data.get('supplier_name', '')
    }

def fetch_eac_data(mpan, api_url=API_URL):
    """Fetch EAC data from Electralink API"""
    try:
        headers = {
            'api-key': API_KEY,
            'api-password': API_PASSWORD,
            'Content-Type': 'application/json'
        }
        
        # Clean MPAN - remove any whitespace or special characters
        clean_mpan = str(mpan).strip()
        
        # Build URL properly without double ?mpan=
        if '?' in api_url:
            url = f"{api_url}{clean_mpan}"
        else:
            url = f"{api_url}?mpan={clean_mpan}"
        
        response = requests.get(
            url,
            headers=headers,
            timeout=10,
            verify=True
        )
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            st.error(f"❌ Authentication failed. Check API credentials.")
            return None
        elif response.status_code == 404:
            st.warning(f"MPAN {clean_mpan}: Not found (404)")
            return None
        elif response.status_code == 422:
            st.warning(f"MPAN {clean_mpan}: Invalid format (422)")
            return None
        else:
            st.warning(f"API Error for MPAN {clean_mpan}: Status {response.status_code}")
            return None
            
    except requests.exceptions.ConnectionError as e:
        st.error(f"Connection Error: API unreachable")
        return None
    except requests.exceptions.Timeout:
        st.error(f"Timeout for {mpan}: API response too slow")
        return None
    except Exception as e:
        st.error(f"Error fetching MPAN {mpan}: {str(e)}")
        return None

# File upload
col1, col2 = st.columns([2, 1])

with col1:
    uploaded_file = st.file_uploader(
        "📁 Upload Excel File (with MPAN column)",
        type=['xlsx', 'xls']
    )

if uploaded_file:
    st.success(f"✓ File loaded: {uploaded_file.name}")
    
    # Read Excel file
    try:
        df_input = pd.read_excel(uploaded_file)
        st.write(f"Found {len(df_input)} rows in Excel file")
        
        # Extract MPANs
        mpan_column = None
        for col in df_input.columns:
            if col.lower() == 'mpan':
                mpan_column = col
                break
        
        if not mpan_column:
            st.error("❌ No 'MPAN' column found in Excel file")
        else:
            mpans = df_input[mpan_column].astype(str).str.strip().tolist()
            mpans = [m for m in mpans if m and m != 'nan'][:MAX_REQUESTS]
            
            st.info(f"Found {len(mpans)} MPANs (max {MAX_REQUESTS})")
            
            # Process button
            if st.button("🚀 Start Processing", key="process_btn"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                results = []
                
                final_api_url = custom_url if custom_url else API_URL
                
                for idx, mpan in enumerate(mpans):
                    status_text.write(f"Processing {idx + 1}/{len(mpans)}: {mpan}")
                    
                    api_data = fetch_eac_data(mpan, final_api_url)
                    if api_data:
                        formatted = format_results(api_data)
                        if formatted:
                            results.append(formatted)
                    
                    progress_bar.progress((idx + 1) / len(mpans))
                    time.sleep(0.3)  # Rate limiting
                
                if results:
                    st.success(f"✅ Successfully processed {len(results)} MPANs")
                    
                    # Convert to DataFrame
                    df_results = pd.DataFrame(results)
                    
                    # Display results
                    st.subheader("📊 Data Preview")
                    st.dataframe(df_results, use_container_width=True)
                    
                    # Download button
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                        df_results.to_excel(writer, index=False, sheet_name='EAC Data')
                    
                    output.seek(0)
                    st.download_button(
                        label="📥 Download Results as Excel",
                        data=output.getvalue(),
                        file_name="electralink_eac_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # Summary stats
                    st.subheader("📈 Summary")
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Total Records", len(results))
                    col2.metric("Success Rate", f"{(len(results)/len(mpans)*100):.1f}%")
                    col3.metric("API Calls Used", f"{len(mpans)}/{MAX_REQUESTS}")
                else:
                    st.error("❌ No data extracted. Please check the MPANs and try again.")
                
                status_text.empty()
                progress_bar.empty()
    
    except Exception as e:
        st.error(f"Error reading Excel file: {str(e)}")
else:
    st.info("👆 Please upload an Excel file to begin")