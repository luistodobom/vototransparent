import streamlit as st
import pandas as pd
import re
import unicodedata
from datetime import datetime # Added for GOVERNMENT_PERIODS
import altair as alt # Added for the new chart

# For extracting BID
import os
import json
from ast import literal_eval
from party_matching import parse_proposing_party_list

# --- Helper Functions ---


# --- Page Configuration ---
st.set_page_config(
    page_title="VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide", # Keep wide for now, can adjust with CSS
    initial_sidebar_state="collapsed"
)

# --- Custom CSS for styling ---
# Injected CSS for centering and cleaner look
st.markdown("""
<style>
    /* General body styling (optional, Streamlit handles most) */
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol";
    }

    /* Main container for centering content */
    .main .block-container {
        max-width: 900px; /* Adjust as needed */
        padding-left: 2rem;
        padding-right: 2rem;
        padding-top: 2rem;
        padding-bottom: 2rem;
        margin: auto; /* Centers the block-container */
    }

    /* Center align text for specific elements if needed */
    .stApp > header { /* Targets the Streamlit header */
        display: none; /* Hide default Streamlit header if we are making a custom one */
    }

    /* Custom title styling */
    .custom-title {
        text-align: center;
        font-size: 2.5em; /* Larger title */
        font-weight: bold;
        margin-bottom: 0.5em;
        color: #333; /* Darker text color */
    }

    .custom-subtitle {
        text-align: center;
        font-size: 1.2em;
        margin-bottom: 2em;
        color: #555; /* Slightly lighter subtitle */
    }

    /* Search input styling */
    div[data-testid="stTextInput"] > div > div > input {
        border-radius: 20px; /* Rounded corners for search bar */
        border: 1px solid #ccc;
        padding: 0.75em 1em;
    }
    div[data-testid="stTextInput"] > label {
        display: none; /* Hide the label above search input if placeholder is enough */
    }

    /* Search button styling */
    button[data-testid="baseButton-secondary"][aria-label="Pesquisar"],
    button[key="search_button"] {
        border-radius: 50% !important;
        border: 1px solid #007bff !important;
        background-color: #007bff !important;
        color: white !important;
        padding: 0.5em !important;
        height: 2.5rem !important;
        width: 2.5rem !important;
        font-size: 1em !important;
        min-width: 2.5rem !important;
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
    }
    button[data-testid="baseButton-secondary"][aria-label="Pesquisar"]:hover,
    button[key="search_button"]:hover {
        background-color: #0056b3 !important;
        border-color: #0056b3 !important;
    }

    /* Mobile responsive search layout */
    @media (max-width: 768px) {
        /* Force search container to stack vertically on mobile */
        div[data-testid="column"]:has(div[data-testid="stTextInput"]),
        div[data-testid="column"]:has(button[key="search_button"]) {
            width: 100% !important;
            flex: 1 1 100% !important;
            max-width: 100% !important;
        }
        
        /* Make search button full width and styled for mobile */
        button[data-testid="baseButton-secondary"][aria-label="Pesquisar"],
        button[key="search_button"] {
            width: 100% !important;
            min-width: 100% !important;
            border-radius: 8px !important;
            margin-top: 0.5rem !important;
            height: 3rem !important;
            font-size: 1.1em !important;
        }
        
        /* Adjust search input container for mobile */
        div[data-testid="stTextInput"] > div > div > input {
            margin-bottom: 0.5rem !important;
        }
        
        /* Ensure parent container allows wrapping */
        div[data-testid="stHorizontalBlock"] {
            flex-wrap: wrap !important;
        }
    }
    
    /* Tablet responsive adjustments */
    @media (max-width: 1024px) and (min-width: 769px) {
        button[data-testid="baseButton-secondary"][aria-label="Pesquisar"],
        button[key="search_button"] {
            width: 100% !important;
            min-width: 100% !important;
        }
    }

    /* Button styling for search results */
    div[data-testid="stButton"] > button {
        border-radius: 8px;
        border: 1px solid #007bff; /* Example primary color */
        background-color: #007bff;
        color: white;
        padding: 0.5em 1em;
        width: 100%; /* Make button take full width of its container */
        transition: background-color 0.3s ease;
    }
    div[data-testid="stButton"] > button:hover {
        background-color: #0056b3;
        border-color: #0056b3;
    }

    /* Styling for containers holding search results */
    div[data-testid="stVerticalBlock"] div[data-testid="stExpander"] {
        border: none; /* Remove border from expanders if used */
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); /* Subtle shadow */
        border-radius: 8px;
        margin-bottom: 1em;
    }
    div[data-testid="stVerticalBlock"] div[data-testid="stContainer"] {
         border: 1px solid #e0e0e0;
         border-radius: 8px;
         padding: 1em;
         margin-bottom: 1em;
         box-shadow: 0 1px 3px rgba(0,0,0,0.03);
    }

    /* Footer styling */
    .footer {
        text-align: center;
        margin-top: 3em;
        padding-top: 1em;
        border-top: 1px solid #eee;
        font-size: 0.9em;
        color: #777;
    }

    /* Hide Streamlit's default sidebar navigation for multi-page apps */
    div[data-testid="stSidebarNav"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)


# --- Data Loading ---
@st.cache_data
def load_data(csv_path="data/parliament_data.csv"):
    try:
        raw_df = pd.read_csv(csv_path)
        all_vote_details = []

        def extract_bid(url):
            if pd.isna(url) or url == "":
                return None
            match = re.search(r'BID=(\d+)', url)
            if match:
                return match.group(1)
            match_alt = re.search(r'Detalhe(?:Iniciativa|ProjetoVoto)\.aspx\?BID=(\d+)', str(url))
            if match_alt:
                return match_alt.group(1)
            return None
        
        for index, row in raw_df.iterrows(): # Use index for fallback id
            issue_id_str = extract_bid(row.get('proposal_gov_link'))
            if issue_id_str is None:
                issue_id_str = row.get('proposal_name_from_session', f"fallback_id_{index}")

            title = row.get('proposal_name_from_session', 'Título não disponível.')
            description_text = row.get('proposal_summary_general', 'Descrição não disponível.')
            hyperlink_url = row.get('proposal_document_url', row.get('proposal_gov_link', ''))
            issue_type = row.get('proposal_document_type', 'N/A')
            authors_json_str = str(row.get('proposal_authors_json', '[]')) if pd.notna(row.get('proposal_authors_json')) else '[]'

            summary_analysis = str(row.get('proposal_summary_analysis', '')) if pd.notna(row.get('proposal_summary_analysis')) else ''
            summary_fiscal = str(row.get('proposal_summary_fiscal_impact', '')) if pd.notna(row.get('proposal_summary_fiscal_impact')) else ''
            summary_colloquial = str(row.get('proposal_summary_colloquial', '')) if pd.notna(row.get('proposal_summary_colloquial')) else ''
            session_pdf_url_val = row.get('session_pdf_url', '')
            session_date_val = row.get('session_date', '')

            # New fields
            proposal_short_title_val = str(row.get('proposal_short_title', 'N/A'))
            proposal_proposing_party_val = str(row.get('proposal_proposing_party', 'N/A'))
            proposal_approval_status_raw = row.get('proposal_approval_status')

            # Skip proposals that don't have a valid proposal_short_title
            if pd.isna(row.get('proposal_short_title')) or proposal_short_title_val in ['N/A', 'nan', '', 'None']:
                continue

            # Parse proposal_category as list of integers
            proposal_category_raw = row.get('proposal_category', '[]')
            proposal_category_list = []
            if pd.notna(proposal_category_raw) and str(proposal_category_raw).strip():
                try:
                    # Handle both string representation of list and actual list
                    if isinstance(proposal_category_raw, str):
                        proposal_category_list = json.loads(proposal_category_raw.replace("'", '"'))
                    elif isinstance(proposal_category_raw, list):
                        proposal_category_list = proposal_category_raw
                    # Ensure all elements are integers
                    proposal_category_list = [int(cat) for cat in proposal_category_list if str(cat).isdigit()]
                except (json.JSONDecodeError, ValueError):
                    proposal_category_list = []

            # Parse proposal_proposing_party using helper function
            proposal_proposing_party_list = parse_proposing_party_list(proposal_proposing_party_val)
            
            # Create display string for proposing party
            if proposal_proposing_party_list:
                proposal_proposing_party_display = ', '.join(proposal_proposing_party_list)
            else:
                proposal_proposing_party_display = 'N/A'

            # Extract parties and votes information from voting_details_json
            voting_details_raw = row.get('voting_details_json', '')
            if pd.isna(voting_details_raw) or voting_details_raw == '':
                continue  # Skip rows with no voting info
            
            try:
                voting_details = json.loads(voting_details_raw)
            except (ValueError, json.JSONDecodeError):
                continue  # Skip rows with malformed voting info

            # Handle the actual CSV format: {"PS": {"Favor": 120, ...}, "PSD": {...}, ...}
            if not isinstance(voting_details, dict):
                continue

            # Determine overall vote outcome
            total_favor = sum(party_data.get('Favor', 0) for party_data in voting_details.values())
            total_contra = sum(party_data.get('Contra', 0) for party_data in voting_details.values())
            
            overall_outcome = "Resultado Desconhecido"
            if pd.notna(proposal_approval_status_raw):
                try:
                    status_as_int = int(proposal_approval_status_raw)
                    if status_as_int == 1:
                        overall_outcome = "Aprovado"
                    elif status_as_int == 0:
                        overall_outcome = "Rejeitado"
                except ValueError:
                    pass

            total_active_votes = total_favor + total_contra
            is_unanimous = total_active_votes > 0 and (total_favor == total_active_votes or total_contra == total_active_votes)

            # Store party votes for this proposal
            proposal_party_votes_list = []
            for party_name, votes_data in voting_details.items():
                if not isinstance(votes_data, dict):
                    continue
                
                votes_favor = votes_data.get('Favor', 0)
                votes_against = votes_data.get('Contra', 0)
                votes_abstention = votes_data.get('Abstenção', 0)
                votes_not_voted = votes_data.get('Não Votaram', 0)
                
                # Skip party if no data
                if all(v == 0 for v in [votes_favor, votes_against, votes_abstention, votes_not_voted]):
                    continue

                proposal_party_votes_list.append({
                    'party': party_name,
                    'votes_favor': votes_favor,
                    'votes_against': votes_against,
                    'votes_abstention': votes_abstention,
                    'votes_not_voted': votes_not_voted,
                })
            
            # Determine overall vote outcome and unanimity for the proposal
            is_unanimous_bool = False

            if proposal_party_votes_list: # If we have party votes
                # Calculate totals for unanimity check
                total_favor_check = sum(p['votes_favor'] for p in proposal_party_votes_list)
                total_against_check = sum(p['votes_against'] for p in proposal_party_votes_list)
                total_abstention_check = sum(p['votes_abstention'] for p in proposal_party_votes_list)
                
                if total_favor_check > 0 and total_against_check == 0 and total_abstention_check == 0:
                    is_unanimous_bool = True
                elif total_against_check > 0 and total_favor_check == 0 and total_abstention_check == 0:
                    is_unanimous_bool = True
                elif total_abstention_check > 0 and total_favor_check == 0 and total_against_check == 0:
                    is_unanimous_bool = True # Unanimous abstention among those who voted

            if proposal_party_votes_list:
                for p_vote in proposal_party_votes_list:
                    all_vote_details.append({
                        'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                        'hyperlink': hyperlink_url, 'vote_outcome': overall_outcome, 'is_unanimous': is_unanimous_bool,
                        'issue_type': issue_type, 'party': p_vote['party'],
                        'votes_favor': p_vote['votes_favor'], 'votes_against': p_vote['votes_against'],
                        'votes_abstention': p_vote['votes_abstention'], 'votes_not_voted': p_vote['votes_not_voted'],
                        'authors_json_str': authors_json_str,
                        'proposal_summary_analysis': summary_analysis,
                        'proposal_summary_fiscal_impact': summary_fiscal,
                        'proposal_summary_colloquial': summary_colloquial,
                        'session_pdf_url': session_pdf_url_val,
                        'session_date': session_date_val,
                        'proposal_category_list': proposal_category_list,
                        'proposal_short_title': proposal_short_title_val,
                        'proposal_proposing_party': proposal_proposing_party_display,
                        'proposal_proposing_party_list': proposal_proposing_party_list,
                        'proposal_approval_status': proposal_approval_status_raw,
                    })
            else:
                all_vote_details.append({
                    'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                    'hyperlink': hyperlink_url, 'vote_outcome': overall_outcome, 'is_unanimous': is_unanimous_bool,
                    'issue_type': issue_type, 'party': 'N/A',
                    'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0, 'votes_not_voted': 0,
                    'authors_json_str': authors_json_str,
                    'proposal_summary_analysis': summary_analysis,
                    'proposal_summary_fiscal_impact': summary_fiscal,
                    'proposal_summary_colloquial': summary_colloquial,
                    'session_pdf_url': session_pdf_url_val,
                    'session_date': session_date_val,
                    'proposal_category_list': proposal_category_list,
                    'proposal_short_title': proposal_short_title_val,
                    'proposal_proposing_party': proposal_proposing_party_display,
                    'proposal_proposing_party_list': proposal_proposing_party_list,
                    'proposal_approval_status': proposal_approval_status_raw,
                })
        
        if not all_vote_details:
            st.info("No vote data could be processed.")
            return pd.DataFrame()
        
        df = pd.DataFrame(all_vote_details)
        
        # Ensure session_date column exists and convert to datetime
        if 'session_date' in df.columns:
            df['session_date'] = pd.to_datetime(df['session_date'], errors='coerce')
        else:
            df['session_date'] = pd.NaT

        # Ensure new columns exist and handle types/NaNs
        if 'proposal_short_title' not in df.columns:
            df['proposal_short_title'] = 'N/A'
        df['proposal_short_title'] = df['proposal_short_title'].fillna('N/A')

        if 'proposal_proposing_party' not in df.columns:
            df['proposal_proposing_party'] = 'N/A'
        df['proposal_proposing_party'] = df['proposal_proposing_party'].fillna('N/A')

        if 'proposal_approval_status' not in df.columns:
            df['proposal_approval_status'] = pd.NA # Use pd.NA for integer with missing
        # Convert to numeric, coercing errors. This will make it float if NaNs are present.
        df['proposal_approval_status'] = pd.to_numeric(df['proposal_approval_status'], errors='coerce')
            
        return df

    except FileNotFoundError: 
        st.error(f"Error: Data file '{csv_path}' not found.")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        st.error(f"Error: Data file '{csv_path}' is empty.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


data_df = load_data()

# --- Helper function to normalize text ---
# Define GOVERNMENT_PERIODS and TARGET_PARTIES after data loading and helper functions
GOVERNMENT_PERIODS = {
    "Todos": {"start": None, "end": None},
    "XXI Governo (Nov 2015 - Out 2019)": {
        "start": datetime(2015, 11, 26),
        "end": datetime(2019, 10, 26)
    },
    "XXII Governo (Out 2019 - Mar 2022)": {
        "start": datetime(2019, 10, 26),
        "end": datetime(2022, 3, 30)
    },
    "XXIII Governo (Mar 2022 - Abr 2024)": {
        "start": datetime(2022, 3, 30),
        "end": datetime(2024, 4, 2)
    },
    "XXIV Governo (Abr 2024 - Jun 2025)": {
        "start": datetime(2024, 4, 2),
        "end": datetime(2025, 6, 5)
    },
    "XXV Governo (Jun 2025 - Presente)": {
        "start": datetime(2025, 6, 5),
        "end": None
    }
}

TARGET_PARTIES = ["PS", "PSD", "CH", "IL", "PCP", "BE", "PAN", "L", "CDS-PP"]

def normalize_text(text):
    # Remove accents
    nfkd_form = unicodedata.normalize('NFKD', str(text))
    text_without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    # Remove special characters and convert to lowercase
    text_without_special_chars = re.sub(r'[^a-zA-Z0-9\\s]', '', text_without_accents)
    return text_without_special_chars.lower()

# --- Homepage ---
# Use markdown for custom styled title and subtitle
st.markdown("<div class='custom-title'>🇵🇹 VotoTransparente</div>", unsafe_allow_html=True)
st.markdown("<div class='custom-subtitle'>O Seu Guia para as Votações Parlamentares na Assembleia da República Portuguesa.</div>", unsafe_allow_html=True)
# st.title(\"🇵🇹 VotoTransparente: O Seu Guia para as Votações Parlamentares\") # Original title
# st.markdown(\"Explore como os partidos políticos votam na Assembleia da República Portuguesa.\") # Original subtitle
# st.markdown(\"---\") # Remove this divider or style it via CSS if needed

if not data_df.empty:
    # Initialize session state for search query
    if 'search_query' not in st.session_state:
        st.session_state.search_query = ''
    if 'last_page' not in st.session_state:
        st.session_state.last_page = 'home'

    # --- Search Functionality ---
    # Create columns for search input and button - responsive layout
    search_col1, search_col2 = st.columns([5, 1], gap="small")
    
    with search_col1:
        search_query = st.text_input(
            "Pesquisar propostas", # Add a descriptive label
            placeholder="Pesquisar por palavra-chave...",
            value=st.session_state.search_query,  # Restore previous search query
            key="search_input",
            label_visibility="collapsed" # Add this if you want to hide the label "Pesquisar propostas",
        )
    
    with search_col2:
        search_button_clicked = st.button("🔍", key="search_button", help="Pesquisar", use_container_width=True)

    # Update session state when search query changes or search button is clicked
    if search_query != st.session_state.search_query or search_button_clicked:
        st.session_state.search_query = search_query
        st.rerun()

    if search_query:
        # Perform a case-insensitive search across relevant fields
        # Consolidate data to one row per issue for search results
        # Ensure 'issue_identifier' is unique for drop_duplicates
        search_df_unique_issues = data_df.drop_duplicates(subset=['issue_identifier']).copy()

        # Normalize search query
        normalized_search_query = normalize_text(search_query)

        # Ensure searchable fields are strings for searching
        search_df_unique_issues['description'] = search_df_unique_issues['description'].astype(str)
        search_df_unique_issues['full_title'] = search_df_unique_issues['full_title'].astype(str)
        search_df_unique_issues['proposal_short_title'] = search_df_unique_issues['proposal_short_title'].astype(str)
        # 'issue_identifier' is already string from load_data

        # Apply normalization to searchable columns
        # Create temporary columns for normalized search
        search_df_unique_issues['normalized_full_title'] = search_df_unique_issues['full_title'].apply(normalize_text)
        search_df_unique_issues['normalized_description'] = search_df_unique_issues['description'].apply(normalize_text)
        search_df_unique_issues['normalized_proposal_short_title'] = search_df_unique_issues['proposal_short_title'].apply(normalize_text)
        search_df_unique_issues['normalized_issue_identifier'] = search_df_unique_issues['issue_identifier'].astype(str).apply(normalize_text)


        results = search_df_unique_issues[
            search_df_unique_issues['normalized_full_title'].str.contains(normalized_search_query, case=False, na=False) |
            search_df_unique_issues['normalized_description'].str.contains(normalized_search_query, case=False, na=False) |
            search_df_unique_issues['normalized_proposal_short_title'].str.contains(normalized_search_query, case=False, na=False) |
            search_df_unique_issues['normalized_issue_identifier'].str.contains(normalized_search_query, case=False, na=False)
        ]

        if not results.empty:
            st.markdown(f"**Resultados da pesquisa para \"{search_query}\":**")
            
            # Sort results by date (newest first) if session_date is available
            if 'session_date' in results.columns:
                results = results.sort_values(by='session_date', ascending=False, na_position='last')
            
            # Group results by date for display
            if 'session_date' in results.columns:
                grouped_results = {}
                for _, row_data in results.iterrows(): # Changed variable name to avoid conflict
                    date_key = row_data['session_date']
                    if pd.isna(date_key):
                        date_str = "Data não disponível"
                    else:
                        date_str = date_key.strftime("%d/%m/%Y")
                    
                    if date_str not in grouped_results:
                        grouped_results[date_str] = []
                    grouped_results[date_str].append(row_data)

                # Display grouped results
                for date_str, results_for_date in grouped_results.items():
                    st.markdown(f"### {date_str}")
                    
                    for row in results_for_date: # This is the 'row' from the original code
                        with st.container(border=True):
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                # --- Resumo da Proposta ---
                                proposing_party_text = ""
                                if pd.notna(row.get('proposal_proposing_party')) and row['proposal_proposing_party'] != 'N/A' and str(row['proposal_proposing_party']).lower() != 'nan':
                                    proposing_party_text = row['proposal_proposing_party']

                                session_date_str_display = ""
                                if pd.notna(row.get('session_date')):
                                    session_date_str_display = row['session_date'].strftime("%d/%m/%Y")
                                    if proposing_party_text:
                                        st.markdown(f"**{proposing_party_text} - {session_date_str_display}**")
                                    else:
                                        st.markdown(f"**{session_date_str_display}**")
                                else:
                                    if proposing_party_text:
                                        st.markdown(f"**{proposing_party_text}**")

                                # Display project identifier as main title
                                if pd.notna(row['proposal_short_title']) and row['proposal_short_title'] != 'N/A':
                                    st.markdown(f"#### {row['proposal_short_title']}")
                                else:
                                    st.markdown(f"#### {row['issue_identifier']}")
                                
                                # Display full title as descriptive text
                                st.markdown(f"*{row['full_title']}*")

                                vote_outcome = row.get('vote_outcome', 'N/A')
                                if vote_outcome == "Aprovado":
                                    st.markdown('<span style="font-size: 1.2em;">✅ **Aprovado**</span>', unsafe_allow_html=True)
                                elif vote_outcome == "Rejeitado":
                                    st.markdown('<span style="font-size: 1.2em;">❌ **Rejeitado**</span>', unsafe_allow_html=True)
                                else:
                                    st.markdown(f'<span style="font-size: 1.2em;">❓ **{vote_outcome}**</span>', unsafe_allow_html=True)
                                # --- End Resumo da Proposta ---

                            with col2:
                                if st.button(f"Ver detalhes", key=f"search_{row['issue_identifier']}", use_container_width=True):
                                    st.session_state.last_page = 'home'
                                    st.session_state.selected_issue_identifier = str(row['issue_identifier'])
                                    # Set query parameters before navigation
                                    st.query_params.update({
                                        "issue_id": str(row['issue_identifier']),
                                        "from_page": "home",
                                        "search_query": search_query
                                    })
                                    st.switch_page("pages/2_Topic_Details.py")

                            # Expander for other descriptions
                            with st.expander("Mais detalhes da proposta"):
                                if pd.notna(row['description']) and row['description'].strip() and row['description'] != 'Descrição não disponível.':
                                    st.markdown(f"**Descrição Geral:**")
                                    st.markdown(f"_{row['description']}_")
                                    st.markdown("---") # Separator if other details follow
                                
                                if pd.notna(row['proposal_summary_analysis']) and row['proposal_summary_analysis'].strip():
                                    st.markdown("**Análise:**")
                                    st.markdown(row['proposal_summary_analysis'])
                                if pd.notna(row['proposal_summary_fiscal_impact']) and row['proposal_summary_fiscal_impact'].strip():
                                    st.markdown("**Impacto Fiscal:**")
                                    st.markdown(row['proposal_summary_fiscal_impact'])
                                if pd.notna(row['proposal_summary_colloquial']) and row['proposal_summary_colloquial'].strip():
                                    st.markdown("🗣️ **Sem precisar de dicionário**")
                                    st.markdown(row['proposal_summary_colloquial'])
                                if not ((pd.notna(row['proposal_summary_analysis']) and row['proposal_summary_analysis'].strip()) or 
                                        (pd.notna(row['proposal_summary_fiscal_impact']) and row['proposal_summary_fiscal_impact'].strip()) or 
                                        (pd.notna(row['proposal_summary_colloquial']) and row['proposal_summary_colloquial'].strip())):
                                    st.markdown("Não há detalhes adicionais disponíveis.")
            else:
                # Fallback to original display if no session_date
                for iter_idx, row_tuple in enumerate(results.iterrows()): # Use enumerate for unique keys if needed
                    _, row = row_tuple  # Unpack the tuple from iterrows()
                    with st.container(border=True):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            # --- Resumo da Proposta ---
                            proposing_party_text = ""
                            if pd.notna(row.get('proposal_proposing_party')) and row['proposal_propondo_party'] != 'N/A' and str(row['proposal_propondo_party']).lower() != 'nan':
                                proposing_party_text = row['proposal_propondo_party']

                            # Date is not available in this fallback, so only party
                            if proposing_party_text:
                                st.markdown(f"**{proposing_party_text}**")

                            # Display project identifier as main title
                            if pd.notna(row['proposal_short_title']) and row['proposal_short_title'] != 'N/A':
                                st.markdown(f"#### {row['proposal_short_title']}")
                            else:
                                st.markdown(f"#### {row['issue_identifier']}")
                            
                            # Display full title as descriptive text
                            st.markdown(f"*{row['full_title']}*")

                            vote_outcome = row.get('vote_outcome', 'N/A')
                            if vote_outcome == "Aprovado":
                                st.markdown('<span style="font-size: 1.2em;">✅ **Aprovado**</span>', unsafe_allow_html=True)
                            elif vote_outcome == "Rejeitado":
                                st.markdown('<span style="font-size: 1.2em;">❌ **Rejeitado**</span>', unsafe_allow_html=True)
                            else:
                                st.markdown(f'<span style="font-size: 1.2em;">❓ **{vote_outcome}**</span>', unsafe_allow_html=True)
                            # --- End Resumo da Proposta ---

                        with col2:
                            # Use iter_idx for a more robust unique key in fallback
                            if st.button(f"Ver detalhes", key=f"search_fallback_{row['issue_identifier']}_{iter_idx}", use_container_width=True):
                                st.session_state.last_page = 'home'
                                st.session_state.selected_issue_identifier = str(row['issue_identifier'])
                                # Set query parameters before navigation
                                st.query_params.update({
                                    "issue_id": str(row['issue_identifier']),
                                    "from_page": "home",
                                    "search_query": search_query
                                })
                                st.switch_page("pages/2_Topic_Details.py")

                        # Expander for other descriptions
                        with st.expander("Mais detalhes da proposta"):
                            if pd.notna(row['description']) and row['description'].strip() and row['description'] != 'Descrição não disponível.':
                                st.markdown(f"**Descrição Geral:**")
                                st.markdown(f"_{row['description']}_")
                                st.markdown("---") # Separator if other details follow

                            if pd.notna(row['proposal_summary_analysis']) and row['proposal_summary_analysis'].strip():
                                st.markdown("**Análise:**")
                                st.markdown(row['proposal_summary_analysis'])
                            if pd.notna(row['proposal_summary_fiscal_impact']) and row['proposal_summary_fiscal_impact'].strip():
                                st.markdown("**Impacto Fiscal:**")
                                st.markdown(row['proposal_summary_fiscal_impact'])
                            if pd.notna(row['proposal_summary_colloquial']) and row['proposal_summary_colloquial'].strip():
                                st.markdown("🗣️ **Sem precisar de dicionário**")
                                st.markdown(row['proposal_summary_colloquial'])
                            if not ((pd.notna(row['proposal_summary_analysis']) and row['proposal_summary_analysis'].strip()) or 
                                    (pd.notna(row['proposal_summary_fiscal_impact']) and row['proposal_summary_fiscal_impact'].strip()) or 
                                    (pd.notna(row['proposal_summary_colloquial']) and row['proposal_summary_colloquial'].strip())):
                                st.markdown("Não há detalhes adicionais disponíveis.")
        else:
            st.info(f"Nenhuma votação encontrada para \"{search_query}\".") # Simpler message
    
    # Centered "Browse All Votes" link/button
    st.markdown("<br>", unsafe_allow_html=True)
    cols_browse = st.columns([1,2,1])
    with cols_browse[1]:
        if st.button("Navegar por Todas as Votações", use_container_width=True, key="browse_all_main"):
            st.session_state.last_page = 'home'
            st.query_params["from_page"] = "home"
            st.switch_page("pages/1_Browse_Topics.py")

    # --- Party Statistics Section ---
    st.markdown("---") # Visual separator
    st.markdown("<h3 style='text-align: center;'>Propostas por Partido Político</h3>", unsafe_allow_html=True)

    _, col_gov_select, col_gov_empty = st.columns([1, 2, 1])
    with col_gov_select:
        selected_government_stats_label = st.selectbox(
            "Selecionar Período Governativo:",
            options=list(GOVERNMENT_PERIODS.keys()),
            index=4,  # Default to PSD Government
            key="gov_period_stats_filter"
        )
    # col_gov_empty is intentionally left empty for 1/3 spacing

    # Filter data based on selected government period
    filtered_df_stats = data_df.copy()
    if selected_government_stats_label != "Todos":
        period_info = GOVERNMENT_PERIODS[selected_government_stats_label]
        start_date = period_info["start"]
        end_date = period_info["end"]

        if 'session_date' in filtered_df_stats.columns and not pd.api.types.is_datetime64_any_dtype(filtered_df_stats['session_date']):
            filtered_df_stats['session_date'] = pd.to_datetime(filtered_df_stats['session_date'], errors='coerce')

        if pd.notna(start_date):
            filtered_df_stats = filtered_df_stats[~filtered_df_stats['session_date'].isna() & (filtered_df_stats['session_date'] >= start_date)]
        if pd.notna(end_date):
            filtered_df_stats = filtered_df_stats[~filtered_df_stats['session_date'].isna() & (filtered_df_stats['session_date'] <= end_date)]
    
    if filtered_df_stats.empty and selected_government_stats_label != "Todos":
        st.info(f"Não foram encontradas propostas para o período '{selected_government_stats_label}'.")
    elif not data_df.empty:
        base_df_for_period = filtered_df_stats # This is already time-filtered or the full data_df if "Todos"

        # Calculate total unique proposals in the selected period with a known outcome (Approved/Rejected)
        unique_proposals_in_period_df = base_df_for_period.drop_duplicates(subset=['issue_identifier'])
        known_status_proposals_in_period_df = unique_proposals_in_period_df[unique_proposals_in_period_df['proposal_approval_status'].isin([0.0, 1.0])]
        total_proposals_for_denominator = len(known_status_proposals_in_period_df)

        party_proposal_stats = {party: {'Approved': 0, 'Rejected': 0} for party in TARGET_PARTIES}

        # Iterate over unique proposals in the period to populate party_proposal_stats
        for _, row in unique_proposals_in_period_df.iterrows(): # Iterate unique proposals
            proposing_party_list = row.get('proposal_proposing_party_list', [])
            approval_status = row.get('proposal_approval_status')

            if approval_status == 1.0:  # Approved
                for party_name in TARGET_PARTIES:
                    if party_name in proposing_party_list:
                        party_proposal_stats[party_name]['Approved'] += 1
            elif approval_status == 0.0:  # Rejected
                for party_name in TARGET_PARTIES:
                    if party_name in proposing_party_list:
                        party_proposal_stats[party_name]['Rejected'] += 1
        
        chart_data_list = []
        if total_proposals_for_denominator > 0:
            for party, counts in party_proposal_stats.items():
                approved_count = counts['Approved']
                rejected_count = counts['Rejected']

                percentage_approved = (approved_count / total_proposals_for_denominator) * 100
                percentage_rejected = (rejected_count / total_proposals_for_denominator) * 100

                if percentage_approved > 0:
                    chart_data_list.append({'Party': party, 'Status': 'Aprovado', 'Percentage': percentage_approved})
                if percentage_rejected > 0:
                    chart_data_list.append({'Party': party, 'Status': 'Rejeitado', 'Percentage': percentage_rejected})
        
        chart_df = pd.DataFrame(chart_data_list)

        chart_df_filtered = pd.DataFrame() # Initialize as empty
        if not chart_df.empty:
            party_totals = chart_df.groupby('Party')['Percentage'].sum()
            parties_with_proposals = party_totals[party_totals > 0].index.tolist()
            chart_df_filtered = chart_df[chart_df['Party'].isin(parties_with_proposals)]

        if not chart_df_filtered.empty:
            status_order = ['Aprovado', 'Rejeitado']
            color_scale = alt.Scale(domain=status_order, range=['#2ca02c', '#d62728']) # Green, Red

            chart_df_filtered['status_order_val'] = chart_df_filtered['Status'].map({'Aprovado': 0, 'Rejeitado': 1})

            chart = alt.Chart(chart_df_filtered).mark_bar().encode(
                x=alt.X('sum(Percentage):Q', title='%', stack='zero', axis=alt.Axis(format='.0f')), # Format to 1 decimal place
                y=alt.Y('Party:N', sort=alt.EncodingSortField(field="Percentage", op="sum", order='descending'), title='Partido'),
                color=alt.Color('Status:N', scale=color_scale, legend=alt.Legend(title='Resultado da Votação', orient='bottom')),
                order=alt.Order('status_order_val:Q', sort='ascending')
            )
            # Use columns to control chart width
            _, col_chart_viz, col_chart_empty = st.columns([1,4,1]) # Chart takes 2/3, empty space takes 1/3
            with col_chart_viz:
                st.altair_chart(chart, use_container_width=True)
        else:
            st.info(f"Não há dados de propostas para exibir para o período '{selected_government_stats_label}' com os partidos selecionados ou nenhuma proposta com resultado conhecido no período.")

else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro acima.")

# --- Footer ---
st.markdown("<div class='footer'>Desenvolvido com ❤️ por Luis Berenguer Todo-Bom<br>Dados extraídos de documentos oficiais da Assembleia da República e processados com Inteligência Artificial (dados de 2020 em diante).<br>Este projeto é Open-Source e aceita contribuições. A informação pode conter erros. <a href=https://github.com/luistodobom/vototransparente/issues>Reporte erros no Github.</a></div>", unsafe_allow_html=True)


st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
st.sidebar.page_link("pages/1_Browse_Topics.py", label="Todas as Votações", icon="📜")