import streamlit as st
import pandas as pd
import os
import json
import re # For extracting BID
from datetime import datetime
from ast import literal_eval
from party_matching import parse_proposing_party_list

# --- Constants for pagination ---
INITIAL_DISPLAY_COUNT = 20
LOAD_MORE_COUNT = 20

# --- Page Configuration ---
st.set_page_config(
    page_title="Todas as Votações - VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide"
)

st.markdown("""
<style>
    /* Hide Streamlit's default sidebar navigation for multi-page apps */
    div[data-testid="stSidebarNav"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Category mapping for display
CATEGORY_MAPPING = {
    0: "Saúde e Cuidados Sociais",
    1: "Educação e Competências", 
    2: "Defesa e Segurança Nacional",
    3: "Justiça, Lei e Ordem",
    4: "Economia e Finanças",
    5: "Bem-Estar e Segurança Social",
    6: "Ambiente, Agricultura e Pescas",
    7: "Energia e Clima",
    8: "Transportes e Infraestruturas",
    9: "Habitação, Comunidades e Administração Local",
    10: "Negócios Estrangeiros e Cooperação Internacional",
    11: "Ciência, Tecnologia e Digital"
}

categories = list(CATEGORY_MAPPING.values())

# Government periods mapping
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

# --- Data Loading ---

# Function to reset the number of displayed topics when filters change
def reset_displayed_topics_count():
    st.session_state.num_displayed_topics = INITIAL_DISPLAY_COUNT

@st.cache_data
def load_data(csv_path="data/parliament_data.csv"): # Adjusted default path for pages
    final_csv_path = csv_path
    # Original path adjustment logic from this file (slightly modified for robustness)
    # Simplified: The default path should be correct if script is in streamlit_app/pages/
    # and data is in data/ at the repo root.
    # If streamlit_app.py is in streamlit_app/ and this is in streamlit_app/pages/,
    # then ../data/ is relative to streamlit_app/pages/, meaning it looks for data in streamlit_app/data/.
    # If data is at repo_root/data/, and pages are at repo_root/streamlit_app/pages/,
    # then the path should be ../../data/parliament_data.csv
    # The provided path "../data/parliament_data.csv" implies that 'data' and 'pages' are siblings.
    # Let's assume the user's pathing logic is what they intend for their structure.
    # The key is that the load_data function itself is what needs fixing.

    if not os.path.exists(final_csv_path):
        # Try a common alternative if running from 'pages' subdirectory
        alternative_path = os.path.join("..", csv_path) # e.g. ../../data/parliament_data.csv
        if os.path.exists(alternative_path):
            final_csv_path = alternative_path
        else:
            st.error(f"Error: The data file '{os.path.abspath(final_csv_path)}' (and alternative '{os.path.abspath(alternative_path)}') was not found. "
                     f"Working directory: '{os.getcwd()}'. Please ensure it's generated.")
            return pd.DataFrame()
            
    try:
        raw_df = pd.read_csv(final_csv_path)
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

        for index, row in raw_df.iterrows():
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
                    if isinstance(proposal_category_raw, str):
                        proposal_category_list = json.loads(proposal_category_raw.replace("'", '"'))
                        # print(f"Parsed proposal_category_raw (str): {proposal_category_list}")  # Debugging line
                    elif isinstance(proposal_category_raw, list):
                        proposal_category_list = proposal_category_raw
                        # print(f"Parsed proposal_category_raw (list): {proposal_category_list}")  # Debugging line
                    proposal_category_list = [int(cat) for cat in proposal_category_list if str(cat).isdigit()]
                except (json.JSONDecodeError, ValueError) as e:
                    # print(f"Error parsing proposal_category_raw: {e}")  # Debugging line
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
            is_unanimous_bool = total_active_votes > 0 and (total_favor == total_active_votes or total_contra == total_active_votes)

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
        
        if not all_vote_details: st.info("No vote data could be processed."); return pd.DataFrame()
        df = pd.DataFrame(all_vote_details)
        
        # Ensure session_date column exists and convert to datetime
        if 'session_date' in df.columns:
            df['session_date'] = pd.to_datetime(df['session_date'], errors='coerce')
        else:
            df['session_date'] = pd.NaT

        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink', 'vote_outcome', 'is_unanimous', 
            'issue_type', 'party', 'votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted',
            'authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial',
            'proposal_category_list',
            'proposal_short_title', 'proposal_proposing_party', 'proposal_proposing_party_list', 'proposal_approval_status' # Added new columns
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                elif col == 'proposal_category_list': df[col] = df[col].apply(lambda x: [] if pd.isna(x) else x) # Adjusted for apply
                elif col == 'proposal_short_title': df[col] = 'N/A'
                elif col == 'proposal_proposing_party': df[col] = 'N/A'
                elif col == 'proposal_proposing_party_list': df[col] = df[col].apply(lambda x: [] if pd.isna(x) else x)
                elif col == 'proposal_approval_status': df[col] = pd.NA
                elif col in ['authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
                    df[col] = '' if col != 'authors_json_str' else '[]'
                else: df[col] = 'N/A' if col != 'hyperlink' else ''
        
        for col_fill_na in ['full_title', 'description', 'vote_outcome', 'issue_type', 'party']: df[col_fill_na] = df[col_fill_na].fillna('N/A')
        df['hyperlink'] = df['hyperlink'].fillna('')
        df['is_unanimous'] = df['is_unanimous'].fillna(False).astype(bool)
        df['authors_json_str'] = df['authors_json_str'].fillna('[]')
        df['proposal_category_list'] = df['proposal_category_list'].fillna('').apply(lambda x: [] if x == '' else x) # Ensure it's list
        df['proposal_short_title'] = df['proposal_short_title'].fillna('N/A') # Ensure new column handled
        df['proposal_proposing_party'] = df['proposal_proposing_party'].fillna('N/A') # Ensure new column handled
        df['proposal_approval_status'] = pd.to_numeric(df['proposal_approval_status'], errors='coerce') # Ensure new column handled

        for col_fill_empty_str in ['proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
            df[col_fill_empty_str] = df[col_fill_empty_str].fillna('')
        for col_to_int in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']:
            df[col_to_int] = pd.to_numeric(df[col_to_int], errors='coerce').fillna(0).astype(int)
        df['issue_identifier'] = df['issue_identifier'].astype(str)
        return df
    except FileNotFoundError: st.error(f"Error: Data file '{os.path.abspath(final_csv_path)}' not found."); return pd.DataFrame()
    except pd.errors.EmptyDataError: st.error(f"Error: Data file '{final_csv_path}' is empty."); return pd.DataFrame()
    except Exception as e: st.error(f"Error loading data from '{final_csv_path}': {e}"); return pd.DataFrame()

data_df = load_data() 

st.title("📜 Todas as Votações Parlamentares")
st.markdown("Navegue pela lista de todas as votações registadas. Clique num item para ver os detalhes.")

# Initialize session state for filters
if 'selected_categories' not in st.session_state:
    st.session_state.selected_categories = []
if 'selected_approval_label' not in st.session_state:
    st.session_state.selected_approval_label = "Todos"
if 'selected_proposing_party' not in st.session_state:
    st.session_state.selected_proposing_party = "Todos"
if 'selected_government' not in st.session_state:
    st.session_state.selected_government = "Todos"
if 'last_page' not in st.session_state:
    st.session_state.last_page = 'browse'
if 'num_displayed_topics' not in st.session_state: # Initialize for pagination
    st.session_state.num_displayed_topics = INITIAL_DISPLAY_COUNT

# Check if returning from details page and restore filters
from_page = st.query_params.get("from_page")
if from_page == "browse":
    categories_param = st.query_params.get("categories", "")
    if categories_param:
        st.session_state.selected_categories = [cat for cat in categories_param.split(",") if cat]
    approval_param = st.query_params.get("approval")
    if approval_param:
        st.session_state.selected_approval_label = approval_param
    proposing_party_param = st.query_params.get("proposing_party")
    if proposing_party_param:
        st.session_state.selected_proposing_party = proposing_party_param
    government_param = st.query_params.get("government")
    if government_param:
        st.session_state.selected_government = government_param
    # Clear query params after restoring state
    # Only clear if we are not just re-running due to "load more"
    # However, filter changes will also cause a rerun.
    # The on_change callback handles resetting num_displayed_topics for filter changes.
    if from_page: # Clear only if we explicitly navigated back with params
        st.query_params.clear()

# --- Filters Section ---
# First row of filters
col_category, col_approval_status = st.columns([3, 2])

with col_category:
    st.markdown("#### Filtrar por Categoria:")
    selected_categories = st.multiselect(
        label="Selecione uma ou mais categorias para filtrar as propostas. Apenas propostas que correspondam a TODAS as categorias selecionadas serão exibidas.",
        options=categories,
        default=st.session_state.selected_categories,
        label_visibility="collapsed",
        on_change=reset_displayed_topics_count, # Reset count on filter change
        key="categories_multiselect"
    )

with col_approval_status:
    st.markdown("#### Aprovação:")
    approval_status_options = {
        "Todos": "all",
        "Aprovado": 1.0,
        "Rejeitado": 0.0,
        "Desconhecido": "unknown"
    }
    
    # Find current index, defaulting to 0 if not found
    try:
        current_approval_index = list(approval_status_options.keys()).index(st.session_state.selected_approval_label)
    except ValueError:
        current_approval_index = 0
        st.session_state.selected_approval_label = "Todos"
    
    selected_approval_label = st.selectbox(
        label="Filtro Estado Aprovação",
        options=list(approval_status_options.keys()),
        index=current_approval_index,
        label_visibility="collapsed",
        on_change=reset_displayed_topics_count, # Reset count on filter change
        key="approval_selectbox"
    )
    selected_approval_filter_val = approval_status_options[selected_approval_label]

# Second row of filters
col_proposing_party, col_government = st.columns([2, 2])

with col_proposing_party:
    st.markdown("#### Proponente:")
    available_proposing_parties = []
    if not data_df.empty and 'proposal_proposing_party_list' in data_df.columns:
        # Extract individual parties from all lists
        all_parties = set()
        for party_list in data_df['proposal_proposing_party_list'].dropna():
            if isinstance(party_list, list):
                all_parties.update(party_list)
        available_proposing_parties = sorted([party for party in all_parties if party and party != 'N/A'])
        if not available_proposing_parties:  # Fallback to display strings if lists are empty
            unique_parties = data_df['proposal_proposing_party'].dropna().unique()
            available_proposing_parties = sorted([party for party in unique_parties if party != 'N/A'])
            
    proposing_party_options = ["Todos"] + available_proposing_parties
    
    # Find current index, defaulting to 0 if not found
    try:
        current_proposing_party_index = proposing_party_options.index(st.session_state.selected_proposing_party)
    except ValueError:
        current_proposing_party_index = 0
        st.session_state.selected_proposing_party = "Todos"

    selected_proposing_party = st.selectbox(
        label="Filtro Proponente",
        options=proposing_party_options,
        index=current_proposing_party_index,
        label_visibility="collapsed",
        on_change=reset_displayed_topics_count, # Reset count on filter change
        key="proposing_party_selectbox"
    )

with col_government:
    st.markdown("#### Governo:")
    
    # Find current index, defaulting to 0 if not found
    try:
        current_government_index = list(GOVERNMENT_PERIODS.keys()).index(st.session_state.selected_government)
    except ValueError:
        current_government_index = 0
        st.session_state.selected_government = "Todos"
    
    selected_government = st.selectbox(
        label="Filtro por Período de Governo",
        options=list(GOVERNMENT_PERIODS.keys()),
        index=current_government_index,
        label_visibility="collapsed",
        on_change=reset_displayed_topics_count, # Reset count on filter change
        key="government_selectbox"
    )

st.markdown("---")

if not data_df.empty:
    # Get unique topics based on issue_identifier, keeping the first occurrence for title and outcome
    unique_topics = data_df.drop_duplicates(subset=['issue_identifier'])

    filtered_topics_full = unique_topics.copy() # Keep the full filtered list

    # Apply category filter
    if selected_categories:
        selected_category_ids = [
            cat_id for cat_id, cat_name in CATEGORY_MAPPING.items() 
            if cat_name in selected_categories
        ]
        
        if selected_category_ids:
            filtered_topics_full = filtered_topics_full[
                filtered_topics_full['proposal_category_list'].apply(
                    lambda cat_list: isinstance(cat_list, list) and all(cat_id in cat_list for cat_id in selected_category_ids)
                )
            ]

    # Apply approval status filter
    if selected_approval_label != "Todos":
        if selected_approval_filter_val == "unknown":
            filtered_topics_full = filtered_topics_full[filtered_topics_full['proposal_approval_status'].isna()]
        else: # 0.0 or 1.0
            filtered_topics_full = filtered_topics_full[filtered_topics_full['proposal_approval_status'] == selected_approval_filter_val]
    
    # Apply proposing party filter
    if selected_proposing_party != "Todos":
        filtered_topics_full = filtered_topics_full[
            filtered_topics_full['proposal_proposing_party_list'].apply(
                lambda party_list: isinstance(party_list, list) and selected_proposing_party in party_list
            )
        ]

    # Apply government period filter
    if selected_government != "Todos":
        gov_period = GOVERNMENT_PERIODS[selected_government]
        start_date = gov_period["start"]
        end_date = gov_period["end"]
        
        # Filter by date range
        if start_date and end_date:
            # Both start and end dates defined
            filtered_topics_full = filtered_topics_full[
                (filtered_topics_full['session_date'].notna()) &
                (filtered_topics_full['session_date'] >= start_date) & 
                (filtered_topics_full['session_date'] <= end_date)
            ]
        elif start_date and not end_date:
            # Only start date (current government)
            filtered_topics_full = filtered_topics_full[(filtered_topics_full['session_date'].notna()) & (filtered_topics_full['session_date'] >= start_date)]
        elif end_date and not start_date:
            # Only end date (shouldn't happen with current data, but handle gracefully)
            filtered_topics_full = filtered_topics_full[(filtered_topics_full['session_date'].notna()) & (filtered_topics_full['session_date'] <= end_date)]

    # Sort by date
    if not filtered_topics_full.empty:
        ascending_order = False # Always sort newest first
        filtered_topics_full = filtered_topics_full.sort_values(
            by='session_date', 
            ascending=ascending_order,
            na_position='last'
        )

        num_total_filtered_topics = len(filtered_topics_full)
        # Get the subset of topics to display for this run
        topics_to_display_df = filtered_topics_full.head(st.session_state.num_displayed_topics)

        # Group by date for display
        if not topics_to_display_df.empty:
            grouped_topics = {}
            for _, topic_row in topics_to_display_df.iterrows():
                date_key = topic_row['session_date']
                if pd.isna(date_key):
                    date_str = "Data não disponível"
                else:
                    date_str = date_key.strftime("%d/%m/%Y")
                
                if date_str not in grouped_topics:
                    grouped_topics[date_str] = []
                grouped_topics[date_str].append(topic_row)

            # Display grouped topics
            for date_str, topics_for_date in grouped_topics.items():
                st.markdown(f"### {date_str}")
                
                for topic_row in topics_for_date:
                    with st.container(border=True):
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            # --- Resumo da Proposta ---
                            proposing_party_text = ""
                            if pd.notna(topic_row.get('proposal_proposing_party')) and topic_row['proposal_proposing_party'] != 'N/A' and str(topic_row['proposal_proposing_party']).lower() != 'nan':
                                proposing_party_text = topic_row['proposal_proposing_party']

                            # Date is already part of the group header (date_str), 
                            # but can be repeated if desired or for context if date_str is "Data não disponível"
                            current_date_str_display = date_str # Use the group date string
                            if date_str == "Data não disponível" and pd.notna(topic_row.get('session_date')):
                                # Fallback if main date_str is unavailable but row has it
                                current_date_str_display = topic_row['session_date'].strftime("%d/%m/%Y")
                            
                            if current_date_str_display != "Data não disponível":
                                if proposing_party_text:
                                    st.markdown(f"**{proposing_party_text} - {current_date_str_display}**")
                                else:
                                    st.markdown(f"**{current_date_str_display}**")
                            else:
                                if proposing_party_text:
                                    st.markdown(f"**{proposing_party_text}**")
                            
                            # Display project identifier as main title
                            if pd.notna(topic_row['proposal_short_title']) and topic_row['proposal_short_title'] != 'N/A':
                                st.markdown(f"#### {topic_row['proposal_short_title']}")
                            else:
                                st.markdown(f"#### {topic_row['issue_identifier']}")
                            
                            # Display full title as descriptive text
                            st.markdown(f"*{topic_row['full_title']}*")

                            vote_outcome = topic_row.get('vote_outcome', 'N/A')
                            if vote_outcome == "Aprovado":
                                st.markdown('<span style="font-size: 1.2em;">✅ **Aprovado**</span>', unsafe_allow_html=True)
                            elif vote_outcome == "Rejeitado":
                                st.markdown('<span style="font-size: 1.2em;">❌ **Rejeitado**</span>', unsafe_allow_html=True)
                            else:
                                st.markdown(f'<span style="font-size: 1.2em;">❓ **{vote_outcome}**</span>', unsafe_allow_html=True)
                            # --- End Resumo da Proposta ---
                            
                        with col2:
                            if st.button(f"Ver detalhes 🗳️", key=f"btn_{topic_row['issue_identifier']}", use_container_width=True):
                                st.session_state.last_page = 'browse'
                                st.session_state.selected_issue_identifier = str(topic_row['issue_identifier'])
                                # Set query parameters with current filter state
                                st.query_params.update({
                                    "issue_id": str(topic_row['issue_identifier']),
                                    "from_page": "browse",
                                    "categories": ",".join(selected_categories),
                                    "approval": selected_approval_label,
                                    "proposing_party": selected_proposing_party,
                                    "government": selected_government
                                })
                                st.switch_page("pages/2_Topic_Details.py")
                
                        # Expander for other descriptions
                        with st.expander("Mais detalhes da proposta"):
                            if pd.notna(topic_row['description']) and topic_row['description'].strip() and topic_row['description'] != 'Descrição não disponível.':
                                st.markdown(f"**Descrição Geral:**")
                                st.markdown(f"_{topic_row['description']}_")
                                st.markdown("---") # Separator if other details follow

                            if pd.notna(topic_row['proposal_summary_analysis']) and topic_row['proposal_summary_analysis'].strip():
                                st.markdown("**Análise:**")
                                st.markdown(topic_row['proposal_summary_analysis'])
                            if pd.notna(topic_row['proposal_summary_fiscal_impact']) and topic_row['proposal_summary_fiscal_impact'].strip():
                                st.markdown("**Impacto Fiscal:**")
                                st.markdown(topic_row['proposal_summary_fiscal_impact'])
                            if pd.notna(topic_row['proposal_summary_colloquial']) and topic_row['proposal_summary_colloquial'].strip():
                                st.markdown("🗣️ **Sem precisar de dicionário**")
                                st.markdown(topic_row['proposal_summary_colloquial'])
                            if not ((pd.notna(topic_row['proposal_summary_analysis']) and topic_row['proposal_summary_analysis'].strip()) or 
                                    (pd.notna(topic_row['proposal_summary_fiscal_impact']) and topic_row['proposal_summary_fiscal_impact'].strip()) or 
                                    (pd.notna(topic_row['proposal_summary_colloquial']) and topic_row['proposal_summary_colloquial'].strip())):
                                st.markdown("Não há detalhes adicionais disponíveis.")
            
            # --- "Load More" Button ---
            if st.session_state.num_displayed_topics < num_total_filtered_topics:
                if st.button("Carregar mais propostas", key="load_more_browse_topics"):
                    st.session_state.num_displayed_topics += LOAD_MORE_COUNT
                    st.rerun()
            elif num_total_filtered_topics > 0 : # All items are displayed
                 st.markdown(f"Mostrando todas as {num_total_filtered_topics} propostas encontradas.")


        else: # This case means topics_to_display_df is empty
            st.info("Não foram encontradas votações para os filtros selecionados.")
    else: # This case means filtered_topics_full is empty
        st.info("Não foram encontradas votações para os filtros selecionados.")
else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro.")


st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
st.sidebar.page_link("pages/1_Browse_Topics.py", label="Todas as Votações", icon="📜")