import streamlit as st
import pandas as pd
import os
import json
import re # For extracting BID
from datetime import datetime

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

            voting_breakdown_json = row.get('voting_details_json')
            current_proposal_overall_favor = 0
            current_proposal_overall_against = 0
            current_proposal_overall_abstention = 0
            proposal_party_votes_list = []
            parsed_voting_breakdown = {}
            valid_breakdown_found = False

            if pd.notna(voting_breakdown_json) and isinstance(voting_breakdown_json, str) and voting_breakdown_json.strip():
                try:
                    parsed_voting_breakdown = json.loads(voting_breakdown_json)
                    if isinstance(parsed_voting_breakdown, dict) and parsed_voting_breakdown:
                        valid_breakdown_found = True
                except json.JSONDecodeError:
                    parsed_voting_breakdown = {}

            if valid_breakdown_found:
                for party_name, party_votes_data in parsed_voting_breakdown.items():
                    if isinstance(party_votes_data, dict):
                        raw_favor_val = party_votes_data.get('Favor', party_votes_data.get('votes_favor', 0))
                        favor_numeric = pd.to_numeric(raw_favor_val, errors='coerce')
                        favor = 0 if pd.isna(favor_numeric) else int(favor_numeric)

                        raw_against_val = party_votes_data.get('Contra', party_votes_data.get('votes_against', 0))
                        against_numeric = pd.to_numeric(raw_against_val, errors='coerce')
                        against = 0 if pd.isna(against_numeric) else int(against_numeric)

                        raw_abstention_val = party_votes_data.get('Abstenção', party_votes_data.get('Abstencao', party_votes_data.get('votes_abstention', 0)))
                        abstention_numeric = pd.to_numeric(raw_abstention_val, errors='coerce')
                        abstention = 0 if pd.isna(abstention_numeric) else int(abstention_numeric)
                        
                        raw_not_voted_val = party_votes_data.get('Não Votaram', party_votes_data.get('Nao Votaram', 0))
                        not_voted_numeric = pd.to_numeric(raw_not_voted_val, errors='coerce')
                        not_voted = 0 if pd.isna(not_voted_numeric) else int(not_voted_numeric)
                        
                        current_proposal_overall_favor += favor
                        current_proposal_overall_against += against
                        current_proposal_overall_abstention += abstention
                        proposal_party_votes_list.append({
                            'party': party_name, 'votes_favor': favor, 'votes_against': against,
                            'votes_abstention': abstention, 'votes_not_voted': not_voted,
                        })
            
            # Determine overall vote outcome and unanimity for the proposal
            is_unanimous_bool = False # Reset for each proposal

            if proposal_party_votes_list:
                if current_proposal_overall_favor > 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                    is_unanimous_bool = True
                elif current_proposal_overall_against > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_abstention == 0:
                    is_unanimous_bool = True
                elif current_proposal_overall_abstention > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_against == 0:
                    all_abstained = all(p_vote['votes_favor'] == 0 and p_vote['votes_against'] == 0 for p_vote in proposal_party_votes_list)
                    if all_abstained:
                        is_unanimous_bool = True
            
            # New logic for vote_outcome_str based on proposal_approval_status
            vote_outcome_str = "Resultado Desconhecido" # Default
            if pd.notna(proposal_approval_status_raw):
                try:
                    status_as_int = int(proposal_approval_status_raw)
                    if status_as_int == 1:
                        vote_outcome_str = "Aprovado"
                    elif status_as_int == 0:
                        vote_outcome_str = "Rejeitado"
                except ValueError:
                    pass # Remains "Resultado Desconhecido"

            if proposal_party_votes_list:
                for p_vote in proposal_party_votes_list:
                    all_vote_details.append({
                        'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                        'hyperlink': hyperlink_url, 'vote_outcome': vote_outcome_str, 'is_unanimous': is_unanimous_bool,
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
                        'proposal_proposing_party': proposal_proposing_party_val,
                        'proposal_approval_status': proposal_approval_status_raw,
                    })
            else:
                all_vote_details.append({
                    'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                    'hyperlink': hyperlink_url, 'vote_outcome': vote_outcome_str, 'is_unanimous': is_unanimous_bool,
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
                    'proposal_proposing_party': proposal_proposing_party_val,
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
            'proposal_short_title', 'proposal_proposing_party', 'proposal_approval_status' # Added new columns
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                elif col == 'proposal_category_list': df[col] = df[col].apply(lambda x: [] if pd.isna(x) else x) # Adjusted for apply
                elif col == 'proposal_short_title': df[col] = 'N/A'
                elif col == 'proposal_proposing_party': df[col] = 'N/A'
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
        label_visibility="collapsed"
    )
    st.session_state.selected_categories = selected_categories

with col_approval_status:
    st.markdown("#### Aprovação:")
    approval_status_options = {
        "Todos": "all",
        "Aprovado": 1.0,
        "Rejeitado": 0.0,
        "Desconhecido": "unknown"
    }
    selected_approval_label = st.selectbox(
        label="Filtro Estado Aprovação",
        options=list(approval_status_options.keys()),
        index=list(approval_status_options.keys()).index(st.session_state.selected_approval_label),
        label_visibility="collapsed"
    )
    st.session_state.selected_approval_label = selected_approval_label
    selected_approval_filter_val = approval_status_options[selected_approval_label]

# Second row of filters
col_proposing_party, col_government = st.columns([2, 2])

with col_proposing_party:
    st.markdown("#### Proponente:")
    available_proposing_parties = []
    if not data_df.empty and 'proposal_proposing_party' in data_df.columns:
        available_proposing_parties = sorted(data_df['proposal_proposing_party'].dropna().unique())
    
    proposing_party_options = ["Todos"] + available_proposing_parties
    selected_proposing_party = st.selectbox(
        label="Filtro Proponente",
        options=proposing_party_options,
        index=0 if st.session_state.selected_proposing_party not in proposing_party_options else proposing_party_options.index(st.session_state.selected_proposing_party),
        label_visibility="collapsed"
    )
    st.session_state.selected_proposing_party = selected_proposing_party

with col_government:
    st.markdown("#### Governo:")
    selected_government = st.selectbox(
        label="Filtro por Período de Governo",
        options=list(GOVERNMENT_PERIODS.keys()),
        index=list(GOVERNMENT_PERIODS.keys()).index(st.session_state.selected_government),
        label_visibility="collapsed"
    )
    st.session_state.selected_government = selected_government

st.markdown("---")

if not data_df.empty:
    # Get unique topics based on issue_identifier, keeping the first occurrence for title and outcome
    unique_topics = data_df.drop_duplicates(subset=['issue_identifier'])

    filtered_topics = unique_topics.copy()

    # Apply category filter
    if selected_categories:
        selected_category_ids = [
            cat_id for cat_id, cat_name in CATEGORY_MAPPING.items() 
            if cat_name in selected_categories
        ]
        
        if selected_category_ids:
            filtered_topics = filtered_topics[
                filtered_topics['proposal_category_list'].apply(
                    lambda cat_list: all(cat_id in cat_list for cat_id in selected_category_ids)
                )
            ]

    # Apply approval status filter
    if selected_approval_label != "Todos":
        if selected_approval_filter_val == "unknown":
            filtered_topics = filtered_topics[filtered_topics['proposal_approval_status'].isna()]
        else: # 0.0 or 1.0
            filtered_topics = filtered_topics[filtered_topics['proposal_approval_status'] == selected_approval_filter_val]
    
    # Apply proposing party filter
    if selected_proposing_party != "Todos":
        filtered_topics = filtered_topics[filtered_topics['proposal_proposing_party'] == selected_proposing_party]

    # Apply government period filter
    if selected_government != "Todos":
        gov_period = GOVERNMENT_PERIODS[selected_government]
        start_date = gov_period["start"]
        end_date = gov_period["end"]
        
        # Filter by date range
        if start_date and end_date:
            # Both start and end dates defined
            filtered_topics = filtered_topics[
                (filtered_topics['session_date'] >= start_date) & 
                (filtered_topics['session_date'] <= end_date)
            ]
        elif start_date and not end_date:
            # Only start date (current government)
            filtered_topics = filtered_topics[filtered_topics['session_date'] >= start_date]
        elif end_date and not start_date:
            # Only end date (shouldn't happen with current data, but handle gracefully)
            filtered_topics = filtered_topics[filtered_topics['session_date'] <= end_date]

    # Sort by date
    if not filtered_topics.empty:
        # ascending_order = sort_order == "Mais antigo primeiro" # Old sorting logic
        ascending_order = False # Always sort newest first
        filtered_topics = filtered_topics.sort_values(
            by='session_date', 
            ascending=ascending_order,
            na_position='last'
        )

        # Group by date for display
        if not filtered_topics.empty:
            # Group topics by session_date
            grouped_topics = {}
            for _, topic_row in filtered_topics.iterrows():
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
                            
                            st.markdown(f"#### {topic_row['full_title']}")
                            if pd.notna(topic_row['proposal_short_title']) and topic_row['proposal_short_title'] != 'N/A':
                                st.markdown(f"*{topic_row['proposal_short_title']}*")

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
        else:
            st.info("Não foram encontradas votações para os filtros selecionados.")
    else:
        st.info("Não foram encontradas votações para os filtros selecionados.")
else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro.")


st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
st.sidebar.page_link("pages/1_Browse_Topics.py", label="Todas as Votações", icon="📜")