import streamlit as st
import pandas as pd
import os
import json
import re # For extracting BID
import unicodedata

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
</style>
""", unsafe_allow_html=True)


# --- Data Loading ---
@st.cache_data
def load_data(csv_path="data/parliament_data.csv"): # Adjusted default path
    final_csv_path = csv_path
    # Note: Path adjustment logic for 'pages' is not strictly needed here if streamlit_app.py is not in 'pages'
    # However, to keep the function identical, it can remain.
    # Or, simplify it for this specific file if it's always run from a known location relative to data.
    # Assuming streamlit_app.py is in 'streamlit_app/' and data is in 'data/', then '../data/' is correct.

    if not os.path.exists(final_csv_path):
        st.error(f"Error: The data file '{os.path.abspath(final_csv_path)}' was not found. "
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
            # Fallback for other link types if necessary, or return None
            # e.g. DetalheProjetoVoto.aspx?BID=178093
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

            voting_breakdown_json = row.get('voting_details_json')
            current_proposal_overall_favor = 0
            current_proposal_overall_against = 0
            current_proposal_overall_abstention = 0
            
            # Store party votes for this proposal to calculate overall outcome later
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
                            'party': party_name,
                            'votes_favor': favor,
                            'votes_against': against,
                            'votes_abstention': abstention,
                            'votes_not_voted': not_voted,
                        })
            
            # Determine overall vote outcome and unanimity for the proposal
            vote_outcome_str = "Dados de votação não disponíveis"
            is_unanimous_bool = False

            if proposal_party_votes_list: # If we have party votes
                if current_proposal_overall_favor > 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                    vote_outcome_str = "Aprovado por unanimidade"
                    is_unanimous_bool = True
                elif current_proposal_overall_against > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_abstention == 0:
                    vote_outcome_str = "Rejeitado por unanimidade"
                    is_unanimous_bool = True
                elif current_proposal_overall_favor > current_proposal_overall_against:
                    vote_outcome_str = "Aprovado"
                elif current_proposal_overall_against > current_proposal_overall_favor:
                    vote_outcome_str = "Rejeitado"
                elif current_proposal_overall_favor == current_proposal_overall_against and current_proposal_overall_favor > 0:
                    vote_outcome_str = "Empate"
                elif current_proposal_overall_abstention > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_against == 0:
                     # Check if all voting was abstention
                    all_abstained = True
                    for p_vote in proposal_party_votes_list:
                        if p_vote['votes_favor'] > 0 or p_vote['votes_against'] > 0:
                            all_abstained = False
                            break
                    if all_abstained:
                        vote_outcome_str = "Abstenção Geral"
                        is_unanimous_bool = True # Unanimous abstention among those who voted (or only abstentions)
                    else:
                        vote_outcome_str = "Resultado misto" # Should not happen if previous checks are exhaustive
                else: # No clear majority, or only abstentions, or no votes
                    if current_proposal_overall_favor == 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                        # Check if there were non-voters
                        total_non_voters = sum(pvd.get('votes_not_voted',0) for pvd in proposal_party_votes_list)
                        if total_non_voters > 0 and not any(pvd.get('votes_favor',0) > 0 or pvd.get('votes_against',0) > 0 or pvd.get('votes_abstention',0) > 0 for pvd in proposal_party_votes_list):
                             vote_outcome_str = "Ausência de votação registada"
                        else:
                             vote_outcome_str = "Sem votos expressos (Favor, Contra, Abstenção)"

                    else: # Some votes but not fitting simple categories
                        vote_outcome_str = "Resultado misto"
            elif not valid_breakdown_found and pd.notna(voting_breakdown_json) and voting_breakdown_json.strip():
                 vote_outcome_str = "Dados de votação malformados"


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
                        'proposal_category_list': proposal_category_list,
                    })
            else: # No party breakdown, create a single row for the proposal
                all_vote_details.append({
                    'issue_identifier': issue_id_str, 'full_title': title, 'description': description_text,
                    'hyperlink': hyperlink_url, 'vote_outcome': vote_outcome_str, 'is_unanimous': is_unanimous_bool,
                    'issue_type': issue_type, 'party': 'N/A',
                    'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0, 'votes_not_voted': 0,
                    'authors_json_str': authors_json_str,
                    'proposal_summary_analysis': summary_analysis,
                    'proposal_summary_fiscal_impact': summary_fiscal,
                    'proposal_summary_colloquial': summary_colloquial,
                    'proposal_category_list': proposal_category_list,
                })
        
        if not all_vote_details:
            st.info("No vote data could be processed from the CSV file. It might be empty or contain no valid vote entries.")
            return pd.DataFrame()

        df = pd.DataFrame(all_vote_details)

        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink',
            'vote_outcome', 'is_unanimous', 'issue_type', 'party',
            'votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted',
            'authors_json_str', 'proposal_summary_analysis', 
            'proposal_summary_fiscal_impact', 'proposal_summary_colloquial',
            'proposal_category_list'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']:
                    df[col] = 0
                elif col == 'is_unanimous':
                    df[col] = False
                elif col == 'proposal_category_list':
                    df[col] = df[col].apply(lambda x: [] if pd.isna(x) else x)
                elif col in ['authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
                    df[col] = '' if col != 'authors_json_str' else '[]'
                else: 
                    df[col] = 'N/A' if col != 'hyperlink' else ''
        
        for col_fill_na in ['full_title', 'description', 'vote_outcome', 'issue_type', 'party']:
            df[col_fill_na] = df[col_fill_na].fillna('N/A')
        df['hyperlink'] = df['hyperlink'].fillna('')
        df['is_unanimous'] = df['is_unanimous'].fillna(False).astype(bool)
        df['authors_json_str'] = df['authors_json_str'].fillna('[]')
        for col_fill_empty_str in ['proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
            df[col_fill_empty_str] = df[col_fill_empty_str].fillna('')

        for col_to_int in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']:
            df[col_to_int] = pd.to_numeric(df[col_to_int], errors='coerce').fillna(0).astype(int)
        
        # Ensure issue_identifier is string
        df['issue_identifier'] = df['issue_identifier'].astype(str)

        return df

    except FileNotFoundError: 
        st.error(f"Error: The data file '{os.path.abspath(final_csv_path)}' was not found after checks.")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        st.error(f"Error: The data file '{final_csv_path}' is empty.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred while loading data from '{final_csv_path}': {e}")
        # import traceback
        # st.error(traceback.format_exc()) # For more detailed error during development
        return pd.DataFrame()


data_df = load_data()

# --- Helper function to normalize text ---
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
    # --- Search Functionality ---
    # st.header(\"🔍 Pesquisar Votações\") # Original header, can be removed for cleaner look
    search_query = st.text_input(
        "", # Label removed, placeholder is more prominent
        placeholder="Pesquisar por palavra-chave...", # Simpler placeholder
        # "Procure por título, número da iniciativa ou palavras-chave na descrição:", # Original label
        # placeholder="Ex: Orçamento do Estado, PL/123/XVI/1, habitação" # Original placeholder
    )

    if search_query:
        # Perform a case-insensitive search across relevant fields
        # Consolidate data to one row per issue for search results
        # Ensure 'issue_identifier' is unique for drop_duplicates
        search_df_unique_issues = data_df.drop_duplicates(subset=['issue_identifier']).copy()

        # Normalize search query
        normalized_search_query = normalize_text(search_query)

        # Ensure 'description' and 'full_title' are strings for searching
        search_df_unique_issues['description'] = search_df_unique_issues['description'].astype(str)
        search_df_unique_issues['full_title'] = search_df_unique_issues['full_title'].astype(str)
        # 'issue_identifier' is already string from load_data

        # Apply normalization to searchable columns
        # Create temporary columns for normalized search
        search_df_unique_issues['normalized_full_title'] = search_df_unique_issues['full_title'].apply(normalize_text)
        search_df_unique_issues['normalized_description'] = search_df_unique_issues['description'].apply(normalize_text)
        search_df_unique_issues['normalized_issue_identifier'] = search_df_unique_issues['issue_identifier'].astype(str).apply(normalize_text)


        results = search_df_unique_issues[
            search_df_unique_issues['normalized_full_title'].str.contains(normalized_search_query, case=False, na=False) |
            search_df_unique_issues['normalized_description'].str.contains(normalized_search_query, case=False, na=False) |
            search_df_unique_issues['normalized_issue_identifier'].str.contains(normalized_search_query, case=False, na=False)
        ]

        if not results.empty:
            st.markdown(f"**Resultados da pesquisa para \"{search_query}\":**") # Simpler subheader
            for _, row in results.iterrows():
                # Use a simpler container, the CSS will style it
                with st.container(): # Removed border=True, CSS handles it
                    col1, col2 = st.columns([3, 1]) # Column for title and button
                    with col1:
                        st.markdown(f"**{row['full_title']}**")
                        if pd.notna(row['description']):
                            st.markdown(f"_{row['description']}_") # Display general description
                        if pd.notna(row['issue_identifier']):
                            st.caption(f"ID: {row['issue_identifier']}")
                        if pd.notna(row['vote_outcome']):
                             st.caption(f"Resultado: {row['vote_outcome']}")

                    with col2:
                         # Button to navigate, styled by CSS
                        if st.button(f"Ver detalhes", key=f"search_{row['issue_identifier']}", use_container_width=True):
                            st.session_state.selected_issue_identifier = row['issue_identifier']
                            st.switch_page("pages/2_Topic_Details.py")

                    # Expander for other descriptions
                    with st.expander("Mais detalhes da proposta"):
                        if pd.notna(row['proposal_summary_analysis']) and row['proposal_summary_analysis'].strip():
                            st.markdown("**Análise:**")
                            st.markdown(row['proposal_summary_analysis'])
                        if pd.notna(row['proposal_summary_fiscal_impact']) and row['proposal_summary_fiscal_impact'].strip():
                            st.markdown("**Impacto Fiscal:**")
                            st.markdown(row['proposal_summary_fiscal_impact'])
                        if pd.notna(row['proposal_summary_colloquial']) and row['proposal_summary_colloquial'].strip():
                            st.markdown("**Versão Coloquial:**")
                            st.markdown(row['proposal_summary_colloquial'])
                        if not ((pd.notna(row['proposal_summary_analysis']) and row['proposal_summary_analysis'].strip()) or \
                                (pd.notna(row['proposal_summary_fiscal_impact']) and row['proposal_summary_fiscal_impact'].strip()) or \
                                (pd.notna(row['proposal_summary_colloquial']) and row['proposal_summary_colloquial'].strip())):
                            st.markdown("Não há detalhes adicionais disponíveis.")
        else:
            st.info(f"Nenhuma votação encontrada para \"{search_query}\".") # Simpler message
    
    # st.markdown(\"---\") # Remove divider
    # st.header(\"📖 Navegar por Todas as Votações\") # Original header
    # st.markdown(\"Veja uma lista completa de todas as votações processadas.\") # Original markdown
    
    # Centered "Browse All Votes" link/button
    st.markdown("<br>", unsafe_allow_html=True) # Add some space
    cols_browse = st.columns([1,2,1]) # Use columns to center the button/link
    with cols_browse[1]:
        if st.button("Navegar por Todas as Votações", use_container_width=True, key="browse_all_main"):
             st.switch_page("pages/1_Browse_Topics.py")
    # st.page_link(\"pages/1_Browse_Topics.py\", label=\"Ver Todos os Tópicos de Votação\", icon=\"📜\") # Original page_link

else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro acima.")

# --- Footer ---
st.markdown("<div class='footer'>Desenvolvido com ❤️ por MEIC-Top<br>Dados extraídos de documentos oficiais da Assembleia da República e processados.</div>", unsafe_allow_html=True)