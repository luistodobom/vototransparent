import streamlit as st
import pandas as pd
import os
import json
import re # For extracting BID

# --- Page Configuration ---
st.set_page_config(
    page_title="VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Data Loading ---
@st.cache_data
def load_data(csv_path="../data/parliament_data.csv"): # Adjusted default path
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
            'proposal_summary_fiscal_impact', 'proposal_summary_colloquial'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']:
                    df[col] = 0
                elif col == 'is_unanimous':
                    df[col] = False
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

# --- Homepage ---
st.title("🇵🇹 VotoTransparente: O Seu Guia para as Votações Parlamentares")
st.markdown("Explore como os partidos políticos votam na Assembleia da República Portuguesa.")
st.markdown("---")

if not data_df.empty:
    # --- Search Functionality ---
    st.header("🔍 Pesquisar Votações")
    search_query = st.text_input(
        "Procure por título, número da iniciativa ou palavras-chave na descrição:",
        placeholder="Ex: Orçamento do Estado, PL/123/XVI/1, habitação"
    )

    if search_query:
        # Perform a case-insensitive search across relevant fields
        # Consolidate data to one row per issue for search results
        # Ensure 'issue_identifier' is unique for drop_duplicates
        search_df_unique_issues = data_df.drop_duplicates(subset=['issue_identifier']).copy()
        
        # Ensure 'description' and 'full_title' are strings for searching
        search_df_unique_issues['description'] = search_df_unique_issues['description'].astype(str)
        search_df_unique_issues['full_title'] = search_df_unique_issues['full_title'].astype(str)
        # 'issue_identifier' is already string from load_data
        # search_df_unique_issues['issue_identifier_str'] = search_df_unique_issues['issue_identifier'].astype(str) # Redundant if already string

        results = search_df_unique_issues[
            search_df_unique_issues['full_title'].str.contains(search_query, case=False, na=False) |
            search_df_unique_issues['description'].str.contains(search_query, case=False, na=False) |
            search_df_unique_issues['issue_identifier'].str.contains(search_query, case=False, na=False) # Use 'issue_identifier' directly
        ]

        if not results.empty:
            st.subheader(f"Resultados da pesquisa para \"{search_query}\":")
            for _, row in results.iterrows():
                with st.container(border=True):
                    st.markdown(f"#### {row['full_title']}")
                    if pd.notna(row['issue_identifier']):
                        st.caption(f"Identificador: {row['issue_identifier']}")
                    if pd.notna(row['vote_outcome']):
                        st.caption(f"Resultado: {row['vote_outcome']}")

                    # Use button with session state for navigation
                    if st.button(f"Ver detalhes da votação 🗳️", key=f"search_{row['issue_identifier']}", use_container_width=True):
                        st.session_state.selected_issue_identifier = row['issue_identifier']
                        st.switch_page("pages/2_Topic_Details.py")
        else:
            st.info(f"Nenhuma votação encontrada para \"{search_query}\". Tente termos diferentes ou navegue por todos os tópicos.")
    
    st.markdown("---")
    st.header("📖 Navegar por Todas as Votações")
    st.markdown("Veja uma lista completa de todas as votações processadas.")
    st.page_link("pages/1_Browse_Topics.py", label="Ver Todos os Tópicos de Votação", icon="📜")

else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro acima.")

st.markdown("---")
st.markdown("Desenvolvido com ❤️ por [Luís Trindade Bento]") # Placeholder for actual name/org
st.markdown("Dados extraídos de documentos oficiais da Assembleia da República e processados.")