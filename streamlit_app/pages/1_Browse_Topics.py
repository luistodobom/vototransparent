import streamlit as st
import pandas as pd
import os
import json
import re # For extracting BID

# --- Page Configuration ---
st.set_page_config(
    page_title="Todas as Votações - VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide"
)

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

            # Parse proposal_category as list of integers
            proposal_category_raw = row.get('proposal_category', '[]')
            proposal_category_list = []
            if pd.notna(proposal_category_raw) and str(proposal_category_raw).strip():
                try:
                    if isinstance(proposal_category_raw, str):
                        proposal_category_list = json.loads(proposal_category_raw.replace("'", '"'))
                    elif isinstance(proposal_category_raw, list):
                        proposal_category_list = proposal_category_raw
                    proposal_category_list = [int(cat) for cat in proposal_category_list if str(cat).isdigit()]
                except (json.JSONDecodeError, ValueError):
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
            
            vote_outcome_str = "Dados de votação não disponíveis"
            is_unanimous_bool = False
            if proposal_party_votes_list:
                if current_proposal_overall_favor > 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                    vote_outcome_str = "Aprovado por unanimidade"; is_unanimous_bool = True
                elif current_proposal_overall_against > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_abstention == 0:
                    vote_outcome_str = "Rejeitado por unanimidade"; is_unanimous_bool = True
                elif current_proposal_overall_favor > current_proposal_overall_against: vote_outcome_str = "Aprovado"
                elif current_proposal_overall_against > current_proposal_overall_favor: vote_outcome_str = "Rejeitado"
                elif current_proposal_overall_favor == current_proposal_overall_against and current_proposal_overall_favor > 0: vote_outcome_str = "Empate"
                elif current_proposal_overall_abstention > 0 and current_proposal_overall_favor == 0 and current_proposal_overall_against == 0:
                    all_abstained = all(p_vote['votes_favor'] == 0 and p_vote['votes_against'] == 0 for p_vote in proposal_party_votes_list)
                    if all_abstained: vote_outcome_str = "Abstenção Geral"; is_unanimous_bool = True
                    else: vote_outcome_str = "Resultado misto"
                else:
                    if current_proposal_overall_favor == 0 and current_proposal_overall_against == 0 and current_proposal_overall_abstention == 0:
                        total_non_voters = sum(pvd.get('votes_not_voted',0) for pvd in proposal_party_votes_list)
                        if total_non_voters > 0 and not any(pvd.get('votes_favor',0) > 0 or pvd.get('votes_against',0) > 0 or pvd.get('votes_abstention',0) > 0 for pvd in proposal_party_votes_list):
                             vote_outcome_str = "Ausência de votação registada"
                        else:
                             vote_outcome_str = "Sem votos expressos (Favor, Contra, Abstenção)"
                    else: vote_outcome_str = "Resultado misto"
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
                    'proposal_category_list': proposal_category_list,
                })
        
        if not all_vote_details: st.info("No vote data could be processed."); return pd.DataFrame()
        df = pd.DataFrame(all_vote_details)
        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink', 'vote_outcome', 'is_unanimous', 
            'issue_type', 'party', 'votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted',
            'authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial',
            'proposal_category_list'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'votes_not_voted']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                elif col == 'proposal_category_list': df[col] = []
                elif col in ['authors_json_str', 'proposal_summary_analysis', 'proposal_summary_fiscal_impact', 'proposal_summary_colloquial']:
                    df[col] = '' if col != 'authors_json_str' else '[]'
                else: df[col] = 'N/A' if col != 'hyperlink' else ''
        
        for col_fill_na in ['full_title', 'description', 'vote_outcome', 'issue_type', 'party']: df[col_fill_na] = df[col_fill_na].fillna('N/A')
        df['hyperlink'] = df['hyperlink'].fillna('')
        df['is_unanimous'] = df['is_unanimous'].fillna(False).astype(bool)
        df['authors_json_str'] = df['authors_json_str'].fillna('[]')
        df['proposal_category_list'] = df['proposal_category_list'].fillna('').apply(lambda x: [] if x == '' else x)
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

# --- Category Filter ---
# Category mapping from integers to names
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

st.markdown("#### Filtrar por Categoria:")
selected_categories = st.multiselect(
    label="Selecione uma ou mais categorias para filtrar as propostas. Apenas propostas que correspondam a TODAS as categorias selecionadas serão exibidas.",
    options=categories,
    label_visibility="collapsed" # More compact
)
st.markdown("---")

if not data_df.empty:
    # Get unique topics based on issue_identifier, keeping the first occurrence for title and outcome
    unique_topics = data_df.drop_duplicates(subset=['issue_identifier'])

    filtered_topics = unique_topics.copy() # Start with all unique topics

    if selected_categories:
        # Convert selected category names to integers
        selected_category_ids = [
            cat_id for cat_id, cat_name in CATEGORY_MAPPING.items() 
            if cat_name in selected_categories
        ]
        
        # Filter topics that contain ALL selected categories
        if selected_category_ids:
            filtered_topics = filtered_topics[
                filtered_topics['proposal_category_list'].apply(
                    lambda cat_list: all(cat_id in cat_list for cat_id in selected_category_ids)
                )
            ]
    
    if not filtered_topics.empty:
        # Optional: sort by title or identifier
        # filtered_topics = filtered_topics.sort_values(by='full_title') 

        for _, topic_row in filtered_topics.iterrows():
            with st.container(border=True):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"#### {topic_row['full_title']}")
                    if pd.notna(topic_row['description']):
                        st.markdown(f"_{topic_row['description']}_") # Display general description
                    if pd.notna(topic_row['issue_identifier']):
                        st.caption(f"Identificador: {topic_row['issue_identifier']}")
                    if pd.notna(topic_row['issue_type']):
                        st.caption(f"Tipo: {topic_row['issue_type']}")
                with col2:
                    if st.button(f"Ver detalhes 🗳️", key=f"btn_{topic_row['issue_identifier']}", use_container_width=True):
                        st.session_state.selected_issue_identifier = topic_row['issue_identifier']
                        st.switch_page("pages/2_Topic_Details.py")
                st.markdown(f"**Resultado da Votação:** {topic_row.get('vote_outcome', 'N/A')}")
                # Expander for other descriptions
                with st.expander("Mais detalhes da proposta"):
                    if pd.notna(topic_row['proposal_summary_analysis']) and topic_row['proposal_summary_analysis'].strip():
                        st.markdown("**Análise:**")
                        st.markdown(topic_row['proposal_summary_analysis'])
                    if pd.notna(topic_row['proposal_summary_fiscal_impact']) and topic_row['proposal_summary_fiscal_impact'].strip():
                        st.markdown("**Impacto Fiscal:**")
                        st.markdown(topic_row['proposal_summary_fiscal_impact'])
                    if pd.notna(topic_row['proposal_summary_colloquial']) and topic_row['proposal_summary_colloquial'].strip():
                        st.markdown("**Versão Coloquial:**")
                        st.markdown(topic_row['proposal_summary_colloquial'])
                    if not ((pd.notna(topic_row['proposal_summary_analysis']) and topic_row['proposal_summary_analysis'].strip()) or 
                            (pd.notna(topic_row['proposal_summary_fiscal_impact']) and topic_row['proposal_summary_fiscal_impact'].strip()) or 
                            (pd.notna(topic_row['proposal_summary_colloquial']) and topic_row['proposal_summary_colloquial'].strip())):
                        st.markdown("Não há detalhes adicionais disponíveis.")

    else:
        st.info("Não foram encontradas votações para os filtros selecionados." if selected_categories else "Não foram encontradas votações para listar.")
else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro.")

st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")
# Removed the duplicate sidebar link to "Todas as Votações" as we are on this page.