import streamlit as st
import pandas as pd
import os
import json  # Added for JSON processing

# --- Page Configuration ---
st.set_page_config(
    page_title="VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Data Loading ---
@st.cache_data
def load_data(json_path="../data/portuguese_parliament_votes_json.json"): # Adjusted path for pages folder
    if not os.path.exists(json_path):
        st.error(f"Error: The data file '{os.path.abspath(json_path)}' was not found. "
                 f"Please ensure it's generated (e.g., by an extraction script).")
        return pd.DataFrame()
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        if 'votes' not in raw_data or 'party_composition' not in raw_data:
            st.error("Error: JSON data is missing essential 'votes' or 'party_composition' keys.")
            return pd.DataFrame()

        party_composition_data = raw_data.get('party_composition', {})
        all_vote_details = []

        for vote_event in raw_data.get('votes', []):
            if not isinstance(vote_event, dict) or not vote_event.get('id'):
                # Optionally, log or notify about skipping malformed vote entries
                # st.warning(f"Skipping a vote entry due to missing 'id' or invalid format.")
                continue

            issue_id = vote_event.get('id')
            title = vote_event.get('title', 'Título não disponível.')
            vote_type = vote_event.get('type', 'N/A')
            url = vote_event.get('url', '')
            outcome = vote_event.get('result', 'N/A')
            is_unanimous_vote = "unanimidade" in outcome.lower()
            # Assuming 'description' is not in the JSON per vote, otherwise use vote_event.get('description', ...)
            description_text = 'Descrição não disponível.'


            voting_breakdown = vote_event.get('voting_breakdown')

            if isinstance(voting_breakdown, dict) and voting_breakdown:
                for party_name, party_votes_data in voting_breakdown.items():
                    favor, against, abstention = 0, 0, 0 # Default votes
                    if isinstance(party_votes_data, dict):
                        favor = party_votes_data.get('favor', party_votes_data.get('votes_favor', 0))
                        against = party_votes_data.get('against', party_votes_data.get('votes_against', 0))
                        abstention = party_votes_data.get('abstention', party_votes_data.get('votes_abstention', 0))
                    
                    row = {
                        'issue_identifier': issue_id,
                        'full_title': title,
                        'description': description_text,
                        'hyperlink': url,
                        'vote_outcome': outcome,
                        'is_unanimous': is_unanimous_vote,
                        'issue_type': vote_type,
                        'party': party_name,
                        'party_total_mps': party_composition_data.get(party_name, 0),
                        'votes_favor': favor,
                        'votes_against': against,
                        'votes_abstention': abstention
                    }
                    all_vote_details.append(row)
            else:
                # If no breakdown, create rows for all parties in party_composition with 0 votes
                if not party_composition_data:
                     all_vote_details.append({
                        'issue_identifier': issue_id, 'full_title': title, 'description': description_text,
                        'hyperlink': url, 'vote_outcome': outcome, 'is_unanimous': is_unanimous_vote,
                        'issue_type': vote_type, 'party': 'N/A', 'party_total_mps': 0,
                        'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0
                    })
                else:
                    for party_name in party_composition_data.keys():
                        all_vote_details.append({
                            'issue_identifier': issue_id, 'full_title': title, 'description': description_text,
                            'hyperlink': url, 'vote_outcome': outcome, 'is_unanimous': is_unanimous_vote,
                            'issue_type': vote_type, 'party': party_name,
                            'party_total_mps': party_composition_data.get(party_name, 0),
                            'votes_favor': 0, 'votes_against': 0, 'votes_abstention': 0
                        })
        
        if not all_vote_details:
            st.info("No vote data could be processed from the JSON file. It might be empty or contain no valid vote entries.")
            return pd.DataFrame()

        df = pd.DataFrame(all_vote_details)

        # Ensure all expected columns exist, filling with defaults if necessary
        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink',
            'vote_outcome', 'is_unanimous', 'issue_type', 'party', 'party_total_mps',
            'votes_favor', 'votes_against', 'votes_abstention'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'party_total_mps']:
                    df[col] = 0
                elif col == 'is_unanimous':
                    df[col] = False
                else: 
                    df[col] = 'N/A' if col != 'hyperlink' else ''


        df['full_title'] = df['full_title'].fillna('Título não disponível.')
        df['description'] = df['description'].fillna('Descrição não disponível.')
        df['hyperlink'] = df['hyperlink'].fillna('')
        
        for col in ['party_total_mps', 'votes_favor', 'votes_against', 'votes_abstention']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int) # Corrected errors='fillna' to errors='coerce'

        return df

    except FileNotFoundError: 
        st.error(f"Error: The data file '{os.path.abspath(json_path)}' was not found.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        st.error(f"Error: Could not decode JSON from '{json_path}'. The file might be corrupted or not valid JSON.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred while loading data from '{json_path}': {e}")
        return pd.DataFrame()

data_df = load_data()  # Will now use default "portuguese_parliament_votes_json.json"

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
        search_df_unique_issues = data_df.drop_duplicates(subset=['issue_identifier']).copy()
        
        # Ensure 'description' and 'full_title' are strings for searching
        search_df_unique_issues['description'] = search_df_unique_issues['description'].astype(str)
        search_df_unique_issues['full_title'] = search_df_unique_issues['full_title'].astype(str)
        search_df_unique_issues['issue_identifier_str'] = search_df_unique_issues['issue_identifier'].astype(str)


        results = search_df_unique_issues[
            search_df_unique_issues['full_title'].str.contains(search_query, case=False, na=False) |
            search_df_unique_issues['description'].str.contains(search_query, case=False, na=False) |
            search_df_unique_issues['issue_identifier_str'].str.contains(search_query, case=False, na=False)
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
st.markdown("Desenvolvido com ❤️ por [Seu Nome/Organização Aqui]")
st.markdown("Dados extraídos de documentos oficiais da Assembleia da República.")