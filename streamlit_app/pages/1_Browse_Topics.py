import streamlit as st
import pandas as pd
import os
import json  # Added for JSON processing

# --- Page Configuration ---
st.set_page_config(
    page_title="Todas as Votações - VotoTransparente PT",
    page_icon="🇵🇹",
    layout="wide"
)

# --- Data Loading ---
@st.cache_data
def load_data(json_path="../data/portuguese_parliament_votes_json.json"): # Adjusted path for pages folder
    # Path adjustment for pages folder if JSON is at root:
    actual_json_path = os.path.join("..", json_path) if not os.path.isabs(json_path) else json_path
    # However, if extract_data.py puts it in the root, and pages run from root, direct path is fine.
    # Let's assume the file is accessible via the relative path from where Streamlit runs the page script,
    # or an absolute path. The original code used a relative path "detailed_parliament_votes.csv"
    # which implies Streamlit's CWD is the project root when running pages.
    # So, json_path="portuguese_parliament_votes_json.json" should work if CWD is project root.
    # For robustness if CWD is pages/, we might need "../portuguese_parliament_votes_json.json"
    # The original code had `csv_path="detailed_parliament_votes.csv"` in pages files,
    # implying it looked for the CSV in the `pages` directory itself, or Streamlit handles CWD.
    # Let's assume the JSON file is in the root, like the CSV was.
    # The original `load_data` in pages files had `csv_path="detailed_parliament_votes.csv"`
    # This means it expected the CSV in the same directory as the script (i.e. `pages/`) or that
    # streamlit runs scripts from the root directory.
    # Given the error message `os.path.abspath(csv_path)` was used, it's safer to assume
    # the path should be relative to the project root.
    # So, for scripts in `pages/`, the path to a root file is `../portuguese_parliament_votes_json.json`
    # OR, the `json_path` argument to `load_data()` call should be `../portuguese_parliament_votes_json.json`
    # Let's make the default in the function signature relative from project root,
    # and adjust the call if necessary, or ensure CWD is project root.
    # The original code in pages had `load_data()` which used the default `detailed_parliament_votes.csv`.
    # This implies the file was expected at `pages/detailed_parliament_votes.csv`.
    # If `portuguese_parliament_votes_json.json` is at the root, then from `pages/` dir, it's `../portuguese_parliament_votes_json.json`.
    # I will modify the default path for pages to reflect this.

    # Corrected path logic for files inside 'pages' directory, assuming json is at project root
    final_json_path = json_path
    if not os.path.isabs(json_path) and os.path.basename(os.getcwd()) == "pages":
        final_json_path = os.path.join("..", json_path)
    elif not os.path.exists(json_path) and os.path.exists(os.path.join("..", json_path)) and "pages" in os.getcwd():  # Fallback for pages
        final_json_path = os.path.join("..", json_path)

    if not os.path.exists(final_json_path):
        st.error(f"Error: The data file '{os.path.abspath(final_json_path)}' was not found. "
                 f"Please ensure it's generated (e.g., by an extraction script).")
        return pd.DataFrame()
    try:
        with open(final_json_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        if 'votes' not in raw_data or 'party_composition' not in raw_data:
            st.error("Error: JSON data is missing essential 'votes' or 'party_composition' keys.")
            return pd.DataFrame()

        party_composition_data = raw_data.get('party_composition', {})
        all_vote_details = []

        for vote_event in raw_data.get('votes', []):
            if not isinstance(vote_event, dict) or not vote_event.get('id'):
                continue

            issue_id = vote_event.get('id')
            title = vote_event.get('title', 'Título não disponível.')
            vote_type = vote_event.get('type', 'N/A')
            url = vote_event.get('url', '')
            outcome = vote_event.get('result', 'N/A')
            is_unanimous_vote = "unanimidade" in outcome.lower()
            description_text = 'Descrição não disponível.'

            voting_breakdown = vote_event.get('voting_breakdown')

            if isinstance(voting_breakdown, dict) and voting_breakdown:
                for party_name, party_votes_data in voting_breakdown.items():
                    favor, against, abstention = 0, 0, 0
                    if isinstance(party_votes_data, dict):
                        favor = party_votes_data.get('favor', party_votes_data.get('votes_favor', 0))
                        against = party_votes_data.get('against', party_votes_data.get('votes_against', 0))
                        abstention = party_votes_data.get('abstention', party_votes_data.get('votes_abstention', 0))

                    row = {
                        'issue_identifier': issue_id, 'full_title': title, 'description': description_text,
                        'hyperlink': url, 'vote_outcome': outcome, 'is_unanimous': is_unanimous_vote,
                        'issue_type': vote_type, 'party': party_name,
                        'party_total_mps': party_composition_data.get(party_name, 0),
                        'votes_favor': favor, 'votes_against': against, 'votes_abstention': abstention
                    }
                    all_vote_details.append(row)
            else:
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
            st.info("No vote data could be processed from the JSON file.")
            return pd.DataFrame()

        df = pd.DataFrame(all_vote_details)
        expected_cols = [
            'issue_identifier', 'full_title', 'description', 'hyperlink',
            'vote_outcome', 'is_unanimous', 'issue_type', 'party', 'party_total_mps',
            'votes_favor', 'votes_against', 'votes_abstention'
        ]
        for col in expected_cols:
            if col not in df.columns:
                if col in ['votes_favor', 'votes_against', 'votes_abstention', 'party_total_mps']: df[col] = 0
                elif col == 'is_unanimous': df[col] = False
                else: df[col] = 'N/A' if col != 'hyperlink' else ''
        
        df['full_title'] = df['full_title'].fillna('Título não disponível.')
        df['description'] = df['description'].fillna('Descrição não disponível.')
        df['hyperlink'] = df['hyperlink'].fillna('')
        for col in ['party_total_mps', 'votes_favor', 'votes_against', 'votes_abstention']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int) # Corrected errors='fillna' to errors='coerce'
        return df

    except FileNotFoundError: 
        st.error(f"Error: The data file '{os.path.abspath(final_json_path)}' was not found.")
        return pd.DataFrame()
    except json.JSONDecodeError:
        st.error(f"Error: Could not decode JSON from '{final_json_path}'. File might be corrupted.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred while loading data from '{final_json_path}': {e}")
        return pd.DataFrame()

data_df = load_data()  # Uses default "portuguese_parliament_votes_json.json"

st.title("📜 Todas as Votações Parlamentares")
st.markdown("Navegue pela lista de todas as votações registadas. Clique num item para ver os detalhes.")
st.markdown("---")

if not data_df.empty:
    # Get unique topics based on issue_identifier, keeping the first occurrence for title and outcome
    unique_topics = data_df.drop_duplicates(subset=['issue_identifier'])
    
    if not unique_topics.empty:
        # Sort by title or identifier if available
        # unique_topics = unique_topics.sort_values(by='full_title') # Optional: sort

        for _, topic_row in unique_topics.iterrows():
            with st.container(border=True):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"#### {topic_row['full_title']}")
                    if pd.notna(topic_row['issue_identifier']):
                        st.caption(f"Identificador: {topic_row['issue_identifier']}")
                    if pd.notna(topic_row['issue_type']):
                        st.caption(f"Tipo: {topic_row['issue_type']}")
                with col2:
                    if st.button(f"Ver detalhes 🗳️", key=f"btn_{topic_row['issue_identifier']}", use_container_width=True):
                        st.session_state.selected_issue_identifier = topic_row['issue_identifier']
                        st.switch_page("pages/2_Topic_Details.py")
                st.markdown(f"**Resultado da Votação:** {topic_row.get('vote_outcome', 'N/A')}")

    else:
        st.info("Não foram encontradas votações para listar.")
else:
    st.warning("Não foi possível carregar os dados das votações. Verifique as mensagens de erro.")

st.sidebar.page_link("streamlit_app.py", label="Página Inicial", icon="🏠")